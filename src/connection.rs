use std::collections::BTreeMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures::{
    self,
    future::{self, Either},
    sync::{mpsc, oneshot},
    Async, AsyncSink, Future, IntoFuture, Sink, Stream,
};
use tokio::net::TcpStream;
use tokio_codec;

use crate::consumer::ConsumerOptions;
use crate::error::{ConnectionError, SharedError};
use crate::executor::{PulsarExecutor, TaskExecutor};
use crate::message::{
    proto::{self, command_subscribe::SubType},
    Codec, Message,
};
use crate::producer::{self, ProducerOptions};

pub enum Register {
    Request {
        key: RequestKey,
        resolver: oneshot::Sender<Message>,
    },
    Consumer {
        consumer_id: u64,
        resolver: mpsc::UnboundedSender<Message>,
    },
}

#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq)]
pub enum RequestKey {
    RequestId(u64),
    ProducerSend { producer_id: u64, sequence_id: u64 },
}

#[derive(Clone)]
pub struct Authentication {
    pub name: String,
    pub data: Vec<u8>,
}

pub struct Receiver<S: Stream<Item = Message, Error = ConnectionError>> {
    inbound: S,
    outbound: mpsc::UnboundedSender<Message>,
    error: SharedError,
    pending_requests: BTreeMap<RequestKey, oneshot::Sender<Message>>,
    consumers: BTreeMap<u64, mpsc::UnboundedSender<Message>>,
    received_messages: BTreeMap<RequestKey, Message>,
    registrations: mpsc::UnboundedReceiver<Register>,
    shutdown: oneshot::Receiver<()>,
}

impl<S: Stream<Item = Message, Error = ConnectionError>> Receiver<S> {
    pub fn new(
        inbound: S,
        outbound: mpsc::UnboundedSender<Message>,
        error: SharedError,
        registrations: mpsc::UnboundedReceiver<Register>,
        shutdown: oneshot::Receiver<()>,
    ) -> Receiver<S> {
        Receiver {
            inbound,
            outbound,
            error,
            pending_requests: BTreeMap::new(),
            received_messages: BTreeMap::new(),
            consumers: BTreeMap::new(),
            registrations,
            shutdown,
        }
    }
}

impl<S: Stream<Item = Message, Error = ConnectionError>> Future for Receiver<S> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        match self.shutdown.poll() {
            Ok(Async::Ready(())) | Err(futures::Canceled) => return Err(()),
            Ok(Async::NotReady) => {}
        }

        //Are we worried about starvation here?
        loop {
            match self.registrations.poll() {
                Ok(Async::Ready(Some(Register::Request { key, resolver }))) => {
                    match self.received_messages.remove(&key) {
                        Some(msg) => {
                            let _ = resolver.send(msg);
                        }
                        None => {
                            self.pending_requests.insert(key, resolver);
                        }
                    }
                }
                Ok(Async::Ready(Some(Register::Consumer {
                    consumer_id,
                    resolver,
                }))) => {
                    self.consumers.insert(consumer_id, resolver);
                }
                Ok(Async::Ready(None)) | Err(_) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Err(());
                }
                Ok(Async::NotReady) => break,
            }
        }

        loop {
            match self.inbound.poll() {
                Ok(Async::Ready(Some(msg))) => {
                    if msg.command.ping.is_some() {
                        let _ = self.outbound.unbounded_send(messages::pong());
                    } else if msg.command.message.is_some() {
                        if let Some(consumer) = self
                            .consumers
                            .get_mut(&msg.command.message.as_ref().unwrap().consumer_id)
                        {
                            let _ = consumer.unbounded_send(msg);
                        }
                    } else if let Some(request_key) = msg.request_key() {
                        if let Some(resolver) = self.pending_requests.remove(&request_key) {
                            // We don't care if the receiver has dropped their future
                            let _ = resolver.send(msg);
                        } else {
                            self.received_messages.insert(request_key, msg);
                        }
                    } else {
                        println!(
                            "Received message with no request_id; dropping. Message: {:?}",
                            msg.command
                        );
                    }
                }
                Ok(Async::Ready(None)) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Err(());
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => {
                    self.error.set(e);
                    return Err(());
                }
            }
        }
    }
}

pub struct Sender<S: Sink<SinkItem = Message, SinkError = ConnectionError>> {
    sink: S,
    outbound: mpsc::UnboundedReceiver<Message>,
    buffered: Option<Message>,
    error: SharedError,
    shutdown: oneshot::Receiver<()>,
}

impl<S: Sink<SinkItem = Message, SinkError = ConnectionError>> Sender<S> {
    pub fn new(
        sink: S,
        outbound: mpsc::UnboundedReceiver<Message>,
        error: SharedError,
        shutdown: oneshot::Receiver<()>,
    ) -> Sender<S> {
        Sender {
            sink,
            outbound,
            buffered: None,
            error,
            shutdown,
        }
    }

    fn try_start_send(&mut self, item: Message) -> futures::Poll<(), ConnectionError> {
        if let AsyncSink::NotReady(item) = self.sink.start_send(item)? {
            self.buffered = Some(item);
            return Ok(Async::NotReady);
        }
        Ok(Async::Ready(()))
    }
}

impl<S: Sink<SinkItem = Message, SinkError = ConnectionError>> Future for Sender<S> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        match self.shutdown.poll() {
            Ok(Async::Ready(())) | Err(futures::Canceled) => return Err(()),
            Ok(Async::NotReady) => {}
        }

        if let Some(item) = self.buffered.take() {
            try_ready!(self.try_start_send(item).map_err(|e| self.error.set(e)))
        }

        loop {
            match self.outbound.poll()? {
                Async::Ready(Some(item)) => {
                    try_ready!(self.try_start_send(item).map_err(|e| self.error.set(e)))
                }
                Async::Ready(None) => {
                    try_ready!(self.sink.close().map_err(|e| self.error.set(e)));
                    return Ok(Async::Ready(()));
                }
                Async::NotReady => {
                    try_ready!(self.sink.poll_complete().map_err(|e| self.error.set(e)));
                    return Ok(Async::NotReady);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SerialId(Arc<AtomicUsize>);

impl Default for SerialId {
    fn default() -> Self {
        SerialId(Arc::new(AtomicUsize::new(0)))
    }
}

impl SerialId {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn get(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) as u64
    }
}

/// An owned type that can send messages like a connection
#[derive(Clone)]
pub struct ConnectionSender {
    tx: mpsc::UnboundedSender<Message>,
    registrations: mpsc::UnboundedSender<Register>,
    request_id: SerialId,
    error: SharedError,
}

impl ConnectionSender {
    pub fn new(
        tx: mpsc::UnboundedSender<Message>,
        registrations: mpsc::UnboundedSender<Register>,
        request_id: SerialId,
        error: SharedError,
    ) -> ConnectionSender {
        ConnectionSender {
            tx,
            registrations,
            request_id,
            error,
        }
    }

    pub fn send(
        &self,
        producer_id: u64,
        producer_name: String,
        sequence_id: u64,
        num_messages: Option<i32>,
        message: producer::Message,
    ) -> impl Future<Item = proto::CommandSendReceipt, Error = ConnectionError> {
        let key = RequestKey::ProducerSend {
            producer_id,
            sequence_id,
        };
        let msg = messages::send(
            producer_id,
            producer_name,
            sequence_id,
            num_messages,
            message,
        );
        self.send_message(msg, key, |resp| resp.command.send_receipt)
    }

    pub fn send_ping(&self) -> Result<(), ConnectionError> {
        self.tx
            .unbounded_send(messages::ping())
            .map_err(|_| ConnectionError::Disconnected)
    }

    pub fn lookup_topic<S: Into<String>>(
        &self,
        topic: S,
        authoritative: bool,
    ) -> impl Future<Item = proto::CommandLookupTopicResponse, Error = ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::lookup_topic(topic.into(), authoritative, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.lookup_topic_response
        })
    }

    pub fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> impl Future<Item = proto::CommandPartitionedTopicMetadataResponse, Error = ConnectionError>
    {
        let request_id = self.request_id.get();
        let msg = messages::lookup_partitioned_topic(topic.into(), request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.partition_metadata_response
        })
    }

    pub fn create_producer(
        &self,
        topic: String,
        producer_id: u64,
        producer_name: Option<String>,
        options: ProducerOptions,
    ) -> impl Future<Item = proto::CommandProducerSuccess, Error = ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::create_producer(topic, producer_name, producer_id, request_id, options);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.producer_success
        })
    }

    pub fn get_topics_of_namespace(
        &self,
        namespace: String,
        mode: proto::get_topics::Mode,
    ) -> impl Future<Item = proto::CommandGetTopicsOfNamespaceResponse, Error = ConnectionError>
    {
        let request_id = self.request_id.get();
        let msg = messages::get_topics_of_namespace(request_id, namespace, mode);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.get_topics_of_namespace_response
        })
    }

    pub fn close_producer(
        &self,
        producer_id: u64,
    ) -> impl Future<Item = proto::CommandSuccess, Error = ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::close_producer(producer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
    }

    pub fn subscribe(
        &self,
        resolver: mpsc::UnboundedSender<Message>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        consumer_id: u64,
        consumer_name: Option<String>,
        options: ConsumerOptions,
    ) -> impl Future<Item = proto::CommandSuccess, Error = ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::subscribe(
            topic,
            subscription,
            sub_type,
            consumer_id,
            request_id,
            consumer_name,
            options,
        );
        match self.registrations.unbounded_send(Register::Consumer {
            consumer_id,
            resolver,
        }) {
            Ok(_) => {}
            Err(_) => {
                self.error.set(ConnectionError::Disconnected);
                return Either::A(future::failed(ConnectionError::Disconnected));
            }
        }
        Either::B(
            self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
                resp.command.success
            }),
        )
    }

    pub fn send_flow(&self, consumer_id: u64, message_permits: u32) -> Result<(), ConnectionError> {
        self.tx
            .unbounded_send(messages::flow(consumer_id, message_permits))
            .map_err(|_| ConnectionError::Disconnected)
    }

    pub fn send_ack(
        &self,
        consumer_id: u64,
        message_ids: Vec<proto::MessageIdData>,
        cumulative: bool,
    ) -> Result<(), ConnectionError> {
        self.tx
            .unbounded_send(messages::ack(consumer_id, message_ids, cumulative))
            .map_err(|_| ConnectionError::Disconnected)
    }

    pub fn send_redeliver_unacknowleged_messages(
        &self,
        consumer_id: u64,
        message_ids: Vec<proto::MessageIdData>,
    ) -> Result<(), ConnectionError> {
        self.tx
            .unbounded_send(messages::redeliver_unacknowleged_messages(
                consumer_id,
                message_ids,
            ))
            .map_err(|_| ConnectionError::Disconnected)
    }

    pub fn close_consumer(
        &self,
        consumer_id: u64,
    ) -> impl Future<Item = proto::CommandSuccess, Error = ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::close_consumer(consumer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
    }

    fn send_message<R: Debug, F>(
        &self,
        msg: Message,
        key: RequestKey,
        extract: F,
    ) -> impl Future<Item = R, Error = ConnectionError>
    where
        F: FnOnce(Message) -> Option<R>,
    {
        let (resolver, response) = oneshot::channel();
        trace!("sending message(key = {:?}): {:?}", key, msg);

        let k = key.clone();
        let response = response
            .map_err(|oneshot::Canceled| ConnectionError::Disconnected)
            .and_then(move |message: Message| {
                trace!("received message(key = {:?}): {:?}", k, message);
                extract_message(message, extract)
            });

        match (
            self.registrations
                .unbounded_send(Register::Request { key, resolver }),
            self.tx.unbounded_send(msg),
        ) {
            (Ok(_), Ok(_)) => Either::A(response),
            _ => Either::B(future::err(ConnectionError::Disconnected)),
        }
    }
}

pub struct Connection {
    addr: String,
    sender: ConnectionSender,
    sender_shutdown: Option<oneshot::Sender<()>>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
    executor: TaskExecutor,
}

impl Connection {
    pub fn new<E: PulsarExecutor>(
        addr: String,
        auth_data: Option<Authentication>,
        proxy_to_broker_url: Option<String>,
        executor: E,
    ) -> impl Future<Item = Connection, Error = ConnectionError> {
        SocketAddr::from_str(&addr)
            .into_future()
            .map_err(|e| ConnectionError::SocketAddr(e.to_string()))
            .and_then(|addr| {
                TcpStream::connect(&addr)
                    .map_err(|e| e.into())
                    .map(|stream| tokio_codec::Framed::new(stream, Codec))
                    .and_then(|stream| {
                        stream
                            .send({
                                let msg = messages::connect(auth_data, proxy_to_broker_url);
                                trace!("connection message: {:?}", msg);
                                msg
                            })
                            .and_then(|stream| stream.into_future().map_err(|(err, _)| err))
                            .and_then(move |(msg, stream)| match msg {
                                Some(Message {
                                    command:
                                        proto::BaseCommand {
                                            error: Some(error), ..
                                        },
                                    ..
                                }) => Err(ConnectionError::PulsarError(format!("{:?}", error))),
                                Some(msg) => {
                                    let cmd = msg.command.clone();
                                    trace!("received connection response: {:?}", msg);
                                    msg.command
                                        .connected
                                        .ok_or_else(|| {
                                            ConnectionError::PulsarError(format!(
                                                "Unexpected message from pulsar: {:?}",
                                                cmd
                                            ))
                                        })
                                        .map(|_msg| stream)
                                }
                                None => Err(ConnectionError::Disconnected),
                            })
                    })
            })
            .map(move |pulsar| {
                let (sink, stream) = pulsar.split();
                let (tx, rx) = mpsc::unbounded();
                let (registrations_tx, registrations_rx) = mpsc::unbounded();
                let error = SharedError::new();
                let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
                let (sender_shutdown_tx, sender_shutdown_rx) = oneshot::channel();
                let executor = TaskExecutor::new(executor);

                executor.spawn(Receiver::new(
                    stream,
                    tx.clone(),
                    error.clone(),
                    registrations_rx,
                    receiver_shutdown_rx,
                ));

                executor.spawn(Sender::new(sink, rx, error.clone(), sender_shutdown_rx));

                let sender = ConnectionSender::new(tx, registrations_tx, SerialId::new(), error);

                Connection {
                    addr,
                    sender,
                    sender_shutdown: Some(sender_shutdown_tx),
                    receiver_shutdown: Some(receiver_shutdown_tx),
                    executor,
                }
            })
    }

    pub fn error(&self) -> Option<ConnectionError> {
        self.sender.error.remove()
    }

    pub fn is_valid(&self) -> bool {
        self.sender.error.is_set()
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Chain to send a message, e.g. conn.sender().send_ping()
    pub fn sender(&self) -> &ConnectionSender {
        &self.sender
    }

    pub fn executor(&self) -> impl PulsarExecutor {
        self.executor.clone()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(shutdown) = self.sender_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.receiver_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

fn extract_message<T: Debug, F>(message: Message, extract: F) -> Result<T, ConnectionError>
where
    F: FnOnce(Message) -> Option<T>,
{
    if message.command.error.is_some() {
        Err(ConnectionError::PulsarError(format!(
            "{:?}",
            message.command.error.unwrap()
        )))
    } else {
        let cmd = message.command.clone();
        if let Some(extracted) = extract(message) {
            trace!("extracted message: {:?}", extracted);
            Ok(extracted)
        } else {
            Err(ConnectionError::UnexpectedResponse(format!("{:?}", cmd)))
        }
    }
}

pub(crate) mod messages {
    use chrono::Utc;

    use crate::connection::Authentication;
    use crate::consumer::ConsumerOptions;
    use crate::message::{
        proto::{self, base_command::Type as CommandType, command_subscribe::SubType},
        Message, Payload,
    };
    use crate::producer::{self, ProducerOptions};

    pub fn connect(auth: Option<Authentication>, proxy_to_broker_url: Option<String>) -> Message {
        let (auth_method_name, auth_data) = match auth {
            Some(auth) => (Some(auth.name), Some(auth.data)),
            None => (None, None),
        };

        Message {
            command: proto::BaseCommand {
                type_: CommandType::Connect as i32,
                connect: Some(proto::CommandConnect {
                    auth_method_name,
                    auth_data,
                    proxy_to_broker_url,
                    client_version: String::from("2.0.1-incubating"),
                    protocol_version: Some(12),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn ping() -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Ping as i32,
                ping: Some(proto::CommandPing {}),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn pong() -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Pong as i32,
                pong: Some(proto::CommandPong {}),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn create_producer(
        topic: String,
        producer_name: Option<String>,
        producer_id: u64,
        request_id: u64,
        options: ProducerOptions,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Producer as i32,
                producer: Some(proto::CommandProducer {
                    topic,
                    producer_id,
                    request_id,
                    producer_name,
                    encrypted: options.encrypted,
                    metadata: options
                        .metadata
                        .iter()
                        .map(|(k, v)| proto::KeyValue {
                            key: k.clone(),
                            value: v.clone(),
                        })
                        .collect(),
                    schema: options.schema,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn get_topics_of_namespace(
        request_id: u64,
        namespace: String,
        mode: proto::get_topics::Mode,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::GetTopicsOfNamespace as i32,
                get_topics_of_namespace: Some(proto::CommandGetTopicsOfNamespace {
                    request_id,
                    namespace,
                    mode: Some(mode as i32),
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn send(
        producer_id: u64,
        producer_name: String,
        sequence_id: u64,
        num_messages: Option<i32>,
        message: producer::Message,
    ) -> Message {
        let properties = message
            .properties
            .into_iter()
            .map(|(key, value)| proto::KeyValue { key, value })
            .collect();

        Message {
            command: proto::BaseCommand {
                type_: CommandType::Send as i32,
                send: Some(proto::CommandSend {
                    producer_id,
                    sequence_id,
                    num_messages,
                }),
                ..Default::default()
            },
            payload: Some(Payload {
                metadata: proto::MessageMetadata {
                    producer_name,
                    sequence_id,
                    properties,
                    publish_time: Utc::now().timestamp_millis() as u64,
                    replicated_from: None,
                    partition_key: message.partition_key,
                    replicate_to: message.replicate_to,
                    compression: message.compression,
                    uncompressed_size: message.uncompressed_size,
                    num_messages_in_batch: message.num_messages_in_batch,
                    event_time: message.event_time,
                    encryption_keys: message.encryption_keys,
                    encryption_algo: message.encryption_algo,
                    encryption_param: message.encryption_param,
                    schema_version: message.schema_version,
                },
                data: message.payload,
            }),
        }
    }

    pub fn lookup_topic(topic: String, authoritative: bool, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Lookup as i32,
                lookup_topic: Some(proto::CommandLookupTopic {
                    topic,
                    request_id,
                    authoritative: Some(authoritative),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn lookup_partitioned_topic(topic: String, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::PartitionedMetadata as i32,
                partition_metadata: Some(proto::CommandPartitionedTopicMetadata {
                    topic,
                    request_id,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn close_producer(producer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::CloseProducer as i32,
                close_producer: Some(proto::CommandCloseProducer {
                    producer_id,
                    request_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn subscribe(
        topic: String,
        subscription: String,
        sub_type: SubType,
        consumer_id: u64,
        request_id: u64,
        consumer_name: Option<String>,
        options: ConsumerOptions,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Subscribe as i32,
                subscribe: Some(proto::CommandSubscribe {
                    topic,
                    subscription,
                    sub_type: sub_type as i32,
                    consumer_id,
                    request_id,
                    consumer_name,
                    priority_level: options.priority_level,
                    durable: options.durable,
                    metadata: options
                        .metadata
                        .iter()
                        .map(|(k, v)| proto::KeyValue {
                            key: k.clone(),
                            value: v.clone(),
                        })
                        .collect(),
                    read_compacted: options.read_compacted,
                    initial_position: options.initial_position,
                    schema: options.schema,
                    start_message_id: options.start_message_id,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn flow(consumer_id: u64, message_permits: u32) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Flow as i32,
                flow: Some(proto::CommandFlow {
                    consumer_id,
                    message_permits,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn ack(
        consumer_id: u64,
        message_id: Vec<proto::MessageIdData>,
        cumulative: bool,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Ack as i32,
                ack: Some(proto::CommandAck {
                    consumer_id,
                    ack_type: if cumulative {
                        proto::command_ack::AckType::Cumulative as i32
                    } else {
                        proto::command_ack::AckType::Individual as i32
                    },
                    message_id,
                    validation_error: None,
                    properties: Vec::new(),
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn redeliver_unacknowleged_messages(
        consumer_id: u64,
        message_ids: Vec<proto::MessageIdData>,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::RedeliverUnacknowledgedMessages as i32,
                redeliver_unacknowledged_messages: Some(
                    proto::CommandRedeliverUnacknowledgedMessages {
                        consumer_id,
                        message_ids,
                    },
                ),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn close_consumer(consumer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::CloseConsumer as i32,
                close_consumer: Some(proto::CommandCloseConsumer {
                    consumer_id,
                    request_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}
