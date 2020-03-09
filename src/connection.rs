use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::error::Error;
use crate::util::SerialId;

use crate::proto::{self, Codec, RequestKey, MessageIdData};
use futures::{self, channel::{mpsc, oneshot}, SinkExt, StreamExt, Future, Sink, Stream};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::prelude::*;
use tokio_util::codec::Framed;
use futures::task::{Context, Poll};
use std::pin::Pin;
use std::mem;
use crate::resolver::Resolver;
use crate::proto::command_subscribe::SubType;
use crate::consumer::ConsumerOptions;
use crate::producer::{self, ProducerOptions};
use crate::message::*;
use crate::proto::proto::get_topics::Mode;
use crate::message;
use crate::engine::ConsumerMessage;
use crate::message::ConsumerMessage;
use std::time::{Instant, Duration};

pub const CLIENT_VERSION: &'static str = "2.0.1-incubating";

#[derive(Clone, Default)]
pub struct ConnectionOptions {
    pub auth_method: Option<proto::AuthMethod>,
    pub auth_method_name: Option<String>,
    pub auth_data: Option<Vec<u8>>,
    pub proxy_to_broker_url: Option<String>,
}

#[derive(Clone, Default)]
pub(crate) struct ConnectionBuilder {
    authentication: Option<Authentication>,
    proxy_to_broker_url: Option<String>,
}

impl ConnectionBuilder {
    pub fn new() -> ConnectionBuilder {
        ConnectionBuilder::default()
    }

    pub fn authentication(mut self, authentication: Authentication) -> Self {
        self.authentication = Some(authentication);
        self
    }

    pub fn proxy_to_broker_url(mut self, proxy_to_broker_url: String) -> Self {
        self.proxy_to_broker_url = Some(proxy_to_broker_url);
        self
    }
}

pub enum ConnectionMessage {
    RegisterStream {
        key: RequestKey,
        stream: mpsc::UnboundedSender<proto::Message>,
    },
    CloseStream {
        key: RequestKey,
    },
    SendMessage {
        message: Box<dyn IntoProto>,
        resolver: Option<Resolver<proto::Message>>,
    },
    CheckConnection {
        resolver: Resolver<bool>,
    }
}

//pub enum ConnectionMessage2 {
//    CreateProducer { message: message::CreateProducer, resolver: Resolver<proto::Message> },
//    GetTopicsOfNamespace { message: message::GetTopicsOfNamespace, resolver: Resolver<proto::Message> },
//    Send { message: message::Send, resolver: Resolver<proto::Message> },
//    LookupTopic { message: message::LookupTopic, resolver: Resolver<proto::Message> },
//    LookupPartitionedTopic { message: message::LookupPartitionedTopic, resolver: Resolver<proto::Message> },
//    CloseProducer { message: message::CloseProducer, resolver: Resolver<proto::Message> },
//    CreateConsumer { consumer_id: u64, stream: mpsc::UnboundedSender<Message> },
//    Subscribe { message: message::Subscribe, resolver: Resolver<proto::Message> },
//    Flow (message::Flow),
//    Ack(message::Ack),
//    RedeliverUnacknowlegedMessages(message::RedeliverUnacknowlegedMessages),
//    CloseConsumer { message: message::CloseConsumer, resolver: Resolver<proto::Message> },
//}


#[derive(Default)]
struct MessageHandlers {
    requests: BTreeMap<RequestKey, Resolver<proto::Message>>,
    streams: BTreeMap<RequestKey, mpsc::UnboundedSender<proto::Message>>,
}

impl MessageHandlers {
    pub fn register_request(&mut self, key: RequestKey, resolver: Resolver<proto::Message>) {
        self.requests.insert(key, resolver);
    }


    pub fn register_stream(&mut self, key: RequestKey, stream: mpsc::UnboundedSender<proto::Message>) {
        self.streams.insert(key, stream);
    }

    pub fn close_straem(&mut self, key: &RequestKey) {
        self.streams.remove(key);
    }

    pub fn resolve(&mut self, message: proto::Message) {
        if let Some(key) = message.request_key() {
            if let Some(resolver) = self.requests.remove(&key) {
                resolver.resolve(Ok(message));
            } else {
                let stream_disconnected = self.streams.get_mut(&key)
                    .map(move |s| s.unbounded_send(message).is_err())
                    .unwrap_or(false);
                if stream_disconnected {
                    self.close_stream(&key);
                }
            }
        }
    }
}

pub(crate) struct Connection {
    inner: Framed<TcpStream, Codec>,
    server_version: String,
    protocol_version: Option<i32>,
    sender: mpsc::UnboundedSender<ConnectionMessage>,
    pending: mpsc::UnboundedReceiver<ConnectionMessage>,
    resolvers: MessageHandlers,
    request_id: SerialId,
    ping_timer: Interval,
    last_response: Instant,
}

impl Connection {
    pub async fn connect<A: ToSocketAddrs>(address: A, options: Option<ConnectionOptions>) -> Result<Connection, Error> {
        let stream = TcpStream::connect(address).await?;
        let mut conn = tokio_util::codec::Framed::new(stream, Codec);
        let connect = message::connect(options);
        trace!("sending connect message: {:?}", connect);
        conn.send(connect).await?;
        let connection_success = conn.next().await
            .ok_or(Error::disconnected("connection failed - TCP Stream closed unexpectedly"))
            .and_then(|r| r)
            .and_then(|mut msg| {
                if let Some(connected) = msg.command.connected {
                    return Ok(connected);
                }
                if let Some(error) = msg.command.error {
                    return Err(error.into());
                }
                Err(Error::unexpected_response(format!("unexpected response to `connect` command: {:?}", msg.command)))
            })?;

        Ok(Connection {
            inner: conn,
            server_version: connection_success.server_version,
            protocol_version: connection_success.protocol_version,
        })
    }

    pub fn handle(&self) -> ConnectionHandle {
        ConnectionHandle { sender: self.sender.clone() }
    }

    pub fn is_active(&self) -> bool {
        self.last_response() > (Instant::now() - Duration::from_secs(90))
    }

    fn resolve_err(&mut self, error: Error) {
        for (_, resolver) in mem::take(&mut self.resolvers) {
            resolve(Err(error.clone()));
        }
        self.pending.close();
        while let Ok(Some(message)) = self.pending.try_next() {
            match message {
                ConnectionMessage::CheckConnection { resolver } => {
                    resolver.resolve(Ok(false))
                }
                ConnectionMessage::SendMessage { resolver, .. } => {
                    resolver.resolver(Err(error.clone()))
                }
                _ => {}
            }
        }
    }

    /// Returns Ready(Ok) when all messages have flushed successfully, Pending when waiting on IO,
    /// and Ready(Err) if the connection fails.
    fn poll_send(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let send_state = loop {
            match self.inner.poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    match self.pending.pop_front() {
                        Some(message) => {
                            match message {
                                ConnectionMessage::RegisterStream { key, stream } => {
                                    self.resolvers.register_stream(key, stream);
                                }
                                ConnectionMessage::CloseStream { key } => {
                                    self.resolvers.close_straem(&key);
                                }
                                ConnectionMessage::SendMessage { message, resolver } => {
                                    let message = message.into_proto(&mut self.request_id);
                                    match (resolver, message.request_key()) {
                                        (Some(resolver), Some(key)) => {
                                            self.resolvers.register_request(key, resolver)
                                        },
                                        (Some(resolver), None) => {
                                            resolver.resolve(Err(Error::unexpected("BUG! connection received resolver, but message has no key")));
                                        },
                                        _ => {}
                                    }
                                    if let (Some(resolver), Some(key)) = (resolver, message.request_key()) {
                                        self.resolvers.register_request(key, resolver);
                                    }
                                    if let Err(e) = self.inner.start_send(message) {
                                        break Poll::Ready(Err(e))
                                    }
                                }
                                ConnectionMessage::CheckConnection { resolver } => {
                                    resolver.resolve(Ok(self.is_active()));
                                }
                            }
                        },
                        None => break Poll::Ready(Ok(()))
                    }
                }
                Poll::Pending => break Poll::Pending,
                Poll::Ready(Err(e)) => break Poll::Ready(Err(e)),
            }
        };
        if let Poll::Ready(_) = &send_state {
            self.inner.poll_flush(cx)
        } else {
            send_state
        }
    }

    /// Returns Ready(Ok) if the connection is closed, Ready(Err) if the connection
    /// fails, and Pending so long as the connection is open
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        while let Poll::Ready(next) = self.connection.poll_next(cx) {
            self.last_response = Some(Instant::now());
            match next {
                Some(Ok(message)) => {
                    let message: proto::Message = message;
                    if message.command.ping.is_some() {
                        self.pending.push_front(ConnectionMessage::SendMessage {
                            message: Box::new(Pong),
                            resolver: None,
                        });
                        continue;
                    }
                    self.resolvers.resolve(message);
                },
                Some(Err(error)) => return Poll::Ready(Err(error)),
                None => return Poll::Ready(Ok(())),
            }
        }
        Poll::Pending
    }

    pub fn has_pending_messages(&self) -> bool {
        !self.pending.is_empty() || !self.resolvers.is_empty()
    }
}

impl Future for Connection {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        while let Poll::Ready(_) = self.ping_timer.poll(cx) {
            self.pending.push_front(ConnectionMessage::SendMessage {
                message: Box::new(Ping),
                resolver: None
            });
        }
        loop {
            match self.poll_send(cx) {
                Poll::Ready(Ok(())) => continue,
                Poll::Ready(Err(error)) => {
                    self.resolve_err(error.clone());
                    return Poll::Ready(Err(error))
                },
                Poll::Pending => break,
            }
        }
        loop {
            match self.poll_recv(cx) {
                Poll::Ready(Ok(())) => {
                    self.resolve_err(Error::disconnected("Unexpected end of stream"));
                    return Poll::Ready(Ok(()));
                },
                Poll::Ready(Err(error)) => {
                    self.resolve_err(error.clone());
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => break,
            }
        }
        Poll::Pending
    }
}

#[derive(Clone)]
pub struct ConnectionHandle {
    sender: mpsc::UnboundedSender<ConnectionMessage>,
}

impl ConnectionHandle {
    async fn make_request<T: FromProto>(&self, msg: I) -> Result<T, Error> {
        let (resolver, f) = Resolver::new();
        self.send_command(cmd, Some(resolver))?;
        T::from_proto(f.await?)
    }

    fn send_message(&self, message: ConnectionMessage) -> Result<(), Error> {
        self.sender.unbounded_send(ConnectionMessage { message, resolver })
            .map_err(|e| Error::disconnected("Connection unexpectedly dropped"))
    }

    pub async fn create_producer(&self, topic: String, producer_id: u64, options: ProducerOptions) -> Result<CreateProducerSuccess, Error> {
        let (resolver, f) = Resolver::new();
        let message = ConnectionMessage::SendMessage {
            message: Box::new((CreateProducer {
                topic,
                producer_id,
                options
            })),
            resolver: Some(resolver)
        };
        self.send_message(message)?;
        CreateProducerSuccess::from_proto(f.await?)
    }

    pub async fn get_topics_of_namespace(&self, namespace: String, mode: Mode) -> Result<NamespaceTopics, Error> {
        let (resolver, f) = Resolver::new();
        let message = ConnectionMessage::SendMessage {
            message: Box::new(GetTopicsOfNamespace {
                namespace,
                mode
            }),
            resolver: Some(resolver)
        };
        self.send_message(message)?;
        NamespaceTopics::from_proto(f.await?)
    }

    pub async fn send(&self, message: Send) -> Result<SendReceipt, Error> {
        let (resolver, f) = Resolver::new();
        let message = ConnectionMessage::SendMessage {
            message: Box::new(message),
            resolver: Some(resolver)
        };
        self.send_message(message)?;
        SendReceipt::from_proto(f.await?)
    }

    pub async fn lookup_topic(&self, topic: String, authoritative: Option<bool>) -> Result<LookupTopicResponse, Error> {
        let (resolver, f) = Resolver::new();
        let message = ConnectionMessage::SendMessage {
            message: Box::new(LookupTopic {
                topic,
                authoritative
            }),
            resolver: Some(resolver)
        };
        self.send_message(message)?;
        LookupTopicResponse::from_proto(f.await?)
    }

    pub async fn lookup_partitioned_topic(&self, topic: String) -> Result<PartitionedTopicMetadata, Error> {
        let (resolver, f) = Resolver::new();
        let message = ConnectionMessage::SendMessage {
            message: Box::new(LookupPartitionedTopic {
                topic
            }),
            resolver: Some(resolver)
        };
        self.send_message(message)?;
        PartitionedTopicMetadata::from_proto(f.await?)
    }

    pub async fn close_producer(&self, producer_id: u64) -> Result<CommandSuccess, Error> {
        let (resolver, f) = Resolver::new();
        let message = ConnectionMessage::SendMessage {
            message: Box::new(CloseProducer { producer_id }),
            resolver: Some(resolver)
        };
        self.send_message(message)?;
        CommandSuccess::from_proto(f.await?)
    }

    pub fn create_consumer(&self, consumer_id: u64) -> Result<impl Stream<Item=Result<ConsumerMessage, Error>>, Error> {
        let (tx, rx) = mpsc::unbounded();
        let message = ConnectionMessage::RegisterStream {
            key: RequestKey::Consumer { consumer_id },
            stream: tx
        };
        self.send_message(message)?;
        Ok(rx.filter_map(|msg| match ConsumerMessage::from_proto(msg) {
            Ok(msg) => Some(msg),
            Err(e) => {
                warn!("Received unexpected message for consumer. This may be a bug. Err: {}", e);
                None
            }
        }))
    }

    pub async fn subscribe(&self, cmd: Subscribe) -> Result<CommandSuccess, Error> {
        let (resolver, f) = Resolver::new();
        let message = ConnectionMessage::SendMessage {
            message: Box::new((cmd)),
            resolver: Some(resolver)
        };
        self.send_message(message)?;
        CommandSuccess::from_proto(f.await?)
    }

    pub fn flow(&self, consumer_id: u64, message_permits: u32) -> Result<(), Error> {
        self.send_message(ConnectionMessage::SendMessage {
            message: Box::new(Flow {
                consumer_id,
                message_permits
            }),
            resolver: None,
        })
    }

    pub fn ack(&self, ack: Ack) -> Result<(), Error> {
        self.send_command(Command::Ack(ack), None)
    }

    pub fn redeliver_unacknowleged_messages(&self, consumer_id: u64, message_ids: Vec<MessageIdData>) -> Result<(), Error> {
        self.send_command(Command::RedeliverUnacknowlegedMessages(RedeliverUnacknowlegedMessages {
            consumer_id,
            message_ids,
        }), None)
    }

    pub async fn close_consumer(&self, consumer_id: u64) -> Result<CommandSuccess, Error> {
        let (resolver, f) = Resolver::new();
        let message = ConnectionMessage::SendMessage {
            message: Box::new(CloseConsumer { consumer_id }),
            resolver: Some(resolver)
        };
        self.make_request(message)?;
        let success = CommandSuccess::from_proto(f.await?)?;
        self.send_message(ConnectionMessage::CloseStream {
            key: RequestKey::Consumer { consumer_id }
        })?;
        Ok(success)
    }

    pub async fn check_connection(&self) -> bool {
        let (resolver, f) = Resolver::new();
        if self.sender.unbounded_send(ConnectionMessage::CheckConnection { resolver }).is_err() {
            return false;
        }
        f.await.unwrap_or(false)
    }
}
//
//impl Sink<Message> for Connection {
//    type Error = Error;
//
//    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//        Sink::poll_close(Pin::new(&mut self.inner), cx)
//    }
//
//    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
//        Sink::start_send(Pin::new(&mut self.inner), item)
//    }
//
//    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//        Sink::poll_flush(Pin::new(&mut self.inner), cx)
//    }
//
//    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//        Sink::poll_close(Pin::new(&mut self.inner), cx)
//    }
//}
//
//impl Stream for Connection {
//    type Item = Result<Message, Error>;
//
//    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//        Stream::poll_next(Pin::new(&mut self.inner), cx)
//    }
//
//    fn size_hint(&self) -> (usize, Option<usize>) {
//        self.inner.size_hint()
//    }
//}


//use tokio_codec;
//
//use crate::consumer::ConsumerOptions;
//use crate::error::{ConnectionError, SharedError};
//use crate::message::{
//    proto::{self, command_subscribe::SubType},
//    Codec, Message,
//};
//use crate::producer::{self, ProducerOptions};
//
//pub enum Register {
//    Request {
//        key: RequestKey,
//        resolver: oneshot::Sender<Message>,
//    },
//    Consumer {
//        consumer_id: u64,
//        resolver: mpsc::UnboundedSender<Message>,
//    },
//}
//

#[derive(Clone)]
pub struct Authentication {
    pub name: String,
    pub data: Vec<u8>,
}

//
//pub struct Receiver<S: Stream<Item = Message, Error = ConnectionError>> {
//    inbound: S,
//    outbound: mpsc::UnboundedSender<Message>,
//    error: SharedError,
//    pending_requests: BTreeMap<RequestKey, oneshot::Sender<Message>>,
//    consumers: BTreeMap<u64, mpsc::UnboundedSender<Message>>,
//    received_messages: BTreeMap<RequestKey, Message>,
//    registrations: mpsc::UnboundedReceiver<Register>,
//    shutdown: oneshot::Receiver<()>,
//}
//
//impl<S: Stream<Item = Message, Error = ConnectionError>> Receiver<S> {
//    pub fn new(
//        inbound: S,
//        outbound: mpsc::UnboundedSender<Message>,
//        error: SharedError,
//        registrations: mpsc::UnboundedReceiver<Register>,
//        shutdown: oneshot::Receiver<()>,
//    ) -> Receiver<S> {
//        Receiver {
//            inbound,
//            outbound,
//            error,
//            pending_requests: BTreeMap::new(),
//            received_messages: BTreeMap::new(),
//            consumers: BTreeMap::new(),
//            registrations,
//            shutdown,
//        }
//    }
//}
//
//impl<S: Stream<Item = Message, Error = ConnectionError>> Future for Receiver<S> {
//    type Item = ();
//    type Error = ();
//
//    fn poll(&mut self) -> Result<Async<()>, ()> {
//        match self.shutdown.poll() {
//            Ok(Async::Ready(())) | Err(futures::Canceled) => return Err(()),
//            Ok(Async::NotReady) => {}
//        }
//
//        //Are we worried about starvation here?
//        loop {
//            match self.registrations.poll() {
//                Ok(Async::Ready(Some(Register::Request { key, resolver }))) => {
//                    match self.received_messages.remove(&key) {
//                        Some(msg) => {
//                            let _ = resolver.send(msg);
//                        }
//                        None => {
//                            self.pending_requests.insert(key, resolver);
//                        }
//                    }
//                }
//                Ok(Async::Ready(Some(Register::Consumer {
//                    consumer_id,
//                    resolver,
//                }))) => {
//                    self.consumers.insert(consumer_id, resolver);
//                }
//                Ok(Async::Ready(None)) | Err(_) => {
//                    self.error.set(ConnectionError::Disconnected);
//                    return Err(());
//                }
//                Ok(Async::NotReady) => break,
//            }
//        }
//
//        loop {
//            match self.inbound.poll() {
//                Ok(Async::Ready(Some(msg))) => {
//                    if msg.command.ping.is_some() {
//                        let _ = self.outbound.unbounded_send(messages::pong());
//                    } else if msg.command.message.is_some() {
//                        if let Some(consumer) = self
//                            .consumers
//                            .get_mut(&msg.command.message.as_ref().unwrap().consumer_id)
//                        {
//                            let _ = consumer.unbounded_send(msg);
//                        }
//                    } else {
//                        if let Some(request_key) = msg.request_key() {
//                            if let Some(resolver) = self.pending_requests.remove(&request_key) {
//                                // We don't care if the receiver has dropped their future
//                                let _ = resolver.send(msg);
//                            } else {
//                                self.received_messages.insert(request_key, msg);
//                            }
//                        } else {
//                            println!(
//                                "Received message with no request_id; dropping. Message: {:?}",
//                                msg.command
//                            );
//                        }
//                    }
//                }
//                Ok(Async::Ready(None)) => {
//                    self.error.set(ConnectionError::Disconnected);
//                    return Err(());
//                }
//                Ok(Async::NotReady) => return Ok(Async::NotReady),
//                Err(e) => {
//                    self.error.set(e);
//                    return Err(());
//                }
//            }
//        }
//    }
//}
//
//pub struct Sender<S: Sink<SinkItem = Message, SinkError = ConnectionError>> {
//    sink: S,
//    outbound: mpsc::UnboundedReceiver<Message>,
//    buffered: Option<Message>,
//    error: SharedError,
//    shutdown: oneshot::Receiver<()>,
//}
//
//impl<S: Sink<SinkItem = Message, SinkError = ConnectionError>> Sender<S> {
//    pub fn new(
//        sink: S,
//        outbound: mpsc::UnboundedReceiver<Message>,
//        error: SharedError,
//        shutdown: oneshot::Receiver<()>,
//    ) -> Sender<S> {
//        Sender {
//            sink,
//            outbound,
//            buffered: None,
//            error,
//            shutdown,
//        }
//    }
//
//    fn try_start_send(&mut self, item: Message) -> futures::Poll<(), ConnectionError> {
//        if let AsyncSink::NotReady(item) = self.sink.start_send(item)? {
//            self.buffered = Some(item);
//            return Ok(Async::NotReady);
//        }
//        Ok(Async::Ready(()))
//    }
//}
//
//impl<S: Sink<SinkItem = Message, SinkError = ConnectionError>> Future for Sender<S> {
//    type Item = ();
//    type Error = ();
//
//    fn poll(&mut self) -> Result<Async<()>, ()> {
//        match self.shutdown.poll() {
//            Ok(Async::Ready(())) | Err(futures::Canceled) => return Err(()),
//            Ok(Async::NotReady) => {}
//        }
//
//        if let Some(item) = self.buffered.take() {
//            try_ready!(self.try_start_send(item).map_err(|e| self.error.set(e)))
//        }
//
//        loop {
//            match self.outbound.poll()? {
//                Async::Ready(Some(item)) => {
//                    try_ready!(self.try_start_send(item).map_err(|e| self.error.set(e)))
//                }
//                Async::Ready(None) => {
//                    try_ready!(self.sink.close().map_err(|e| self.error.set(e)));
//                    return Ok(Async::Ready(()));
//                }
//                Async::NotReady => {
//                    try_ready!(self.sink.poll_complete().map_err(|e| self.error.set(e)));
//                    return Ok(Async::NotReady);
//                }
//            }
//        }
//    }
//}
//
//#[derive(Clone)]
//pub struct SerialId(Arc<AtomicUsize>);
//
//impl SerialId {
//    pub fn new() -> SerialId {
//        SerialId(Arc::new(AtomicUsize::new(0)))
//    }
//    pub fn get(&self) -> u64 {
//        self.0.fetch_add(1, Ordering::Relaxed) as u64
//    }
//}
//
///// An owned type that can send messages like a connection
//#[derive(Clone)]
//pub struct ConnectionSender {
//    tx: mpsc::UnboundedSender<Message>,
//    registrations: mpsc::UnboundedSender<Register>,
//    request_id: SerialId,
//    error: SharedError,
//}
//
//impl ConnectionSender {
//    pub fn new(
//        tx: mpsc::UnboundedSender<Message>,
//        registrations: mpsc::UnboundedSender<Register>,
//        request_id: SerialId,
//        error: SharedError,
//    ) -> ConnectionSender {
//        ConnectionSender {
//            tx,
//            registrations,
//            request_id,
//            error,
//        }
//    }
//
//    pub fn send(
//        &self,
//        producer_id: u64,
//        producer_name: String,
//        sequence_id: u64,
//        num_messages: Option<i32>,
//        message: producer::Message,
//    ) -> impl Future<Item = proto::CommandSendReceipt, Error = ConnectionError> {
//        let key = RequestKey::ProducerSend {
//            producer_id,
//            sequence_id,
//        };
//        let msg = messages::send(
//            producer_id,
//            producer_name,
//            sequence_id,
//            num_messages,
//            message,
//        );
//        self.send_message(msg, key, |resp| resp.command.send_receipt)
//    }
//
//    pub fn send_ping(&self) -> Result<(), ConnectionError> {
//        self.tx
//            .unbounded_send(messages::ping())
//            .map_err(|_| ConnectionError::Disconnected)
//    }
//
//    pub fn lookup_topic<S: Into<String>>(
//        &self,
//        topic: S,
//        authoritative: bool,
//    ) -> impl Future<Item = proto::CommandLookupTopicResponse, Error = ConnectionError> {
//        let request_id = self.request_id.get();
//        let msg = messages::lookup_topic(topic.into(), authoritative, request_id);
//        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
//            resp.command.lookup_topic_response
//        })
//    }
//
//    pub fn lookup_partitioned_topic<S: Into<String>>(
//        &self,
//        topic: S,
//    ) -> impl Future<Item = proto::CommandPartitionedTopicMetadataResponse, Error = ConnectionError>
//    {
//        let request_id = self.request_id.get();
//        let msg = messages::lookup_partitioned_topic(topic.into(), request_id);
//        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
//            resp.command.partition_metadata_response
//        })
//    }
//
//    pub fn create_producer(
//        &self,
//        topic: String,
//        producer_id: u64,
//        producer_name: Option<String>,
//        options: ProducerOptions,
//    ) -> impl Future<Item = proto::CommandProducerSuccess, Error = ConnectionError> {
//        let request_id = self.request_id.get();
//        let msg = messages::create_producer(topic, producer_name, producer_id, request_id, options);
//        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
//            resp.command.producer_success
//        })
//    }
//
//    pub fn get_topics_of_namespace(
//        &self,
//        namespace: String,
//        mode: proto::get_topics::Mode,
//    ) -> impl Future<Item = proto::CommandGetTopicsOfNamespaceResponse, Error = ConnectionError>
//    {
//        let request_id = self.request_id.get();
//        let msg = messages::get_topics_of_namespace(request_id, namespace, mode);
//        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
//            resp.command.get_topics_of_namespace_response
//        })
//    }
//
//    pub fn close_producer(
//        &self,
//        producer_id: u64,
//    ) -> impl Future<Item = proto::CommandSuccess, Error = ConnectionError> {
//        let request_id = self.request_id.get();
//        let msg = messages::close_producer(producer_id, request_id);
//        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
//            resp.command.success
//        })
//    }
//
//    pub fn subscribe(
//        &self,
//        resolver: mpsc::UnboundedSender<Message>,
//        topic: String,
//        subscription: String,
//        sub_type: SubType,
//        consumer_id: u64,
//        consumer_name: Option<String>,
//        options: ConsumerOptions,
//    ) -> impl Future<Item = proto::CommandSuccess, Error = ConnectionError> {
//        let request_id = self.request_id.get();
//        let msg = messages::subscribe(
//            topic,
//            subscription,
//            sub_type,
//            consumer_id,
//            request_id,
//            consumer_name,
//            options,
//        );
//        match self.registrations.unbounded_send(Register::Consumer {
//            consumer_id,
//            resolver,
//        }) {
//            Ok(_) => {}
//            Err(_) => {
//                self.error.set(ConnectionError::Disconnected);
//                return Either::A(future::failed(ConnectionError::Disconnected));
//            }
//        }
//        Either::B(
//            self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
//                resp.command.success
//            }),
//        )
//    }
//
//    pub fn send_flow(&self, consumer_id: u64, message_permits: u32) -> Result<(), ConnectionError> {
//        self.tx
//            .unbounded_send(messages::flow(consumer_id, message_permits))
//            .map_err(|_| ConnectionError::Disconnected)
//    }
//
//    pub fn send_ack(
//        &self,
//        consumer_id: u64,
//        message_ids: Vec<proto::MessageIdData>,
//        cumulative: bool,
//    ) -> Result<(), ConnectionError> {
//        self.tx
//            .unbounded_send(messages::ack(consumer_id, message_ids, cumulative))
//            .map_err(|_| ConnectionError::Disconnected)
//    }
//
//    pub fn send_redeliver_unacknowleged_messages(
//        &self,
//        consumer_id: u64,
//        message_ids: Vec<proto::MessageIdData>,
//    ) -> Result<(), ConnectionError> {
//        self.tx
//            .unbounded_send(messages::redeliver_unacknowleged_messages(
//                consumer_id,
//                message_ids,
//            ))
//            .map_err(|_| ConnectionError::Disconnected)
//    }
//
//    pub fn close_consumer(
//        &self,
//        consumer_id: u64,
//    ) -> impl Future<Item = proto::CommandSuccess, Error = ConnectionError> {
//        let request_id = self.request_id.get();
//        let msg = messages::close_consumer(consumer_id, request_id);
//        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
//            resp.command.success
//        })
//    }
//
//    fn send_message<R: Debug, F>(
//        &self,
//        msg: Message,
//        key: RequestKey,
//        extract: F,
//    ) -> impl Future<Item = R, Error = ConnectionError>
//    where
//        F: FnOnce(Message) -> Option<R>,
//    {
//        let (resolver, response) = oneshot::channel();
//        trace!("sending message(key = {:?}): {:?}", key, msg);
//
//        let k = key.clone();
//        let response = response
//            .map_err(|oneshot::Canceled| ConnectionError::Disconnected)
//            .and_then(move |message: Message| {
//                trace!("received message(key = {:?}): {:?}", k, message);
//                extract_message(message, extract)
//            });
//
//        match (
//            self.registrations
//                .unbounded_send(Register::Request { key, resolver }),
//            self.tx.unbounded_send(msg),
//        ) {
//            (Ok(_), Ok(_)) => Either::A(response),
//            _ => Either::B(future::err(ConnectionError::Disconnected)),
//        }
//    }
//}
//
//pub struct Connection {
//    addr: String,
//    sender: ConnectionSender,
//    sender_shutdown: Option<oneshot::Sender<()>>,
//    receiver_shutdown: Option<oneshot::Sender<()>>,
//    executor: TaskExecutor,
//}
//
//impl Connection {
//    pub fn new(
//        addr: String,
//        auth_data: Option<Authentication>,
//        proxy_to_broker_url: Option<String>,
//        executor: TaskExecutor,
//    ) -> impl Future<Item = Connection, Error = ConnectionError> {
//        SocketAddr::from_str(&addr)
//            .into_future()
//            .map_err(|e| ConnectionError::SocketAddr(e.to_string()))
//            .and_then(|addr| {
//                TcpStream::connect(&addr)
//                    .map_err(|e| e.into())
//                    .map(|stream| tokio_codec::Framed::new(stream, Codec))
//                    .and_then(|stream| {
//                        stream
//                            .send({
//                                let msg = messages::connect(auth_data, proxy_to_broker_url);
//                                trace!("connection message: {:?}", msg);
//                                msg
//                            })
//                            .and_then(|stream| stream.into_future().map_err(|(err, _)| err))
//                            .and_then(move |(msg, stream)| match msg {
//                                Some(Message {
//                                    command:
//                                        proto::BaseCommand {
//                                            error: Some(error), ..
//                                        },
//                                    ..
//                                }) => Err(ConnectionError::PulsarError(format!("{:?}", error))),
//                                Some(msg) => {
//                                    let cmd = msg.command.clone();
//                                    trace!("received connection response: {:?}", msg);
//                                    msg.command
//                                        .connected
//                                        .ok_or_else(|| {
//                                            ConnectionError::PulsarError(format!(
//                                                "Unexpected message from pulsar: {:?}",
//                                                cmd
//                                            ))
//                                        })
//                                        .map(|_msg| stream)
//                                }
//                                None => Err(ConnectionError::Disconnected),
//                            })
//                    })
//            })
//            .map(move |pulsar| {
//                let (sink, stream) = pulsar.split();
//                let (tx, rx) = mpsc::unbounded();
//                let (registrations_tx, registrations_rx) = mpsc::unbounded();
//                let error = SharedError::new();
//                let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
//                let (sender_shutdown_tx, sender_shutdown_rx) = oneshot::channel();
//
//                executor.spawn(Receiver::new(
//                    stream,
//                    tx.clone(),
//                    error.clone(),
//                    registrations_rx,
//                    receiver_shutdown_rx,
//                ));
//
//                executor.spawn(Sender::new(sink, rx, error.clone(), sender_shutdown_rx));
//
//                let sender = ConnectionSender::new(tx, registrations_tx, SerialId::new(), error);
//
//                Connection {
//                    addr,
//                    sender,
//                    sender_shutdown: Some(sender_shutdown_tx),
//                    receiver_shutdown: Some(receiver_shutdown_tx),
//                    executor,
//                }
//            })
//    }
//
//    pub fn error(&self) -> Option<ConnectionError> {
//        self.sender.error.remove()
//    }
//
//    pub fn is_valid(&self) -> bool {
//        self.sender.error.is_set()
//    }
//
//    pub fn addr(&self) -> &str {
//        &self.addr
//    }
//
//    /// Chain to send a message, e.g. conn.sender().send_ping()
//    pub fn sender(&self) -> &ConnectionSender {
//        &self.sender
//    }
//
//    pub fn executor(&self) -> TaskExecutor {
//        self.executor.clone()
//    }
//}
//
//impl Drop for Connection {
//    fn drop(&mut self) {
//        if let Some(shutdown) = self.sender_shutdown.take() {
//            let _ = shutdown.send(());
//        }
//        if let Some(shutdown) = self.receiver_shutdown.take() {
//            let _ = shutdown.send(());
//        }
//    }
//}
//
//fn extract_message<T: Debug, F>(message: Message, extract: F) -> Result<T, ConnectionError>
//where
//    F: FnOnce(Message) -> Option<T>,
//{
//    if message.command.error.is_some() {
//        Err(ConnectionError::PulsarError(format!(
//            "{:?}",
//            message.command.error.unwrap()
//        )))
//    } else if message.command.send_error.is_some() {
//        Err(ConnectionError::PulsarError(format!(
//            "{:?}",
//            message.command.error.unwrap()
//        )))
//    } else {
//        let cmd = message.command.clone();
//        if let Some(extracted) = extract(message) {
//            trace!("extracted message: {:?}", extracted);
//            Ok(extracted)
//        } else {
//            Err(ConnectionError::UnexpectedResponse(format!("{:?}", cmd)))
//        }
//    }
//}
//
