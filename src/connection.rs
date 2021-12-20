use native_tls::Certificate;
use proto::MessageIdData;
use rand::{thread_rng, Rng};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use futures::{
    self,
    channel::{mpsc, oneshot},
    future::{select, Either},
    pin_mut,
    task::{Context, Poll},
    Future, FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use url::Url;

use crate::consumer::ConsumerOptions;
use crate::error::{ConnectionError, SharedError};
use crate::executor::{Executor, ExecutorKind};
use crate::message::{
    proto::{self, command_subscribe::SubType},
    BaseCommand, Codec, Message,
};
use crate::producer::{self, ProducerOptions};

pub(crate) enum Register {
    Request {
        key: RequestKey,
        resolver: oneshot::Sender<Message>,
    },
    Consumer {
        consumer_id: u64,
        resolver: mpsc::UnboundedSender<Message>,
    },
    Ping {
        resolver: oneshot::Sender<()>,
    },
}

/// identifier for a message
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq)]
pub enum RequestKey {
    RequestId(u64),
    ProducerSend { producer_id: u64, sequence_id: u64 },
    Consumer { consumer_id: u64 },
    CloseConsumer { consumer_id: u64, request_id: u64 },
}

/// Authentication parameters
#[derive(Clone)]
pub struct Authentication {
    /// Authentication kid. Use "token" for JWT
    pub name: String,
    /// Authentication data
    pub data: Vec<u8>,
}

pub(crate) struct Receiver<S: Stream<Item = Result<Message, ConnectionError>>> {
    inbound: Pin<Box<S>>,
    outbound: mpsc::UnboundedSender<Message>,
    error: SharedError,
    pending_requests: BTreeMap<RequestKey, oneshot::Sender<Message>>,
    consumers: BTreeMap<u64, mpsc::UnboundedSender<Message>>,
    received_messages: BTreeMap<RequestKey, Message>,
    registrations: Pin<Box<mpsc::UnboundedReceiver<Register>>>,
    shutdown: Pin<Box<oneshot::Receiver<()>>>,
    ping: Option<oneshot::Sender<()>>,
}

impl<S: Stream<Item = Result<Message, ConnectionError>>> Receiver<S> {
    pub fn new(
        inbound: S,
        outbound: mpsc::UnboundedSender<Message>,
        error: SharedError,
        registrations: mpsc::UnboundedReceiver<Register>,
        shutdown: oneshot::Receiver<()>,
    ) -> Receiver<S> {
        Receiver {
            inbound: Box::pin(inbound),
            outbound,
            error,
            pending_requests: BTreeMap::new(),
            received_messages: BTreeMap::new(),
            consumers: BTreeMap::new(),
            registrations: Box::pin(registrations),
            shutdown: Box::pin(shutdown),
            ping: None,
        }
    }
}

impl<S: Stream<Item = Result<Message, ConnectionError>>> Future for Receiver<S> {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shutdown.as_mut().poll(cx) {
            Poll::Ready(Ok(())) | Poll::Ready(Err(futures::channel::oneshot::Canceled)) => {
                return Poll::Ready(Err(()))
            }
            Poll::Pending => {}
        }

        //Are we worried about starvation here?
        loop {
            match self.registrations.as_mut().poll_next(cx) {
                Poll::Ready(Some(Register::Request { key, resolver })) => {
                    match self.received_messages.remove(&key) {
                        Some(msg) => {
                            let _ = resolver.send(msg);
                        }
                        None => {
                            self.pending_requests.insert(key, resolver);
                        }
                    }
                }
                Poll::Ready(Some(Register::Consumer {
                    consumer_id,
                    resolver,
                })) => {
                    self.consumers.insert(consumer_id, resolver);
                }
                Poll::Ready(Some(Register::Ping { resolver })) => {
                    self.ping = Some(resolver);
                }
                Poll::Ready(None) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Poll::Ready(Err(()));
                }
                Poll::Pending => break,
            }
        }

        loop {
            match self.inbound.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => match msg {
                    Message {
                        command: BaseCommand { ping: Some(_), .. },
                        ..
                    } => {
                        let _ = self.outbound.unbounded_send(messages::pong());
                    }
                    Message {
                        command: BaseCommand { pong: Some(_), .. },
                        ..
                    } => {
                        if let Some(sender) = self.ping.take() {
                            let _ = sender.send(());
                        }
                    }
                    msg => match msg.request_key() {
                        Some(key @ RequestKey::RequestId(_))
                        | Some(key @ RequestKey::ProducerSend { .. }) => {
                            if let Some(resolver) = self.pending_requests.remove(&key) {
                                // We don't care if the receiver has dropped their future
                                let _ = resolver.send(msg);
                            } else {
                                self.received_messages.insert(key, msg);
                            }
                        }
                        Some(RequestKey::Consumer { consumer_id }) => {
                            let _ = self
                                .consumers
                                .get_mut(&consumer_id)
                                .map(move |consumer| consumer.unbounded_send(msg));
                        }
                        Some(RequestKey::CloseConsumer {
                            consumer_id,
                            request_id,
                        }) => {
                            // FIXME: could the registration still be in queue while we get the
                            // CloseConsumer message?
                            if let Some(resolver) = self
                                .pending_requests
                                .remove(&RequestKey::RequestId(request_id))
                            {
                                // We don't care if the receiver has dropped their future
                                let _ = resolver.send(msg);
                            } else {
                                let res = self
                                    .consumers
                                    .get_mut(&consumer_id)
                                    .map(move |consumer| consumer.unbounded_send(msg));

                                if !res.as_ref().map(|r| r.is_ok()).unwrap_or(false) {
                                    error!("ConnectionReceiver: error transmitting message to consumer: {:?}", res);
                                }
                            }
                        }
                        None => {
                            warn!(
                                "Received unexpected message; dropping. Message {:?}",
                                msg.command
                            )
                        }
                    },
                },
                Poll::Ready(None) => {
                    self.error.set(ConnectionError::Disconnected);
                    return Poll::Ready(Err(()));
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => {
                    self.error.set(e);
                    return Poll::Ready(Err(()));
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
//#[derive(Clone)]
pub struct ConnectionSender<Exe: Executor> {
    tx: mpsc::UnboundedSender<Message>,
    registrations: mpsc::UnboundedSender<Register>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
    request_id: SerialId,
    error: SharedError,
    executor: Arc<Exe>,
    operation_timeout: Duration,
}

impl<Exe: Executor> ConnectionSender<Exe> {
    pub(crate) fn new(
        tx: mpsc::UnboundedSender<Message>,
        registrations: mpsc::UnboundedSender<Register>,
        receiver_shutdown: oneshot::Sender<()>,
        request_id: SerialId,
        error: SharedError,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> ConnectionSender<Exe> {
        ConnectionSender {
            tx,
            registrations,
            receiver_shutdown: Some(receiver_shutdown),
            request_id,
            error,
            executor,
            operation_timeout,
        }
    }

    pub(crate) async fn send(
        &self,
        producer_id: u64,
        producer_name: String,
        sequence_id: u64,
        message: producer::ProducerMessage,
    ) -> Result<proto::CommandSendReceipt, ConnectionError> {
        let key = RequestKey::ProducerSend {
            producer_id,
            sequence_id,
        };
        let msg = messages::send(producer_id, producer_name, sequence_id, message);
        self.send_message(msg, key, |resp| resp.command.send_receipt)
            .await
    }

    pub async fn send_ping(&self) -> Result<(), ConnectionError> {
        let (resolver, response) = oneshot::channel();
        trace!("sending ping");

        match (
            self.registrations
                .unbounded_send(Register::Ping { resolver }),
            self.tx.unbounded_send(messages::ping()),
        ) {
            (Ok(_), Ok(_)) => {
                let delay_f = self.executor.delay(self.operation_timeout);
                pin_mut!(response);
                pin_mut!(delay_f);

                match select(response, delay_f).await {
                    Either::Left((res, _)) => res
                        .map_err(|oneshot::Canceled| ConnectionError::Disconnected)
                        .map(move |_| trace!("received pong")),
                    Either::Right(_) => {
                        self.error.set(ConnectionError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "timeout when sending ping to the Pulsar server",
                        )));
                        Err(ConnectionError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "timeout when sending ping to the Pulsar server",
                        )))
                    }
                }
            }
            _ => Err(ConnectionError::Disconnected),
        }
    }

    pub async fn lookup_topic<S: Into<String>>(
        &self,
        topic: S,
        authoritative: bool,
    ) -> Result<proto::CommandLookupTopicResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::lookup_topic(topic.into(), authoritative, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.lookup_topic_response
        })
        .await
    }

    pub async fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<proto::CommandPartitionedTopicMetadataResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::lookup_partitioned_topic(topic.into(), request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.partition_metadata_response
        })
        .await
    }

    pub async fn get_last_message_id(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandGetLastMessageIdResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::get_last_message_id(consumer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.get_last_message_id_response
        }).await
    }

    pub async fn create_producer(
        &self,
        topic: String,
        producer_id: u64,
        producer_name: Option<String>,
        options: ProducerOptions,
    ) -> Result<proto::CommandProducerSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::create_producer(topic, producer_name, producer_id, request_id, options);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.producer_success
        })
        .await
    }

    pub async fn get_topics_of_namespace(
        &self,
        namespace: String,
        mode: proto::command_get_topics_of_namespace::Mode,
    ) -> Result<proto::CommandGetTopicsOfNamespaceResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::get_topics_of_namespace(request_id, namespace, mode);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.get_topics_of_namespace_response
        })
        .await
    }

    pub async fn close_producer(
        &self,
        producer_id: u64,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::close_producer(producer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
    }

    pub async fn subscribe(
        &self,
        resolver: mpsc::UnboundedSender<Message>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        consumer_id: u64,
        consumer_name: Option<String>,
        options: ConsumerOptions,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
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
                return Err(ConnectionError::Disconnected);
            }
        }
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
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

    pub async fn close_consumer(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::close_consumer(consumer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
    }

    pub async fn seek(
        &self,
        consumer_id: u64,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::seek(consumer_id, request_id, message_id, timestamp);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
    }

    pub async fn unsubscribe(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::unsubscribe(consumer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.success
        })
        .await
    }

    async fn send_message<R: Debug, F>(
        &self,
        msg: Message,
        key: RequestKey,
        extract: F,
    ) -> Result<R, ConnectionError>
    where
        F: FnOnce(Message) -> Option<R>,
    {
        let (resolver, response) = oneshot::channel();
        trace!("sending message(key = {:?}): {:?}", key, msg);

        let k = key.clone();
        let response = async {
            response
                .await
                .map_err(|oneshot::Canceled| {
                    self.error.set(ConnectionError::Disconnected);
                    ConnectionError::Disconnected
                })
                .map(move |message: Message| {
                    trace!("received message(key = {:?}): {:?}", k, message);
                    extract_message(message, extract)
                })?
        };

        match (
            self.registrations
                .unbounded_send(Register::Request { key, resolver }),
            self.tx.unbounded_send(msg),
        ) {
            (Ok(_), Ok(_)) => {
                let delay_f = self.executor.delay(self.operation_timeout);
                pin_mut!(response);
                pin_mut!(delay_f);

                match select(response, delay_f).await {
                    Either::Left((res, _)) => res,
                    Either::Right(_) => Err(ConnectionError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timeout sending message to the Pulsar server",
                    ))),
                }
            }
            _ => Err(ConnectionError::Disconnected),
        }
    }
}

pub struct Connection<Exe: Executor> {
    id: i64,
    url: Url,
    sender: ConnectionSender<Exe>,
}

impl<Exe: Executor> Connection<Exe> {
    pub async fn new(
        url: Url,
        auth_data: Option<Authentication>,
        proxy_to_broker_url: Option<String>,
        certificate_chain: &[Certificate],
        allow_insecure_connection: bool,
        tls_hostname_verification_enabled: bool,
        connection_timeout: Duration,
        operation_timeout: Duration,
        executor: Arc<Exe>,
    ) -> Result<Connection<Exe>, ConnectionError> {
        if url.scheme() != "pulsar" && url.scheme() != "pulsar+ssl" {
            error!("invalid scheme: {}", url.scheme());
            return Err(ConnectionError::NotFound);
        }
        let hostname = url.host().map(|s| s.to_string());

        let tls = match url.scheme() {
            "pulsar" => false,
            "pulsar+ssl" => true,
            s => {
                error!("invalid scheme: {}", s);
                return Err(ConnectionError::NotFound);
            }
        };

        let u = url.clone();
        let address: SocketAddr = match executor
            .spawn_blocking(move || {
                u.socket_addrs(|| match u.scheme() {
                    "pulsar" => Some(6650),
                    "pulsar+ssl" => Some(6651),
                    _ => None,
                })
                .map_err(|e| {
                    error!("could not look up address: {:?}", e);
                    e
                })
                .ok()
                .and_then(|v| {
                    let mut rng = thread_rng();
                    let index: usize = rng.gen_range(0..v.len());
                    v.get(index).copied()
                })
            })
            .await
        {
            Some(Some(address)) => address,
            _ =>
            //return Err(Error::Custom(format!("could not query address: {}", url))),
            {
                return Err(ConnectionError::NotFound)
            }
        };

        let hostname = hostname.unwrap_or_else(|| address.ip().to_string());

        debug!("Connecting to {}: {}", url, address);
        let sender_prepare = Connection::prepare_stream(
            address,
            hostname,
            tls,
            auth_data,
            proxy_to_broker_url,
            certificate_chain,
            allow_insecure_connection,
            tls_hostname_verification_enabled,
            executor.clone(),
            operation_timeout,
        );
        let delay_f = executor.delay(connection_timeout);

        pin_mut!(sender_prepare);
        pin_mut!(delay_f);

        let sender;
        match select(sender_prepare, delay_f).await {
            Either::Left((res, _)) => sender = res?,
            Either::Right(_) => {
                return Err(ConnectionError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timeout connecting to the Pulsar server",
                )));
            }
        };

        let id = rand::random();
        Ok(Connection { id, url, sender })
    }

    async fn prepare_stream(
        address: SocketAddr,
        hostname: String,
        tls: bool,
        auth_data: Option<Authentication>,
        proxy_to_broker_url: Option<String>,
        certificate_chain: &[Certificate],
        allow_insecure_connection: bool,
        tls_hostname_verification_enabled: bool,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> Result<ConnectionSender<Exe>, ConnectionError> {
        match executor.kind() {
            #[cfg(feature = "tokio-runtime")]
            ExecutorKind::Tokio => {
                if tls {
                    let stream = tokio::net::TcpStream::connect(&address).await?;

                    let mut builder = native_tls::TlsConnector::builder();
                    for certificate in certificate_chain {
                        builder.add_root_certificate(certificate.clone());
                    }
                    builder.danger_accept_invalid_hostnames(
                        allow_insecure_connection && !tls_hostname_verification_enabled,
                    );
                    builder.danger_accept_invalid_certs(allow_insecure_connection);
                    let cx = builder.build()?;
                    let cx = tokio_native_tls::TlsConnector::from(cx);
                    let stream = cx
                        .connect(&hostname, stream)
                        .await
                        .map(|stream| tokio_util::codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        stream,
                        auth_data,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                    )
                    .await
                } else {
                    let stream = tokio::net::TcpStream::connect(&address)
                        .await
                        .map(|stream| tokio_util::codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        stream,
                        auth_data,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                    )
                    .await
                }
            }
            #[cfg(not(feature = "tokio-runtime"))]
            ExecutorKind::Tokio => {
                unimplemented!("the tokio-runtime cargo feature is not active");
            }
            #[cfg(feature = "async-std-runtime")]
            ExecutorKind::AsyncStd => {
                if tls {
                    let stream = async_std::net::TcpStream::connect(&address).await?;
                    let mut connector = async_native_tls::TlsConnector::new();
                    for certificate in certificate_chain {
                        connector = connector.add_root_certificate(certificate.clone());
                    }
                    connector = connector.danger_accept_invalid_hostnames(
                        allow_insecure_connection && !tls_hostname_verification_enabled,
                    );
                    connector = connector.danger_accept_invalid_certs(allow_insecure_connection);
                    let stream = connector
                        .connect(&hostname, stream)
                        .await
                        .map(|stream| asynchronous_codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        stream,
                        auth_data,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                    )
                    .await
                } else {
                    let stream = async_std::net::TcpStream::connect(&address)
                        .await
                        .map(|stream| asynchronous_codec::Framed::new(stream, Codec))?;

                    Connection::connect(
                        stream,
                        auth_data,
                        proxy_to_broker_url,
                        executor,
                        operation_timeout,
                    )
                    .await
                }
            }
            #[cfg(not(feature = "async-std-runtime"))]
            ExecutorKind::AsyncStd => {
                unimplemented!("the async-std-runtime cargo feature is not active");
            }
        }
    }

    pub async fn connect<S>(
        mut stream: S,
        auth_data: Option<Authentication>,
        proxy_to_broker_url: Option<String>,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> Result<ConnectionSender<Exe>, ConnectionError>
    where
        S: Stream<Item = Result<Message, ConnectionError>>,
        S: Sink<Message, Error = ConnectionError>,
        S: Send + std::marker::Unpin + 'static,
    {
        let _ = stream
            .send({
                let msg = messages::connect(auth_data, proxy_to_broker_url);
                trace!("connection message: {:?}", msg);
                msg
            })
            .await?;

        let msg = stream.next().await;
        match msg {
            Some(Ok(Message {
                command:
                    proto::BaseCommand {
                        error: Some(error), ..
                    },
                ..
            })) => Err(ConnectionError::PulsarError(
                crate::error::server_error(error.error),
                Some(error.message),
            )),
            Some(Ok(msg)) => {
                let cmd = msg.command.clone();
                trace!("received connection response: {:?}", msg);
                msg.command.connected.ok_or_else(|| {
                    ConnectionError::Unexpected(format!(
                        "Unexpected message from pulsar: {:?}",
                        cmd
                    ))
                })
            }
            Some(Err(e)) => Err(e),
            None => Err(ConnectionError::Disconnected),
        }?;

        let (mut sink, stream) = stream.split();
        let (tx, mut rx) = mpsc::unbounded();
        let (registrations_tx, registrations_rx) = mpsc::unbounded();
        let error = SharedError::new();
        let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();

        if executor
            .spawn(Box::pin(
                Receiver::new(
                    stream,
                    tx.clone(),
                    error.clone(),
                    registrations_rx,
                    receiver_shutdown_rx,
                )
                .map(|_| ()),
            ))
            .is_err()
        {
            error!("the executor could not spawn the Receiver future");
            return Err(ConnectionError::Shutdown);
        }

        let err = error.clone();
        let res = executor.spawn(Box::pin(async move {
            while let Some(msg) = rx.next().await {
                if let Err(e) = sink.send(msg).await {
                    err.set(e);
                    break;
                }
            }
        }));
        if res.is_err() {
            error!("the executor could not spawn the Receiver future");
            return Err(ConnectionError::Shutdown);
        }

        let sender = ConnectionSender::new(
            tx,
            registrations_tx,
            receiver_shutdown_tx,
            SerialId::new(),
            error,
            executor.clone(),
            operation_timeout,
        );

        Ok(sender)
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn error(&self) -> Option<ConnectionError> {
        self.sender.error.remove()
    }

    pub fn is_valid(&self) -> bool {
        !self.sender.error.is_set()
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Chain to send a message, e.g. conn.sender().send_ping()
    pub fn sender(&self) -> &ConnectionSender<Exe> {
        &self.sender
    }
}

impl<Exe: Executor> Drop for Connection<Exe> {
    fn drop(&mut self) {
        trace!("dropping connection {} for {}", self.id, self.url);
        if let Some(shutdown) = self.sender.receiver_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

fn extract_message<T: Debug, F>(message: Message, extract: F) -> Result<T, ConnectionError>
where
    F: FnOnce(Message) -> Option<T>,
{
    if let Some(e) = message.command.error {
        Err(ConnectionError::PulsarError(
            crate::error::server_error(e.error),
            Some(e.message),
        ))
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
    use proto::MessageIdData;

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
                r#type: CommandType::Connect as i32,
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
                r#type: CommandType::Ping as i32,
                ping: Some(proto::CommandPing {}),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn pong() -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Pong as i32,
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
                r#type: CommandType::Producer as i32,
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
        mode: proto::command_get_topics_of_namespace::Mode,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::GetTopicsOfNamespace as i32,
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

    pub(crate) fn send(
        producer_id: u64,
        producer_name: String,
        sequence_id: u64,
        message: producer::ProducerMessage,
    ) -> Message {
        let properties = message
            .properties
            .into_iter()
            .map(|(key, value)| proto::KeyValue { key, value })
            .collect();

        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Send as i32,
                send: Some(proto::CommandSend {
                    producer_id,
                    sequence_id,
                    num_messages: message.num_messages_in_batch,
                    ..Default::default()
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
                    deliver_at_time: message.deliver_at_time,
                    ..Default::default()
                },
                data: message.payload,
            }),
        }
    }

    pub fn lookup_topic(topic: String, authoritative: bool, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Lookup as i32,
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
                r#type: CommandType::PartitionedMetadata as i32,
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

    pub fn get_last_message_id(consumer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::GetLastMessageId as i32,
                get_last_message_id: Some(proto::CommandGetLastMessageId {
                    consumer_id,
                    request_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn close_producer(producer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::CloseProducer as i32,
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
                r#type: CommandType::Subscribe as i32,
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
                    read_compacted: Some(options.read_compacted.unwrap_or(false)),
                    initial_position: Some(options.initial_position.into()),
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
                r#type: CommandType::Flow as i32,
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
                r#type: CommandType::Ack as i32,
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
                    ..Default::default()
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
                r#type: CommandType::RedeliverUnacknowledgedMessages as i32,
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
                r#type: CommandType::CloseConsumer as i32,
                close_consumer: Some(proto::CommandCloseConsumer {
                    consumer_id,
                    request_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn seek(
        consumer_id: u64,
        request_id: u64,
        message_id: Option<MessageIdData>,
        message_publish_time: Option<u64>,
    ) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Seek as i32,
                seek: Some(proto::CommandSeek {
                    consumer_id,
                    request_id,
                    message_id,
                    message_publish_time,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    pub fn unsubscribe(consumer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::Unsubscribe as i32,
                unsubscribe: Some(proto::CommandUnsubscribe {
                    consumer_id,
                    request_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}
