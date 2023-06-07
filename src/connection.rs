use std::{
    collections::BTreeMap,
    fmt::Debug,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::{
    self,
    channel::{mpsc, oneshot},
    future::{select, Either},
    lock::Mutex,
    pin_mut,
    task::{Context, Poll},
    Future, FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use native_tls::Certificate;
use proto::MessageIdData;
use rand::{seq::SliceRandom, thread_rng};
use url::Url;
use uuid::Uuid;

use crate::{
    consumer::ConsumerOptions,
    error::{AuthenticationError, ConnectionError, SharedError},
    executor::{Executor, ExecutorKind},
    message::{
        proto::{self, command_subscribe::SubType},
        BaseCommand, Codec, Message,
    },
    producer::{self, ProducerOptions},
};

pub(crate) enum Register {
    Request {
        key: RequestKey,
        resolver: oneshot::Sender<Message>,
    },
    Consumer {
        consumer_id: u64,
        resolver: mpsc::UnboundedSender<Message>,
    },
    RemoveConsumer {
        consumer_id: u64,
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
    AuthChallenge,
}

/// Authentication parameters
#[derive(Clone)]
pub struct Authentication {
    /// Authentication kid. Use "token" for JWT
    pub name: String,
    /// Authentication data
    pub data: Vec<u8>,
}

#[async_trait]
impl crate::authentication::Authentication for Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn auth_method_name(&self) -> String {
        self.name.clone()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn initialize(&mut self) -> Result<(), AuthenticationError> {
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
        Ok(self.data.clone())
    }
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
    auth_challenge: mpsc::UnboundedSender<()>,
}

impl<S: Stream<Item = Result<Message, ConnectionError>>> Receiver<S> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(
        inbound: S,
        outbound: mpsc::UnboundedSender<Message>,
        error: SharedError,
        registrations: mpsc::UnboundedReceiver<Register>,
        shutdown: oneshot::Receiver<()>,
        auth_challenge: mpsc::UnboundedSender<()>,
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
            auth_challenge,
        }
    }
}

impl<S: Stream<Item = Result<Message, ConnectionError>>> Future for Receiver<S> {
    type Output = Result<(), ()>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shutdown.as_mut().poll(cx) {
            Poll::Ready(Ok(())) | Poll::Ready(Err(futures::channel::oneshot::Canceled)) => {
                return Poll::Ready(Err(()));
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
                Poll::Ready(Some(Register::RemoveConsumer { consumer_id })) => {
                    self.consumers.remove(&consumer_id);
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
                            trace!("received this message: {:?}", msg);
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
                        Some(RequestKey::AuthChallenge) => {
                            debug!("Received AuthChallenge");
                            let _ = self.auth_challenge.unbounded_send(());
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
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn default() -> Self {
        SerialId(Arc::new(AtomicUsize::new(0)))
    }
}

impl SerialId {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn get(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) as u64
    }
}

/// An owned type that can send messages like a connection
//#[derive(Clone)]
pub struct ConnectionSender<Exe: Executor> {
    connection_id: Uuid,
    tx: mpsc::UnboundedSender<Message>,
    registrations: mpsc::UnboundedSender<Register>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
    request_id: SerialId,
    error: SharedError,
    executor: Arc<Exe>,
    operation_timeout: Duration,
}

impl<Exe: Executor> ConnectionSender<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) fn new(
        connection_id: Uuid,
        tx: mpsc::UnboundedSender<Message>,
        registrations: mpsc::UnboundedSender<Register>,
        receiver_shutdown: oneshot::Sender<()>,
        request_id: SerialId,
        error: SharedError,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> ConnectionSender<Exe> {
        ConnectionSender {
            connection_id,
            tx,
            registrations,
            receiver_shutdown: Some(receiver_shutdown),
            request_id,
            error,
            executor,
            operation_timeout,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_ping(&self) -> Result<(), ConnectionError> {
        let (resolver, response) = oneshot::channel();
        trace!("sending ping to connection {}", self.connection_id);

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
                        .map_err(|oneshot::Canceled| {
                            self.error.set(ConnectionError::Disconnected);
                            ConnectionError::Disconnected
                        })
                        .map(move |_| trace!("received pong from {}", self.connection_id)),
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_consumer_stats(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandConsumerStatsResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::consumer_stats(request_id, consumer_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.consumer_stats_response
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandGetLastMessageIdResponse, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::get_last_message_id(consumer_id, request_id);
        self.send_message(msg, RequestKey::RequestId(request_id), |resp| {
            resp.command.get_last_message_id_response
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn wait_for_exclusive_access(
        &self,
        request_id: u64,
    ) -> Result<proto::CommandProducerSuccess, ConnectionError> {
        self.wait_exclusive_access(RequestKey::RequestId(request_id), |resp| {
            resp.command.producer_success
        })
        .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn send_flow(&self, consumer_id: u64, message_permits: u32) -> Result<(), ConnectionError> {
        self.tx
            .unbounded_send(messages::flow(consumer_id, message_permits))
            .map_err(|_| ConnectionError::Disconnected)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close_consumer(
        &self,
        consumer_id: u64,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let request_id = self.request_id.get();
        let msg = messages::close_consumer(consumer_id, request_id);
        match self
            .registrations
            .unbounded_send(Register::RemoveConsumer { consumer_id })
        {
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn auth_challenge(
        &self,
        auth_data: Authentication,
    ) -> Result<proto::CommandSuccess, ConnectionError> {
        let msg = messages::auth_challenge(auth_data);
        self.send_message(msg, RequestKey::AuthChallenge, |resp| resp.command.success)
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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
                    Either::Left((res, _)) => {
                        // println!("recv msg: {:?}", res);
                        res
                    }
                    Either::Right(_) => {
                        warn!(
                            "connection {} timedout sending message to the Pulsar server",
                            self.connection_id
                        );
                        self.error.set(ConnectionError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                " connection {} timedout sending message to the Pulsar server",
                                self.connection_id
                            ),
                        )));
                        Err(ConnectionError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                " connection {} timedout sending message to the Pulsar server",
                                self.connection_id
                            ),
                        )))
                    }
                }
            }
            _ => {
                warn!(
                    "connection {} disconnected sending message to the Pulsar server",
                    self.connection_id
                );
                self.error.set(ConnectionError::Disconnected);
                Err(ConnectionError::Disconnected)
            }
        }
    }

    /// wait for desired message(commandproducersuccess with ready field true)
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn wait_exclusive_access<R: Debug, F>(
        &self,
        key: RequestKey,
        extract: F,
    ) -> Result<R, ConnectionError>
    where
        F: FnOnce(Message) -> Option<R>,
    {
        let (resolver, response) = oneshot::channel();

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

        match self
            .registrations
            .unbounded_send(Register::Request { key, resolver })
        {
            Ok(_) => {
                //there should be no timeout for this message
                pin_mut!(response);
                response.await
            }
            _ => Err(ConnectionError::Disconnected),
        }
    }
}

pub struct Connection<Exe: Executor> {
    id: Uuid,
    url: Url,
    sender: ConnectionSender<Exe>,
}

impl<Exe: Executor> Connection<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn new(
        url: Url,
        auth_data: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
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
        let addresses: Vec<SocketAddr> = match executor
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
                .map(|mut v| {
                    v.shuffle(&mut thread_rng());
                    v
                })
            })
            .await
        {
            Some(Some(addresses)) if !addresses.is_empty() => addresses,
            _ => return Err(ConnectionError::NotFound),
        };

        let id = Uuid::new_v4();
        let mut errors = vec![];
        for address in addresses {
            let hostname = hostname.clone().unwrap_or_else(|| address.ip().to_string());
            debug!("Connecting to {}: {}, as {}", url, address, id);
            let sender_prepare = Connection::prepare_stream(
                id,
                address,
                hostname,
                tls,
                auth_data.clone(),
                proxy_to_broker_url.clone(),
                certificate_chain,
                allow_insecure_connection,
                tls_hostname_verification_enabled,
                executor.clone(),
                operation_timeout,
            );
            let delay_f = executor.delay(connection_timeout);

            pin_mut!(sender_prepare);
            pin_mut!(delay_f);

            let sender = match select(sender_prepare, delay_f).await {
                Either::Left((Ok(res), _)) => res,
                Either::Left((Err(err), _)) => {
                    errors.push(err);
                    continue;
                }
                Either::Right(_) => {
                    errors.push(ConnectionError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "timeout connecting to the Pulsar server",
                    )));
                    continue;
                }
            };

            return Ok(Connection { id, url, sender });
        }

        let mut fatal_errors = vec![];
        let mut retryable_errors = vec![];
        for e in errors.into_iter() {
            if e.establish_retryable() {
                retryable_errors.push(e);
            } else {
                fatal_errors.push(e);
            }
        }

        if retryable_errors.is_empty() {
            error!("connection error, not retryable: {:?}", fatal_errors);
            Err(ConnectionError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "fatal error when connecting to the Pulsar server",
            )))
        } else {
            warn!("retry establish connection on: {:?}", retryable_errors);
            Err(retryable_errors.into_iter().next().unwrap())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn prepare_auth_data(
        auth: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
    ) -> Result<Option<Authentication>, ConnectionError> {
        match auth {
            Some(m_auth) => {
                let mut auth_guard = m_auth.lock().await;
                Ok(Some(Authentication {
                    name: auth_guard.auth_method_name(),
                    data: auth_guard.auth_data().await?,
                }))
            }
            None => Ok(None),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn prepare_stream(
        connection_id: Uuid,
        address: SocketAddr,
        hostname: String,
        tls: bool,
        auth: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
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
                        connection_id,
                        stream,
                        auth,
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
                        connection_id,
                        stream,
                        auth,
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
                        connection_id,
                        stream,
                        auth,
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
                        connection_id,
                        stream,
                        auth,
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn connect<S>(
        connection_id: Uuid,
        mut stream: S,
        auth: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
        proxy_to_broker_url: Option<String>,
        executor: Arc<Exe>,
        operation_timeout: Duration,
    ) -> Result<ConnectionSender<Exe>, ConnectionError>
    where
        S: Stream<Item = Result<Message, ConnectionError>>,
        S: Sink<Message, Error = ConnectionError>,
        S: Send + std::marker::Unpin + 'static,
    {
        let auth_data = Self::prepare_auth_data(auth.clone()).await?;
        stream
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
                    ConnectionError::Unexpected(format!("Unexpected message from pulsar: {cmd:?}"))
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
        let (auth_challenge_tx, mut auth_challenge_rx) = mpsc::unbounded();

        if executor
            .spawn(Box::pin(
                Receiver::new(
                    stream,
                    tx.clone(),
                    error.clone(),
                    registrations_rx,
                    receiver_shutdown_rx,
                    auth_challenge_tx,
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
                // println!("real sent msg: {:?}", msg);
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

        if auth.is_some() {
            let auth_challenge_res = executor.spawn({
                let err = error.clone();
                let mut tx = tx.clone();
                let auth = auth.clone();
                Box::pin(async move {
                    while auth_challenge_rx.next().await.is_some() {
                        match Self::prepare_auth_data(auth.clone()).await {
                            Ok(Some(auth_data)) => {
                                let _ = tx.send(messages::auth_challenge(auth_data)).await;
                            }
                            Ok(None) => (),
                            Err(e) => {
                                err.set(e);
                                break;
                            }
                        }
                    }
                })
            });
            if auth_challenge_res.is_err() {
                error!("the executor could not spawn the auth_challenge future");
                return Err(ConnectionError::Shutdown);
            }
        }

        let sender = ConnectionSender::new(
            connection_id,
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn id(&self) -> Uuid {
        self.id
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn error(&self) -> Option<ConnectionError> {
        self.sender.error.remove()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn is_valid(&self) -> bool {
        !self.sender.error.is_set()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Chain to send a message, e.g. conn.sender().send_ping()
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn sender(&self) -> &ConnectionSender<Exe> {
        &self.sender
    }
}

impl<Exe: Executor> Drop for Connection<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn drop(&mut self) {
        debug!("dropping connection {} for {}", self.id, self.url);
        if let Some(shutdown) = self.sender.receiver_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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
            Err(ConnectionError::UnexpectedResponse(format!("{cmd:?}")))
        }
    }
}

pub(crate) mod messages {
    use chrono::Utc;
    use proto::MessageIdData;

    use crate::{
        connection::Authentication,
        consumer::ConsumerOptions,
        message::{
            proto::{self, base_command::Type as CommandType, command_subscribe::SubType},
            Message, Payload,
        },
        producer::{self, ProducerOptions},
    };

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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
                    client_version: proto::client_version(),
                    protocol_version: Some(12),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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
                    producer_access_mode: options.access_mode,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn consumer_stats(request_id: u64, consumer_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::ConsumerStats as i32,
                consumer_stats: Some(proto::CommandConsumerStats {
                    request_id,
                    consumer_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
            payload: None,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn auth_challenge(auth: Authentication) -> Message {
        Message {
            command: proto::BaseCommand {
                r#type: CommandType::AuthResponse as i32,
                auth_response: Some(proto::CommandAuthResponse {
                    response: Some(proto::AuthData {
                        auth_method_name: Some(auth.name),
                        auth_data: Some(auth.data),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use futures::{
        channel::{mpsc, oneshot},
        lock::Mutex,
        stream::StreamExt,
        SinkExt,
    };
    use tokio::{net::TcpListener, sync::RwLock};
    use uuid::Uuid;

    use super::{Connection, Receiver};
    use crate::{
        authentication::Authentication,
        error::{AuthenticationError, SharedError},
        message::{BaseCommand, Codec, Message},
        proto::{AuthData, CommandAuthChallenge, CommandAuthResponse, CommandConnected},
        TokioExecutor,
    };

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn receiver_auth_challenge_test() {
        let (message_tx, message_rx) = mpsc::unbounded();
        let (tx, _) = mpsc::unbounded();
        let (_registrations_tx, registrations_rx) = mpsc::unbounded();
        let error = SharedError::new();
        let (_receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
        let (auth_challenge_tx, mut auth_challenge_rx) = mpsc::unbounded();

        let _ = message_tx.unbounded_send(Ok(Message {
            command: BaseCommand {
                r#type: 36,
                auth_challenge: Some(CommandAuthChallenge {
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }));

        tokio::spawn(Box::pin(Receiver::new(
            message_rx,
            tx,
            error.clone(),
            registrations_rx,
            receiver_shutdown_rx,
            auth_challenge_tx,
        )));

        if error.is_set() {
            panic!("{:?}", error.remove())
        }

        match tokio::time::timeout(Duration::from_secs(1), auth_challenge_rx.next()).await {
            Ok(auth) => assert!(auth.is_some()),
            _ => panic!("operation timeout"),
        };
    }

    struct TestAuthentication {
        count: Arc<RwLock<usize>>,
    }

    #[async_trait]
    impl Authentication for TestAuthentication {
        fn auth_method_name(&self) -> String {
            "test_auth".to_string()
        }
        async fn initialize(&mut self) -> Result<(), AuthenticationError> {
            Ok(())
        }
        async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
            *self.count.write().await += 1;
            Ok("test_auth_data".as_bytes().to_vec())
        }
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn connection_auth_challenge_test() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

        let stream = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .map(|stream| tokio_util::codec::Framed::new(stream, Codec))
            .unwrap();

        let mut server_stream = listener
            .accept()
            .await
            .map(|(stream, _)| tokio_util::codec::Framed::new(stream, Codec))
            .unwrap();

        let auth_count = Arc::new(RwLock::new(0));

        server_stream
            .send(Message {
                command: BaseCommand {
                    connected: Some(CommandConnected {
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                payload: None,
            })
            .await
            .unwrap();

        server_stream
            .send(Message {
                command: BaseCommand {
                    r#type: 36,
                    auth_challenge: Some(CommandAuthChallenge {
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                payload: None,
            })
            .await
            .unwrap();

        let connection = Connection::connect(
            Uuid::new_v4(),
            stream,
            Some(Arc::new(Mutex::new(Box::new(TestAuthentication {
                count: auth_count.clone(),
            })))),
            None,
            TokioExecutor.into(),
            Duration::from_secs(10),
        )
        .await;

        let _ = server_stream.next().await;
        let auth_challenge = server_stream.next().await.unwrap();

        assert!(connection.is_ok());
        assert_eq!(*auth_count.read().await, 2);
        match auth_challenge {
            Ok(Message {
                command:
                    BaseCommand {
                        auth_response:
                            Some(CommandAuthResponse {
                                response:
                                    Some(AuthData {
                                        auth_method_name,
                                        auth_data,
                                    }),
                                ..
                            }),
                        ..
                    },
                ..
            }) => {
                assert_eq!(auth_method_name.unwrap(), "test_auth");
                assert_eq!(
                    String::from_utf8(auth_data.unwrap()).unwrap(),
                    "test_auth_data"
                );
            }
            _ => panic!("Unexpected message"),
        };
    }
}
