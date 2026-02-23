use std::{string::FromUtf8Error, sync::Arc};

use futures::{
    channel::{mpsc, oneshot},
    future::{select, Either},
    lock::Mutex,
    pin_mut, StreamExt,
};

use crate::{
    connection::Authentication,
    connection_manager::{
        BrokerAddress, ConnectionManager, ConnectionRetryOptions, OperationRetryOptions, TlsOptions,
    },
    consumer::{ConsumerBuilder, ConsumerOptions, InitialPosition},
    error::{ConnectionError, Error},
    executor::Executor,
    message::{
        proto::{self, CommandSendReceipt},
        Payload,
    },
    producer::{self, ProducerBuilder, SendFuture},
    service_discovery::ServiceDiscovery,
};

/// Helper trait for consumer deserialization
pub trait DeserializeMessage {
    /// type produced from the message
    type Output: Sized;
    /// deserialize method that will be called by the consumer
    fn deserialize_message(payload: &Payload) -> Self::Output;
}

impl DeserializeMessage for Vec<u8> {
    type Output = Self;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn deserialize_message(payload: &Payload) -> Self::Output {
        payload.data.to_vec()
    }
}

impl DeserializeMessage for String {
    type Output = Result<String, FromUtf8Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn deserialize_message(payload: &Payload) -> Self::Output {
        String::from_utf8(payload.data.to_vec())
    }
}

/// Helper trait for message serialization
pub trait SerializeMessage {
    /// serialize method that will be called by the producer
    fn serialize_message(input: Self) -> Result<producer::Message, Error>;
}

impl SerializeMessage for producer::Message {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        Ok(input)
    }
}

impl SerializeMessage for () {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn serialize_message(_input: Self) -> Result<producer::Message, Error> {
        Ok(producer::Message {
            ..Default::default()
        })
    }
}

impl SerializeMessage for &[u8] {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        Ok(producer::Message {
            payload: input.to_vec(),
            ..Default::default()
        })
    }
}

impl SerializeMessage for Vec<u8> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        Ok(producer::Message {
            payload: input,
            ..Default::default()
        })
    }
}

impl SerializeMessage for String {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        let payload = input.into_bytes();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl SerializeMessage for &String {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        let payload = input.as_bytes().to_vec();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl SerializeMessage for &str {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        let payload = input.as_bytes().to_vec();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

/// Pulsar client
///
/// This is the starting point of this API, used to create connections, producers and consumers
///
/// While methods are provided to create the client, producers and consumers directly,
/// the builders should be used for more clarity:
///
/// ```rust,no_run
/// use pulsar::{Pulsar, TokioExecutor};
///
/// # async fn run(auth: pulsar::Authentication, retry: pulsar::ConnectionRetryOptions) -> Result<(), pulsar::Error> {
/// let addr = "pulsar://127.0.0.1:6650";
/// // you can indicate which executor you use as the return type of client creation
/// let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor)
///     .with_auth(auth)
///     .with_connection_retry_options(retry)
///     .build()
///     .await?;
///
/// let mut producer = pulsar
///     .producer()
///     .with_topic("non-persistent://public/default/test")
///     .with_name("my producer")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Pulsar<Exe: Executor> {
    pub(crate) manager: Arc<ConnectionManager<Exe>>,
    service_discovery: Arc<ServiceDiscovery<Exe>>,
    // this field is an Option to avoid a cyclic dependency between Pulsar
    // and run_producer: the run_producer loop needs a client to create
    // a multitopic producer, this producer stores internally a copy
    // of the Pulsar struct. So even if we drop the main Pulsar instance,
    // the run_producer loop still lives because it contains a copy of
    // the sender it waits on.
    // o,solve this, we create a client without this sender, use it in
    // run_producer, then fill in the producer field afterwards in the
    // main Pulsar instance
    producer: Option<mpsc::UnboundedSender<SendMessage>>,
    pub(crate) operation_retry_options: OperationRetryOptions,
    pub(crate) executor: Arc<Exe>,
    /// Admin REST API base URL, set via [`PulsarBuilder::with_admin_url`].
    pub(crate) admin_url: Option<String>,
}

impl<Exe: Executor> Pulsar<Exe> {
    /// creates a new client
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn new<S: Into<String>>(
        url: S,
        auth: Option<Arc<Mutex<Box<dyn crate::authentication::Authentication>>>>,
        connection_retry_parameters: Option<ConnectionRetryOptions>,
        operation_retry_parameters: Option<OperationRetryOptions>,
        tls_options: Option<TlsOptions>,
        outbound_channel_size: Option<usize>,
        executor: Exe,
    ) -> Result<Self, Error> {
        let url: String = url.into();
        let executor = Arc::new(executor);
        let operation_retry_options = operation_retry_parameters.unwrap_or_default();
        let outbound_channel_size = outbound_channel_size.unwrap_or(100);
        let manager = ConnectionManager::new(
            url,
            auth,
            connection_retry_parameters,
            operation_retry_options.clone(),
            tls_options,
            outbound_channel_size,
            executor.clone(),
        )
        .await?;
        let manager = Arc::new(manager);

        // set up a regular connection check
        let weak_manager = Arc::downgrade(&manager);
        let mut interval = executor.interval(std::time::Duration::from_secs(60));
        let res = executor.spawn(Box::pin(async move {
            while let Some(()) = interval.next().await {
                if let Some(strong_manager) = weak_manager.upgrade() {
                    strong_manager.check_connections().await;
                } else {
                    // if all the strong references to the manager were dropped,
                    // we can stop the task
                    break;
                }
            }
        }));
        if res.is_err() {
            error!("the executor could not spawn the check connection task");
            return Err(crate::error::ConnectionError::Shutdown.into());
        }

        let service_discovery = Arc::new(ServiceDiscovery::with_manager(manager.clone()));
        let (producer, producer_rx) = mpsc::unbounded();

        let mut client = Pulsar {
            manager,
            service_discovery,
            producer: None,
            operation_retry_options,
            executor,
            admin_url: None,
        };

        let _ = client
            .executor
            .spawn(Box::pin(run_producer(client.clone(), producer_rx)));
        client.producer = Some(producer);
        Ok(client)
    }

    /// creates a new client builder
    ///
    /// ```rust,no_run
    /// use pulsar::{Pulsar, TokioExecutor};
    ///
    /// # async fn run() -> Result<(), pulsar::Error> {
    /// let addr = "pulsar://127.0.0.1:6650";
    /// // you can indicate which executor you use as the return type of client creation
    /// let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn builder<S: Into<String>>(url: S, executor: Exe) -> PulsarBuilder<Exe> {
        PulsarBuilder {
            url: url.into(),
            auth_provider: None,
            connection_retry_options: None,
            operation_retry_options: None,
            tls_options: None,
            outbound_channel_size: None,
            executor,
            admin_url: None,
        }
    }

    /// creates a consumer builder
    ///
    /// ```rust,no_run
    /// use pulsar::{SubType, Consumer};
    ///
    /// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// # type TestData = String;
    /// let mut consumer: Consumer<TestData, _> = pulsar
    ///     .consumer()
    ///     .with_topic("non-persistent://public/default/test")
    ///     .with_consumer_name("test_consumer")
    ///     .with_subscription_type(SubType::Exclusive)
    ///     .with_subscription("test_subscription")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn consumer(&self) -> ConsumerBuilder<Exe> {
        ConsumerBuilder::new(self)
    }

    /// creates a producer builder
    ///
    /// ```rust,no_run
    /// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let mut producer = pulsar
    ///     .producer()
    ///     .with_topic("non-persistent://public/default/test")
    ///     .with_name("my producer")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn producer(&self) -> ProducerBuilder<Exe> {
        ProducerBuilder::new(self)
    }

    /// creates a reader builder
    /// ```rust, no_run
    /// use pulsar::reader::Reader;
    ///
    /// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// # type TestData = String;
    /// let mut reader: Reader<TestData, _> = pulsar
    ///     .reader()
    ///     .with_topic("non-persistent://public/default/test")
    ///     .with_consumer_name("my_reader")
    ///     .into_reader()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn reader(&self) -> ConsumerBuilder<Exe> {
        // this makes it exactly the same like the consumer() method though
        ConsumerBuilder::new(self).with_options(
            ConsumerOptions::default()
                .durable(false)
                .with_initial_position(InitialPosition::Latest),
        )
    }

    /// gets the address of a broker handling the topic
    ///
    /// ```rust,no_run
    /// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let broker_address = pulsar.lookup_topic("persistent://public/default/test").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn lookup_topic<S: Into<String>>(&self, topic: S) -> Result<BrokerAddress, Error> {
        self.service_discovery
            .lookup_topic(topic)
            .await
            .map_err(|e| e.into())
    }

    /// gets the number of partitions for a partitioned topic
    ///
    /// ```rust,no_run
    /// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let nb = pulsar.lookup_partitioned_topic_number("persistent://public/default/test").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn lookup_partitioned_topic_number<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<u32, Error> {
        self.service_discovery
            .lookup_partitioned_topic_number(topic)
            .await
            .map_err(|e| e.into())
    }

    /// gets the address of brokers handling the topic's partitions. If the topic is not
    /// a partitioned topic, result will be a single element containing the topic and address
    /// of the non-partitioned topic provided.
    ///
    /// ```rust,no_run
    /// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let broker_addresses = pulsar.lookup_partitioned_topic("persistent://public/default/test").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<Vec<(String, BrokerAddress)>, Error> {
        self.service_discovery
            .lookup_partitioned_topic(topic)
            .await
            .map_err(|e| e.into())
    }

    /// gets the list of topics from a namespace
    ///
    /// ```rust,no_run
    /// use pulsar::message::proto::command_get_topics_of_namespace::Mode;
    ///
    /// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let topics = pulsar.get_topics_of_namespace("public/default".to_string(), Mode::Persistent).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_topics_of_namespace(
        &self,
        namespace: String,
        mode: proto::command_get_topics_of_namespace::Mode,
    ) -> Result<Vec<String>, Error> {
        let conn = self.manager.get_base_connection().await?;
        let topics = conn
            .sender()
            .get_topics_of_namespace(namespace, mode)
            .await?;
        Ok(topics.topics)
    }

    /// Sends a message on a topic.
    ///
    /// This function will lazily initialize and re-use producers as needed. For better
    /// control over producers, creating and using a `Producer` is recommended.
    ///
    /// ```rust,no_run
    /// use pulsar::message::proto::command_get_topics_of_namespace::Mode;
    ///
    /// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let topics = pulsar.send("persistent://public/default/test", "hello world!").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send<S: Into<String>, M: SerializeMessage + Sized>(
        &self,
        topic: S,
        message: M,
    ) -> Result<SendFuture, Error> {
        let message = M::serialize_message(message)?;
        self.send_raw(message, topic).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_raw<S: Into<String>>(
        &self,
        message: producer::Message,
        topic: S,
    ) -> Result<SendFuture, Error> {
        let (resolver, future) = oneshot::channel();
        self.producer
            .as_ref()
            .expect("a client without the producer channel should only be used internally")
            .unbounded_send(SendMessage {
                topic: topic.into(),
                message,
                resolver,
            })
            .map_err(|_| Error::Custom("producer unexpectedly disconnected".into()))?;
        Ok(SendFuture(future))
    }

    /// Creates an [`AdminClient`][crate::AdminClient] for this cluster.
    ///
    /// The admin client reuses the TLS and authentication configuration
    /// already present on this `Pulsar` instance. Requires one of the
    /// `admin-api` feature flags and a tokio runtime.
    ///
    /// # Arguments
    ///
    /// * `admin_url` — base URL of the Pulsar admin HTTP endpoint, e.g.
    ///   `"http://pulsar-proxy"`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let admin = pulsar.admin("http://pulsar-proxy")?;
    /// admin
    ///     .set_max_unacked_messages_per_consumer(
    ///         "persistent://public/default/my-topic",
    ///         500,
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "admin-api")]
    pub fn admin(&self, admin_url: impl Into<String>) -> Result<crate::AdminClient, Error> {
        crate::admin::AdminClient::new(
            admin_url.into(),
            &self.manager.tls_options,
            self.manager.auth.clone(),
        )
    }
}

/// Helper structure to generate a [Pulsar] client
pub struct PulsarBuilder<Exe: Executor> {
    url: String,
    auth_provider: Option<Box<dyn crate::authentication::Authentication>>,
    connection_retry_options: Option<ConnectionRetryOptions>,
    operation_retry_options: Option<OperationRetryOptions>,
    tls_options: Option<TlsOptions>,
    outbound_channel_size: Option<usize>,
    executor: Exe,
    admin_url: Option<String>,
}

impl<Exe: Executor> PulsarBuilder<Exe> {
    /// Authentication parameters (JWT, Biscuit, etc)
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_auth(self, auth: Authentication) -> Self {
        self.with_auth_provider(Box::new(auth))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_auth_provider(
        mut self,
        auth: Box<dyn crate::authentication::Authentication>,
    ) -> Self {
        self.auth_provider = Some(auth);
        self
    }

    /// Exponential back off parameters for automatic reconnection
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_connection_retry_options(
        mut self,
        connection_retry_options: ConnectionRetryOptions,
    ) -> Self {
        self.connection_retry_options = Some(connection_retry_options);
        self
    }

    /// Retry parameters for Pulsar operations
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_operation_retry_options(
        mut self,
        operation_retry_options: OperationRetryOptions,
    ) -> Self {
        self.operation_retry_options = Some(operation_retry_options);
        self
    }

    /// add a custom certificate chain to authenticate the server in TLS connections
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_certificate_chain(mut self, certificate_chain: Vec<u8>) -> Self {
        match &mut self.tls_options {
            Some(tls) => tls.certificate_chain = Some(certificate_chain),
            None => {
                self.tls_options = Some(TlsOptions {
                    certificate_chain: Some(certificate_chain),
                    ..Default::default()
                })
            }
        }
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_allow_insecure_connection(mut self, allow: bool) -> Self {
        match &mut self.tls_options {
            Some(tls) => tls.allow_insecure_connection = allow,
            None => {
                self.tls_options = Some(TlsOptions {
                    allow_insecure_connection: allow,
                    ..Default::default()
                })
            }
        }
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_tls_hostname_verification_enabled(mut self, enabled: bool) -> Self {
        match &mut self.tls_options {
            Some(tls) => tls.tls_hostname_verification_enabled = enabled,
            None => {
                self.tls_options = Some(TlsOptions {
                    tls_hostname_verification_enabled: enabled,
                    ..Default::default()
                })
            }
        }
        self
    }

    /// add a custom certificate chain from a file to authenticate the server in TLS connections
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_certificate_chain_file<P: AsRef<std::path::Path>>(
        self,
        path: P,
    ) -> Result<Self, std::io::Error> {
        use std::io::Read;

        let mut file = std::fs::File::open(path)?;
        let mut v = vec![];
        file.read_to_end(&mut v)?;

        Ok(self.with_certificate_chain(v))
    }

    /// The internal pending queue size for each producer on a topic partition. (default: 100)
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_outbound_channel_size(mut self, size: usize) -> Self {
        self.outbound_channel_size = Some(size);
        self
    }

    /// Sets the base URL of the Pulsar Admin HTTP endpoint (e.g. `"http://localhost:8080"`).
    ///
    /// When set, consumer builders can automatically apply broker-side topic
    /// policies (such as [`with_max_unacked_messages_per_consumer`][crate::ConsumerBuilder::with_max_unacked_messages_per_consumer])
    /// immediately after subscription.
    ///
    /// Requires the `admin-api` feature.
    #[cfg(feature = "admin-api")]
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_admin_url<S: Into<String>>(mut self, admin_url: S) -> Self {
        self.admin_url = Some(admin_url.into());
        self
    }

    /// creates the Pulsar client and connects it
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn build(self) -> Result<Pulsar<Exe>, Error> {
        let PulsarBuilder {
            url,
            auth_provider,
            connection_retry_options,
            operation_retry_options,
            tls_options,
            outbound_channel_size,
            executor,
            admin_url,
        } = self;

        let mut pulsar = Pulsar::new(
            url,
            auth_provider.map(|p| Arc::new(Mutex::new(p))),
            connection_retry_options,
            operation_retry_options,
            tls_options,
            outbound_channel_size,
            executor,
        )
        .await?;

        pulsar.admin_url = admin_url;
        Ok(pulsar)
    }
}

struct SendMessage {
    topic: String,
    message: producer::Message,
    resolver: oneshot::Sender<Result<CommandSendReceipt, Error>>,
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
async fn run_producer<Exe: Executor>(
    client: Pulsar<Exe>,
    mut messages: mpsc::UnboundedReceiver<SendMessage>,
) {
    let mut producer = client.producer().build_multi_topic();
    while let Some(SendMessage {
        topic,
        message: payload,
        resolver,
    }) = messages.next().await
    {
        match producer.send_non_blocking(topic, payload).await {
            Ok(send_f) => {
                let delay_f = client
                    .executor
                    .delay(client.operation_retry_options.operation_timeout);

                let _ = client.executor.spawn(Box::pin(async move {
                    pin_mut!(delay_f);
                    match select(send_f, delay_f).await {
                        Either::Left((res, _)) => {
                            let _ = resolver.send(res);
                        }
                        Either::Right(_) => {
                            let _ = resolver.send(Err(Error::from(ConnectionError::Io(
                                std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "client sink timed out when sending message to the Pulsar server",
                                ),
                            ))));
                        }
                    }
                }));
            }
            Err(e) => {
                let _ = resolver.send(Err(e));
            }
        }
    }
}
