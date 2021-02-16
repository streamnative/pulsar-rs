use std::string::FromUtf8Error;
use std::sync::Arc;

use futures::channel::{mpsc, oneshot};

use crate::connection::Authentication;
use crate::connection_manager::{BackOffOptions, BrokerAddress, ConnectionManager, TlsOptions};
use crate::consumer::ConsumerBuilder;
use crate::error::Error;
use crate::executor::Executor;
use crate::message::proto::{self, CommandSendReceipt};
use crate::message::Payload;
use crate::producer::{self, ProducerBuilder, SendFuture};
use crate::service_discovery::ServiceDiscovery;
use futures::StreamExt;

/// Helper trait for consumer deserialization
pub trait DeserializeMessage {
    type Output: Sized;
    fn deserialize_message(payload: &Payload) -> Self::Output;
}

impl DeserializeMessage for Vec<u8> {
    type Output = Self;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        payload.data.to_vec()
    }
}

impl DeserializeMessage for String {
    type Output = Result<String, FromUtf8Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        String::from_utf8(payload.data.to_vec())
    }
}

/// Helper trait for message serialization
pub trait SerializeMessage {
    fn serialize_message(input: Self) -> Result<producer::Message, Error>;
}

impl SerializeMessage for producer::Message {
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        Ok(input)
    }
}

impl<'a> SerializeMessage for &'a [u8] {
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        Ok(producer::Message {
            payload: input.to_vec(),
            ..Default::default()
        })
    }
}

impl SerializeMessage for Vec<u8> {
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        Ok(producer::Message {
            payload: input,
            ..Default::default()
        })
    }
}

impl SerializeMessage for String {
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        let payload = input.into_bytes();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl<'a> SerializeMessage for &String {
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        let payload = input.as_bytes().to_vec();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl<'a> SerializeMessage for &'a str {
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
/// # async fn run(auth: pulsar::Authentication, backoff: pulsar::BackOffOptions) -> Result<(), pulsar::Error> {
/// let addr = "pulsar://127.0.0.1:6650";
/// // you can indicate which executor you use as the return type of client creation
/// let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor)
///     .with_auth(auth)
///     .with_back_off_options(backoff)
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
    pub(crate) executor: Arc<Exe>,
}

impl<Exe: Executor> Pulsar<Exe> {
    /// creates a new client
    pub(crate) async fn new<S: Into<String>>(
        url: S,
        auth: Option<Authentication>,
        backoff_parameters: Option<BackOffOptions>,
        tls_options: Option<TlsOptions>,
        executor: Exe,
    ) -> Result<Self, Error> {
        let url: String = url.into();
        let executor = Arc::new(executor);
        let manager =
            ConnectionManager::new(url, auth, backoff_parameters, tls_options, executor.clone())
                .await?;
        let manager = Arc::new(manager);
        let service_discovery = Arc::new(ServiceDiscovery::with_manager(manager.clone()));
        let (producer, producer_rx) = mpsc::unbounded();

        let mut client = Pulsar {
            manager,
            service_discovery,
            producer: None,
            executor,
        };

        let _ = client
            .executor
            .spawn(Box::pin(run_producer(client.clone(), producer_rx)));
        client.producer = Some(producer);
        Ok(client)
    }

    /// creates a new client builder
    pub fn builder<S: Into<String>>(url: S, executor: Exe) -> PulsarBuilder<Exe> {
        PulsarBuilder {
            url: url.into(),
            auth: None,
            back_off_options: None,
            tls_options: None,
            executor,
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
    pub fn producer(&self) -> ProducerBuilder<Exe> {
        ProducerBuilder::new(self)
    }

    /// gets the address of a broker handling the topic
    pub async fn lookup_topic<S: Into<String>>(&self, topic: S) -> Result<BrokerAddress, Error> {
        self.service_discovery
            .lookup_topic(topic)
            .await
            .map_err(|e| e.into())
    }

    /// gets the number of partitions for a partitioned topic
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
    pub async fn send<S: Into<String>, M: SerializeMessage + Sized>(
        &self,
        topic: S,
        message: M,
    ) -> Result<SendFuture, Error> {
        let message = M::serialize_message(message)?;
        self.send_raw(message, topic).await
    }

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
}

/// Helper structure to generate a [Pulsar] client
pub struct PulsarBuilder<Exe: Executor> {
    url: String,
    auth: Option<Authentication>,
    back_off_options: Option<BackOffOptions>,
    tls_options: Option<TlsOptions>,
    executor: Exe,
}

impl<Exe: Executor> PulsarBuilder<Exe> {
    /// Authentication parameters (JWT, Biscuit, etc)
    pub fn with_auth(self, auth: Authentication) -> Self {
        PulsarBuilder {
            url: self.url,
            auth: Some(auth),
            back_off_options: self.back_off_options,
            tls_options: self.tls_options,
            executor: self.executor,
        }
    }

    /// Exponential back off parameters for automatic reconnection
    pub fn with_back_off_options(self, back_off_options: BackOffOptions) -> Self {
        PulsarBuilder {
            url: self.url,
            auth: self.auth,
            back_off_options: Some(back_off_options),
            tls_options: self.tls_options,
            executor: self.executor,
        }
    }

    /// add a custom certificate chain to authenticate the server in TLS connectioons
    pub fn with_certificate_chain(self, certificate_chain: Vec<u8>) -> Self {
        PulsarBuilder {
            url: self.url,
            auth: self.auth,
            back_off_options: self.back_off_options,
            tls_options: Some(TlsOptions {
                certificate_chain: Some(certificate_chain),
            }),
            executor: self.executor,
        }
    }

    /// add a custom certificate chain from a file to authenticate the server in TLS connectioons
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

    /// creates the Pulsar client and connects it
    pub async fn build(self) -> Result<Pulsar<Exe>, Error> {
        let PulsarBuilder {
            url,
            auth,
            back_off_options,
            tls_options,
            executor,
        } = self;
        Pulsar::new(url, auth, back_off_options, tls_options, executor).await
    }
}

struct SendMessage {
    topic: String,
    message: producer::Message,
    resolver: oneshot::Sender<Result<CommandSendReceipt, Error>>,
}

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
        match producer.send(topic, payload).await {
            Ok(future) => {
                let _ = client.executor.spawn(Box::pin(async move {
                    let _ = resolver.send(future.await);
                }));
            }
            Err(e) => {
                let _ = resolver.send(Err(e));
            }
        }
    }
}
