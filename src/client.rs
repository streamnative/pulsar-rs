use std::marker::PhantomData;
use std::string::FromUtf8Error;
use std::sync::Arc;

use futures::channel::{oneshot, mpsc};

use crate::connection::Authentication;
use crate::connection_manager::{BackOffOptions, BrokerAddress, ConnectionManager, TlsOptions};
use crate::consumer::ConsumerBuilder;
use crate::error::Error;
use crate::executor::Executor;
use crate::message::proto::{self, CommandSendReceipt};
use crate::message::Payload;
use crate::producer::{self, MultiTopicProducer, ProducerBuilder, ProducerOptions, SendFuture};
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
/// ```rust,ignore
/// let addr = "pulsar://127.0.0.1:6650";
/// // you can indicate which executor you use as the return type of client creation
/// let pulsar: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await?;
/// let mut producer = pulsar
///     .producer()
///     .with_topic("non-persistent://public/default/test")
///     .with_name("my producer")
///     .build()
///     .await?;
/// ```
#[derive(Clone)]
pub struct Pulsar<E: Executor + ?Sized> {
    pub(crate) manager: Arc<ConnectionManager<E>>,
    service_discovery: Arc<ServiceDiscovery<E>>,
    producer: mpsc::UnboundedSender<SendMessage>,
}

impl<Exe: Executor> Pulsar<Exe> {
    /// creates a new client
    pub async fn new<S: Into<String>>(
        url: S,
        auth: Option<Authentication>,
        backoff_parameters: Option<BackOffOptions>,
        tls_options: Option<TlsOptions>,
    ) -> Result<Self, Error> {
        let url: String = url.into();
        let manager = ConnectionManager::new(url, auth, backoff_parameters, tls_options).await?;
        let manager = Arc::new(manager);
        let service_discovery = Arc::new(ServiceDiscovery::with_manager(manager.clone()));
        let (producer, producer_rx) = mpsc::unbounded();
        let client = Pulsar {
            manager,
            service_discovery,
            producer
        };
        let _ = Exe::spawn(Box::pin(run_producer(client.clone(), producer_rx)));
        Ok(client)
    }

    /// creates a new client builder
    pub fn builder<S: Into<String>>(url: S) -> PulsarBuilder<Exe> {
        PulsarBuilder {
            url: url.into(),
            auth: None,
            back_off_options: None,
            tls_options: None,
            executor: PhantomData,
        }
    }

    /// creates a consumer builder
    ///
    /// ```rust,ignore
    /// let addr = "pulsar://127.0.0.1:6650";
    /// let pulsar: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await?;
    ///
    /// let mut consumer: Consumer<TestData> = pulsar
    /// .consumer()
    /// .with_topic("non-persistent://public/default/test")
    /// .with_consumer_name("test_consumer")
    /// .with_subscription_type(SubType::Exclusive)
    /// .with_subscription("test_subscription")
    /// .build()
    /// .await?;
    /// ```
    pub fn consumer(&self) -> ConsumerBuilder<Exe> {
        ConsumerBuilder::new(self)
    }

    /// creates a producer builder
    ///
    /// ```rust,ignore
    /// let addr = "pulsar://127.0.0.1:6650";
    /// let pulsar: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await?;
    ///
    /// let mut producer = pulsar
    ///     .producer()
    ///     .with_topic("non-persistent://public/default/test")
    ///     .with_name("my producer")
    ///     .build()
    ///     .await?;
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

    /// gets the address of brokers handling the topic's partitions
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
        mode: proto::get_topics::Mode,
    ) -> Result<Vec<String>, Error> {
        let conn = self.manager.get_base_connection().await?;
        let topics = conn
            .sender()
            .get_topics_of_namespace(namespace, mode)
            .await?;
        Ok(topics.topics)
    }


    /// sends one message on a topic (not recommended for multiple messages)
    ///
    /// this function will create a producer, send the message then drop the producer
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
        self.producer.unbounded_send(SendMessage {
            topic: topic.into(),
            message,
            resolver
        }).map_err(|_| Error::Custom("producer unexpectedly disconnected".into()))?;
        Ok(SendFuture(future))
    }
}

/// Helper structure to generate a [Pulsar] client
pub struct PulsarBuilder<Exe> {
    url: String,
    auth: Option<Authentication>,
    back_off_options: Option<BackOffOptions>,
    tls_options: Option<TlsOptions>,
    executor: PhantomData<Exe>,
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
            executor: _,
        } = self;
        Pulsar::new(url, auth, back_off_options, tls_options).await
    }
}

struct SendMessage {
    topic: String,
    message: producer::Message,
    resolver: oneshot::Sender<Result<CommandSendReceipt, Error>>,
}

async fn run_producer<Exe: Executor + ?Sized>(client: Pulsar<Exe>, mut messages: mpsc::UnboundedReceiver<SendMessage>) {
    let mut producer = MultiTopicProducer::new(client, ProducerOptions::default());
    while let Some(SendMessage { topic, message: payload, resolver }) = messages.next().await {
        match producer.send(topic, payload).await {
            Ok(future) => {
                let _ = Exe::spawn(Box::pin(async move {
                    let _ = resolver.send(future.await);
                }));
            }
            Err(e) => {
                let _ = resolver.send(Err(e));
            }
        }

    }
}
