use std::marker::PhantomData;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::oneshot;
use futures::future;

use crate::connection::Authentication;
use crate::connection_manager::{BackOffOptions, BrokerAddress, ConnectionManager, TlsOptions};
use crate::consumer::{
    Consumer, ConsumerBuilder, ConsumerOptions, DeadLetterPolicy, MultiTopicConsumer, Unset,
};
use crate::error::Error;
use crate::executor::Executor;
use crate::message::proto::{self, command_subscribe::SubType, CommandSendReceipt};
use crate::message::Payload;
use crate::producer::{self, MultiTopicProducer, Producer, ProducerBuilder, ProducerOptions};
use crate::service_discovery::ServiceDiscovery;

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
        Ok(Pulsar {
            manager,
            service_discovery,
        })
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
    pub fn consumer(&self) -> ConsumerBuilder<Unset, Unset, Unset, Exe> {
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
    pub fn producer(&self) -> ProducerBuilder<Unset, Exe> {
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

    pub fn create_multi_topic_consumer<T, S1, S2>(
        &self,
        topic_regex: regex::Regex,
        subscription: S1,
        namespace: S2,
        sub_type: SubType,
        topic_refresh: Duration,
        unacked_message_resend_delay: Option<Duration>,
        dead_letter_policy: Option<DeadLetterPolicy>,
        options: ConsumerOptions,
    ) -> MultiTopicConsumer<T, Exe>
    where
        T: DeserializeMessage,
        S1: Into<String>,
        S2: Into<String>,
    {
        MultiTopicConsumer::new(
            self.clone(),
            namespace.into(),
            topic_regex,
            subscription.into(),
            sub_type,
            topic_refresh,
            unacked_message_resend_delay,
            dead_letter_policy,
            options,
        )
    }

    /// creates a consumer
    pub async fn create_consumer<T, S1, S2>(
        &self,
        topic: S1,
        subscription: S2,
        sub_type: SubType,
        batch_size: Option<u32>,
        consumer_name: Option<String>,
        consumer_id: Option<u64>,
        unacked_message_redelivery_delay: Option<Duration>,
        dead_letter_policy: Option<DeadLetterPolicy>,
        options: ConsumerOptions,
    ) -> Result<Consumer<T>, Error>
    where
        T: DeserializeMessage,
        S1: Into<String>,
        S2: Into<String>,
    {
        let manager = self.manager.clone();
        let topic = topic.into();

        let broker_address = self.service_discovery.lookup_topic(topic.clone()).await?;
        let conn = manager.get_connection(&broker_address).await?;
        Consumer::from_connection(
            self.clone(),
            conn,
            topic,
            subscription.into(),
            sub_type,
            consumer_id,
            consumer_name,
            batch_size,
            unacked_message_redelivery_delay,
            dead_letter_policy,
            options,
        )
        .await
    }

    pub async fn create_partitioned_consumers<
        T: DeserializeMessage + Sized,
        S1: Into<String> + Clone,
        S2: Into<String> + Clone,
    >(
        &self,
        topic: S1,
        subscription: S2,
        sub_type: SubType,
        options: ConsumerOptions,
    ) -> Result<Vec<Consumer<T>>, Error> {
        let manager = self.manager.clone();

        let v = self
            .service_discovery
            .lookup_partitioned_topic(topic)
            .await?;

        let mut res = Vec::new();
        for (topic, broker_address) in v.iter() {
            let subscription = subscription.clone();
            let options = options.clone();

            let conn = manager.get_connection(&broker_address).await?;
            res.push(Consumer::from_connection(
                self.clone(),
                conn,
                topic.to_string(),
                subscription.into(),
                sub_type,
                None,
                None,
                None,
                None, //TODO make configurable
                None,
                options,
            ))
        }

        future::try_join_all(res).await
    }

    pub async fn create_producer<S: Into<String>>(
        &self,
        topic: S,
        name: Option<String>,
        options: ProducerOptions,
    ) -> Result<Producer<Exe>, Error> {
        let manager = self.manager.clone();
        let topic = topic.into();
        let broker_address = self.service_discovery.lookup_topic(topic.clone()).await?;
        let conn = manager.get_connection(&broker_address).await?;

        Producer::from_connection::<_>(self.clone(), conn, topic, name, options).await
    }

    pub async fn create_partitioned_producers<S: Into<String>>(
        &self,
        topic: S,
        options: ProducerOptions,
    ) -> Result<Vec<Producer<Exe>>, Error> {
        let manager = self.manager.clone();

        let v = self
            .service_discovery
            .lookup_partitioned_topic(topic)
            .await?;

        let mut res = Vec::new();
        for (topic, broker_address) in v.iter() {
            let conn = manager.get_connection(&broker_address).await?;
            res.push(Producer::from_connection::<_>(
                self.clone(),
                conn,
                topic,
                None,
                options.clone(),
            ));
        }

        future::try_join_all(res).await
    }

    /// sends one mssage on a topic (not recommended for multiple messages)
    ///
    /// this function will create a producer, send the message then drop the producer
    pub async fn send<S: Into<String>, M: SerializeMessage + Sized>(
        &self,
        topic: S,
        message: M,
        options: ProducerOptions,
    ) -> Result<oneshot::Receiver<CommandSendReceipt>, Error> {
        match M::serialize_message(message) {
            Ok(message) => self.send_raw::<S>(message, topic, options).await,
            Err(e) => Err(e),
        }
    }

    async fn send_raw<S: Into<String>>(
        &self,
        message: producer::Message,
        topic: S,
        options: ProducerOptions,
    ) -> Result<oneshot::Receiver<CommandSendReceipt>, Error> {
        let mut producer = self.create_producer(topic, None, options).await?;
        let rx = producer.send_raw(message.into()).await?;
        Ok(rx)
    }

    pub fn create_multi_topic_producer(
        &self,
        options: Option<ProducerOptions>,
    ) -> MultiTopicProducer<Exe> {
        MultiTopicProducer::new(self.clone(), options.unwrap_or_default())
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
