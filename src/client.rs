use std::net::SocketAddr;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;

use futures::future;
use trust_dns_resolver::{config::*, TokioAsyncResolver};

use crate::connection::{Authentication, Connection};
use crate::connection_manager::{BrokerAddress, ConnectionManager};
use crate::consumer::{Consumer, ConsumerBuilder, ConsumerOptions, MultiTopicConsumer, Unset};
use crate::error::{Error, ServiceDiscoveryError};
use crate::executor::{Executor, TaskExecutor};
use crate::message::proto::{self, command_subscribe::SubType, CommandSendReceipt};
use crate::message::Payload;
use crate::producer::{self, Producer, ProducerOptions, TopicProducer};
use crate::service_discovery::ServiceDiscovery;

/// Helper trait for consumer deserialization
pub trait DeserializeMessage {
    type Output: Sized;
    fn deserialize_message(payload: Payload) -> Self::Output;
}

impl DeserializeMessage for Payload {
    type Output = Self;

    fn deserialize_message(payload: Payload) -> Self::Output {
        payload
    }
}

impl DeserializeMessage for Vec<u8> {
    type Output = Self;

    fn deserialize_message(payload: Payload) -> Self::Output {
        payload.data
    }
}

impl DeserializeMessage for String {
    type Output = Result<String, FromUtf8Error>;

    fn deserialize_message(payload: Payload) -> Self::Output {
        String::from_utf8(payload.data)
    }
}

pub trait SerializeMessage {
    fn serialize_message(input: &Self) -> Result<producer::Message, Error>;
}

impl SerializeMessage for [u8] {
    fn serialize_message(input: &Self) -> Result<producer::Message, Error> {
        //TODO figure out how to avoid copying here
        Ok(producer::Message {
            payload: input.to_vec(),
            ..Default::default()
        })
    }
}

impl SerializeMessage for String {
    fn serialize_message(input: &Self) -> Result<producer::Message, Error> {
        let payload = input.as_bytes().to_vec();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl SerializeMessage for str {
    fn serialize_message(input: &Self) -> Result<producer::Message, Error> {
        let payload = input.as_bytes().to_vec();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

//TODO add more DeserializeMessage impls

#[derive(Clone)]
pub struct Pulsar {
    manager: Arc<ConnectionManager>,
    service_discovery: Arc<ServiceDiscovery>,
    executor: TaskExecutor,
}

impl Pulsar {
    pub async fn new<E: Executor + 'static, S: Into<String>>(
        addr: S,
        auth: Option<Authentication>,
        executor: E,
    ) -> Result<Self, Error> {
        let executor = TaskExecutor::new(executor);
        let addr: String = addr.into();

        let address: SocketAddr = match addr.parse::<SocketAddr>() {
            Ok(a) => a,
            Err(_) => {
                let mut it = addr.split(':');
                let host = it.next().ok_or(Error::Custom("invalid address".to_string()))?;
                let port = match it.next() {
                    Some(s) => s.parse().map_err(|e| Error::Custom(format!("invalid port: {:?}", e)))?,
                    None => 6650,
                };

                let resolver = TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default()).await
                .map_err(|e| {
                    Error::Custom(format!("could not create DNS resolver: {:?}", e))
                })?;

                let ip = resolver.lookup_ip(host).await.map_err(|_| ServiceDiscoveryError::DnsLookupError)?
                    .iter().next().ok_or(ServiceDiscoveryError::DnsLookupError)?;
                SocketAddr::new(ip, port)
            }
        };

        let conn = Connection::new(address.to_string(), auth.clone(), None, executor.clone()).await?;
        let manager = ConnectionManager::from_connection(conn, auth, address, executor.clone())?;
        let manager = Arc::new(manager);
        let service_discovery = Arc::new(ServiceDiscovery::with_manager(
                manager.clone(),
                executor.clone(),
        ));
        Ok(Pulsar {
            manager,
            service_discovery,
            executor,
        })
    }

    pub async fn lookup_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<BrokerAddress, Error> {
        self.service_discovery.lookup_topic(topic).await.map_err(|e| e.into())
    }

    /// get the number of partitions for a partitioned topic
    pub async fn lookup_partitioned_topic_number<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<u32, Error> {
        self.service_discovery.lookup_partitioned_topic_number(topic).await
            .map_err(|e| e.into())
    }

    pub async fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<Vec<(String, BrokerAddress)>, Error> {
        self.service_discovery
            .lookup_partitioned_topic(topic).await
            .map_err(|e| e.into())
    }

    pub async fn get_topics_of_namespace(
        &self,
        namespace: String,
        mode: proto::get_topics::Mode,
    ) -> Result<Vec<String>, Error> {
        let conn = self.manager
            .get_base_connection().await?;
        let topics = conn.sender().get_topics_of_namespace(namespace, mode).await?;
        Ok(topics.topics)
    }

    pub fn consumer(&self) -> ConsumerBuilder<Unset, Unset, Unset> {
        ConsumerBuilder::new(self)
    }

    pub fn create_multi_topic_consumer<T, S1, S2>(
        &self,
        topic_regex: regex::Regex,
        subscription: S1,
        namespace: S2,
        sub_type: SubType,
        topic_refresh: Duration,
        unacked_message_resend_delay: Option<Duration>,
        options: ConsumerOptions,
    ) -> MultiTopicConsumer<T>
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
            options,
        )
    }

    pub async fn create_consumer<T, S1, S2>(
        &self,
        topic: S1,
        subscription: S2,
        sub_type: SubType,
        batch_size: Option<u32>,
        consumer_name: Option<String>,
        consumer_id: Option<u64>,
        unacked_message_redelivery_delay: Option<Duration>,
        options: ConsumerOptions,
    ) -> Result<Consumer<T>, Error>
    where
        T: DeserializeMessage,
        S1: Into<String>,
        S2: Into<String>,
    {
        let manager = self.manager.clone();
        let topic = topic.into();

        let broker_address = self.service_discovery
            .lookup_topic(topic.clone()).await?;
        let conn = manager.get_connection(&broker_address).await?;
        Consumer::from_connection(
            conn,
            topic,
            subscription.into(),
            sub_type,
            consumer_id,
            consumer_name,
            batch_size,
            unacked_message_redelivery_delay,
            options,
        ).await
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

        let v = self.service_discovery
            .lookup_partitioned_topic(topic).await?;

        let mut res = Vec::new();
        for (topic, broker_address) in v.iter() {
                let subscription = subscription.clone();
                let options = options.clone();

                let conn = manager.get_connection(&broker_address).await?;
                res.push(Consumer::from_connection(
                    conn,
                    topic.to_string(),
                    subscription.into(),
                    sub_type,
                    None,
                    None,
                    None,
                    None, //TODO make configurable
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
    ) -> Result<TopicProducer, Error> {
        let manager = self.manager.clone();
        let topic = topic.into();
        let broker_address = self.service_discovery
            .lookup_topic(topic.clone()).await?;
        let conn = manager.get_connection(&broker_address).await?;

        TopicProducer::from_connection::<_>(conn, topic, name, options).await
    }

    pub async fn create_partitioned_producers<S: Into<String>>(
        &self,
        topic: S,
        options: ProducerOptions,
    ) -> Result<Vec<TopicProducer>, Error> {
        let manager = self.manager.clone();

        let v = self.service_discovery
            .lookup_partitioned_topic(topic).await?;

        let mut res = Vec::new();
        for (topic, broker_address) in v.iter() {
            let conn = manager.get_connection(&broker_address).await?;
            res.push(TopicProducer::from_connection(conn, topic, None, options.clone()));
        }

        future::try_join_all(res).await
    }

    pub async fn send<S: Into<String>, M: SerializeMessage + ?Sized>(
        &self,
        topic: S,
        message: &M,
        options: ProducerOptions,
    ) -> Result<CommandSendReceipt, Error> {
        match M::serialize_message(message) {
            Ok(message) => self.send_raw::<S>(message, topic, options).await,
            Err(e) => Err(e),
        }
    }

    pub async fn send_raw<S: Into<String>>(
        &self,
        message: producer::Message,
        topic: S,
        options: ProducerOptions,
    ) -> Result<CommandSendReceipt, Error> {
        let producer = self.create_producer(topic, None, options).await?;
        producer.send_raw(message).await
    }

    pub fn producer(&self, options: Option<ProducerOptions>) -> Producer {
        Producer::new(self.clone(), options.unwrap_or_default())
    }

    pub(crate) fn executor(&self) -> &TaskExecutor {
        &self.executor
    }
}
