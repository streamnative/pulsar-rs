use std::net::SocketAddr;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;

use futures::{
    future::{self, Either},
    Future,
};
use tokio::runtime::TaskExecutor;

use crate::connection::{Authentication, ConnectionOptions};
use crate::connection_manager::{BrokerAddress, ConnectionManager, ConnectionManagerOptions};
use crate::consumer::{Consumer, ConsumerBuilder, ConsumerOptions, MultiTopicConsumer, Unset};
use crate::error::Error;
use crate::proto::{self, Payload, command_subscribe::SubType, CommandSendReceipt, AuthMethod};
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
    fn serialize_message(input: Self) -> Result<producer::Message, Error>;
}

impl SerializeMessage for Vec<u8> {
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        Ok(producer::Message {
            payload: input,
            ..Default::default()
        })
    }
}

impl SerializeMessage for &[u8] {
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        Ok(producer::Message {
            payload: input.to_vec(),
            ..Default::default()
        })
    }
}

impl SerializeMessage for String {
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        Ok(producer::Message {
            payload: input.into_bytes(),
            ..Default::default()
        })
    }
}

impl SerializeMessage for &str {
    fn serialize_message(input: Self) -> Result<producer::Message, Error> {
        let payload = input.as_bytes().to_vec();
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

pub struct ClientBuilder {
    address: String,
    connection_options: ConnectionOptions,
    connection_manager_options: ConnectionManagerOptions,
}

impl ClientBuilder {
    pub fn with_auth(mut self, auth: Authentication) -> Self {
        self.connection_options.auth_data = Some(auth.data);
        self.connection_options.auth_method_name = Some(auth.name);
        self
    }

    pub fn with_proxy_to_broker_url(mut self, broker_url: String) -> Self {
        self.connection_options.proxy_to_broker_url = Some(broker_url);
        self
    }

    pub fn with_proactive_reconnect(mut self, proactive_reconnect: bool) -> Self {
        self.connection_manager_options.proactive_reconnect = Some(proactive_reconnect);
        self
    }

    pub fn with_reconnect_backoff(mut self, backoff: Duration) -> Self {
        self.connection_manager_options.reconnect_backoff = Some(backoff);
        self
    }

    pub fn build(self) -> (impl Future<Output=Result<Client, Error>>, impl Future<Output=()>) {
        let ClientBuilder { address, connection_options, connection_manager_options} = self;
        let conn = ConnectionManager::new(address, Some(connection_options), Some(connection_manager_options));
        let producers =
    }
}

pub struct Client {

}

impl Client {
    pub fn new(address: String) -> ClientBuilder {
        let
    }
}

//TODO add more DeserializeMessage impls
//
//#[derive(Clone)]
//pub struct Pulsar {
//    manager: Arc<ConnectionManager>,
//    service_discovery: Arc<ServiceDiscovery>,
//    executor: TaskExecutor,
//}
//
//impl Pulsar {
//    pub fn new(
//        addr: SocketAddr,
//        auth: Option<Authentication>,
//        executor: TaskExecutor,
//    ) -> impl Future<Item = Self, Error = Error> {
//        ConnectionManager::new(addr, auth.clone(), executor.clone())
//            .from_err()
//            .map(|manager| {
//                let manager = Arc::new(manager);
//                let service_discovery = Arc::new(ServiceDiscovery::with_manager(
//                    manager.clone(),
//                    executor.clone(),
//                ));
//                Pulsar {
//                    manager,
//                    service_discovery,
//                    executor,
//                }
//            })
//    }
//
//    pub fn lookup_topic<S: Into<String>>(
//        &self,
//        topic: S,
//    ) -> impl Future<Item = BrokerAddress, Error = Error> {
//        self.service_discovery.lookup_topic(topic).from_err()
//    }
//
//    /// get the number of partitions for a partitioned topic
//    pub fn lookup_partitioned_topic_number<S: Into<String>>(
//        &self,
//        topic: S,
//    ) -> impl Future<Item = u32, Error = Error> {
//        self.service_discovery
//            .lookup_partitioned_topic_number(topic)
//            .from_err()
//    }
//
//    pub fn lookup_partitioned_topic<S: Into<String>>(
//        &self,
//        topic: S,
//    ) -> impl Future<Item = Vec<(String, BrokerAddress)>, Error = Error> {
//        self.service_discovery
//            .lookup_partitioned_topic(topic)
//            .from_err()
//    }
//
//    pub fn get_topics_of_namespace(
//        &self,
//        namespace: String,
//        mode: proto::get_topics::Mode,
//    ) -> impl Future<Item = Vec<String>, Error = Error> {
//        self.manager
//            .get_base_connection()
//            .and_then(move |conn| conn.sender().get_topics_of_namespace(namespace, mode))
//            .from_err()
//            .map(|topics| topics.topics)
//    }
//
//    pub fn consumer(&self) -> ConsumerBuilder<Unset, Unset, Unset> {
//        ConsumerBuilder::new(self)
//    }
//
//    pub fn create_multi_topic_consumer<T, S1, S2>(
//        &self,
//        topic_regex: regex::Regex,
//        subscription: S1,
//        namespace: S2,
//        sub_type: SubType,
//        topic_refresh: Duration,
//        unacked_message_resend_delay: Option<Duration>,
//        options: ConsumerOptions,
//    ) -> MultiTopicConsumer<T>
//    where
//        T: DeserializeMessage,
//        S1: Into<String>,
//        S2: Into<String>,
//    {
//        MultiTopicConsumer::new(
//            self.clone(),
//            namespace.into(),
//            topic_regex,
//            subscription.into(),
//            sub_type,
//            topic_refresh,
//            unacked_message_resend_delay,
//            options,
//        )
//    }
//
//    pub fn create_consumer<T, S1, S2>(
//        &self,
//        topic: S1,
//        subscription: S2,
//        sub_type: SubType,
//        batch_size: Option<u32>,
//        consumer_name: Option<String>,
//        consumer_id: Option<u64>,
//        unacked_message_redelivery_delay: Option<Duration>,
//        options: ConsumerOptions,
//    ) -> impl Future<Item = Consumer<T>, Error = Error>
//    where
//        T: DeserializeMessage,
//        S1: Into<String>,
//        S2: Into<String>,
//    {
//        let manager = self.manager.clone();
//        let topic = topic.into();
//
//        self.service_discovery
//            .lookup_topic(topic.clone())
//            .from_err()
//            .and_then(move |broker_address| manager.get_connection(&broker_address).from_err())
//            .and_then(move |conn| {
//                Consumer::from_connection(
//                    conn,
//                    topic,
//                    subscription.into(),
//                    sub_type,
//                    consumer_id,
//                    consumer_name,
//                    batch_size,
//                    unacked_message_redelivery_delay,
//                    options,
//                )
//                .from_err()
//            })
//    }
//
//    pub fn create_partitioned_consumers<
//        T: DeserializeMessage + Sized,
//        S1: Into<String> + Clone,
//        S2: Into<String> + Clone,
//    >(
//        &self,
//        topic: S1,
//        subscription: S2,
//        sub_type: SubType,
//        options: ConsumerOptions,
//    ) -> impl Future<Item = Vec<Consumer<T>>, Error = Error> {
//        let manager = self.manager.clone();
//
//        self.service_discovery
//            .lookup_partitioned_topic(topic.clone())
//            .from_err()
//            .and_then(move |v| {
//                let res =
//                    v.iter()
//                        .cloned()
//                        .map(|(topic, broker_address)| {
//                            let subscription = subscription.clone();
//                            let options = options.clone();
//
//                            manager.get_connection(&broker_address).from_err().and_then(
//                                move |conn| {
//                                    Consumer::from_connection(
//                                        conn,
//                                        topic.to_string(),
//                                        subscription.into(),
//                                        sub_type,
//                                        None,
//                                        None,
//                                        None,
//                                        None, //TODO make configurable
//                                        options,
//                                    )
//                                    .from_err()
//                                },
//                            )
//                        })
//                        .collect::<Vec<_>>();
//
//                future::join_all(res)
//            })
//    }
//
//    pub fn create_producer<S: Into<String>>(
//        &self,
//        topic: S,
//        name: Option<String>,
//        options: ProducerOptions,
//    ) -> impl Future<Item = TopicProducer, Error = Error> {
//        let manager = self.manager.clone();
//        let topic = topic.into();
//        self.service_discovery
//            .lookup_topic(topic.clone())
//            .from_err()
//            .and_then(move |broker_address| manager.get_connection(&broker_address).from_err())
//            .and_then(move |conn| {
//                TopicProducer::from_connection(conn, topic, name, options).from_err()
//            })
//    }
//
//    pub fn create_partitioned_producers<S: Into<String> + Clone>(
//        &self,
//        topic: S,
//        options: ProducerOptions,
//    ) -> impl Future<Item = Vec<TopicProducer>, Error = Error> {
//        let manager = self.manager.clone();
//
//        self.service_discovery
//            .lookup_partitioned_topic(topic.clone())
//            .from_err()
//            .and_then(move |v| {
//                let res = v
//                    .iter()
//                    .cloned()
//                    .map(|(topic, broker_address)| {
//                        let options = options.clone();
//                        manager
//                            .get_connection(&broker_address)
//                            .from_err()
//                            .and_then(move |conn| {
//                                TopicProducer::from_connection(conn, topic, None, options.clone())
//                                    .from_err()
//                            })
//                    })
//                    .collect::<Vec<_>>();
//
//                future::join_all(res)
//            })
//    }
//
//    pub fn send<S: Into<String>, M: SerializeMessage>(
//        &self,
//        topic: S,
//        message: &M,
//        options: ProducerOptions,
//    ) -> impl Future<Item = CommandSendReceipt, Error = Error> {
//        match M::serialize_message(message) {
//            Ok(message) => Either::A(self.send_raw(message, topic, options)),
//            Err(e) => Either::B(future::failed(e.into())),
//        }
//    }
//
//    pub fn send_raw<S: Into<String>>(
//        &self,
//        message: producer::Message,
//        topic: S,
//        options: ProducerOptions,
//    ) -> impl Future<Item = CommandSendReceipt, Error = Error> {
//        self.create_producer(topic, None, options)
//            .and_then(|producer| producer.send_raw(message).from_err())
//    }
//
//    pub fn producer(&self, options: Option<ProducerOptions>) -> Producer {
//        Producer::new(self.clone(), options.unwrap_or_default())
//    }
//
//    pub(crate) fn executor(&self) -> &TaskExecutor {
//        &self.executor
//    }
//}
