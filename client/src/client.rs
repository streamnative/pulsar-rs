use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{
    future::{self, Either, join_all},
    Future,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::runtime::TaskExecutor;

use crate::connection::Authentication;
use crate::connection_manager::{BrokerAddress, ConnectionManager};
use crate::consumer::{Consumer, MultiTopicConsumer, Unset};
use crate::error::{ConsumerError, Error};
use crate::message::Payload;
use crate::message::proto::{command_subscribe::SubType, CommandSendReceipt};
use crate::producer::Producer;
use crate::service_discovery::ServiceDiscovery;
use crate::ConsumerBuilder;
use std::string::FromUtf8Error;

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

impl DeserializeMessage for serde_json::Value {
    type Output = Result<serde_json::Value, serde_json::Error>;

    fn deserialize_message(payload: Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

impl DeserializeMessage for String {
    type Output = Result<String, FromUtf8Error>;

    fn deserialize_message(payload: Payload) -> Self::Output {
        String::from_utf8(payload.data)
    }
}

//TODO add more DeserializeMessage impls

#[derive(Clone)]
pub struct Pulsar {
    manager: Arc<ConnectionManager>,
    service_discovery: Arc<ServiceDiscovery>,
}

impl Pulsar {
    pub fn new(
        addr: SocketAddr,
        auth: Option<Authentication>,
        executor: TaskExecutor,
    ) -> impl Future<Item=Self, Error=Error> {
        ConnectionManager::new(addr, auth.clone(), executor.clone())
            .from_err()
            .map(|manager| {
                let manager = Arc::new(manager);
                let service_discovery =
                    Arc::new(ServiceDiscovery::with_manager(manager.clone(), executor));
                Pulsar {
                    manager,
                    service_discovery,
                }
            })
    }

    pub fn lookup_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> impl Future<Item=BrokerAddress, Error=Error> {
        self.service_discovery.lookup_topic(topic).from_err()
    }

    /// get the number of partitions for a partitioned topic
    pub fn lookup_partitioned_topic_number<S: Into<String>>(
        &self,
        topic: S,
    ) -> impl Future<Item=u32, Error=Error> {
        self.service_discovery
            .lookup_partitioned_topic_number(topic)
            .from_err()
    }

    pub fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> impl Future<Item=Vec<(String, BrokerAddress)>, Error=Error> {
        self.service_discovery
            .lookup_partitioned_topic(topic)
            .from_err()
    }

    pub fn get_topics_of_namespace(&self, namespace: String) -> impl Future<Item=Vec<String>, Error=Error> {
        self.manager.get_base_connection()
            .and_then(move |conn| conn.sender().get_topics_of_namespace(namespace))
            .from_err()
            .map(|topics| topics.topics)
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
    ) -> MultiTopicConsumer<T>
        where T: DeserializeMessage,
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
        )
    }

    pub fn create_consumer<T, S1, S2>(
        &self,
        topic: S1,
        subscription: S2,
        sub_type: SubType,
        batch_size: Option<u32>,
        consumer_name: Option<String>,
        consumer_id: Option<u64>
    ) -> impl Future<Item=Consumer<T>, Error=Error>
        where S1: Into<String>,
              S2: Into<String>,
    {
        let manager = self.manager.clone();
        let topic = topic.into();

        self.service_discovery
            .lookup_topic(topic.clone())
            .from_err()
            .and_then(move |broker_address| manager.get_connection(&broker_address).from_err())
            .and_then(move |conn| {
                Consumer::from_connection(
                    conn,
                    topic,
                    subscription.into(),
                    sub_type,
                    consumer_id,
                    consumer_name,
                    batch_size,
                )
                    .from_err()
            })
    }

    pub fn create_partitioned_consumers<
        T: DeserializeOwned + DeserializeMessage + Sized,
        S1: Into<String> + Clone,
        S2: Into<String> + Clone,
    >(
        &self,
        topic: S1,
        subscription: S2,
        sub_type: SubType,
    ) -> impl Future<Item=Vec<Consumer<T>>, Error=Error> {
        let manager = self.manager.clone();

        self.service_discovery
            .lookup_partitioned_topic(topic.clone())
            .from_err()
            .and_then(move |v| {
                let res = v
                    .iter()
                    .cloned()
                    .map(|(topic, broker_address)| {
                        let subscription = subscription.clone();

                        manager
                            .get_connection(&broker_address)
                            .from_err()
                            .and_then(move |conn| {
                                Consumer::from_connection(
                                    conn,
                                    topic.to_string(),
                                    subscription.into(),
                                    sub_type,
                                    None,
                                    None,
                                    None,
                                )
                                    .from_err()
                            })
                    })
                    .collect::<Vec<_>>();

                join_all(res)
            })
    }

    pub fn create_producer<S: Into<String> + Clone>(
        &self,
        topic: S,
        name: Option<String>,
    ) -> impl Future<Item=Producer, Error=Error> {
        let manager = self.manager.clone();

        self.service_discovery
            .lookup_topic(topic)
            .from_err()
            .and_then(move |broker_address| manager.get_connection(&broker_address).from_err())
            .map(move |conn| Producer::from_connection(conn, name))
    }

    pub fn create_partitioned_producers<S: Into<String> + Clone>(
        &self,
        topic: S,
    ) -> impl Future<Item=Vec<Producer>, Error=Error> {
        let manager = self.manager.clone();

        self.service_discovery
            .lookup_partitioned_topic(topic.clone())
            .from_err()
            .and_then(move |v| {
                let res = v
                    .iter()
                    .cloned()
                    .map(|(topic, broker_address)| {
                        manager
                            .get_connection(&broker_address)
                            .from_err()
                            .map(move |conn| Producer::from_connection(conn, topic.into()))
                    })
                    .collect::<Vec<_>>();

                join_all(res)
            })
    }

    pub fn send_raw<S: Into<String> + Clone>(
        &self,
        topic: S,
        data: Vec<u8>,
        properties: Option<HashMap<String, String>>,
    ) -> impl Future<Item=CommandSendReceipt, Error=Error> {
        let t = topic.clone();
        self.create_producer(topic, None)
            .and_then(|mut producer| {
                producer.send_raw(t, data, properties)
                    .from_err()
            })
    }

    pub fn send_json<S: Into<String> + Clone, T: Serialize>(
        &self,
        topic: S,
        msg: &T,
        properties: Option<HashMap<String, String>>,
    ) -> impl Future<Item=CommandSendReceipt, Error=Error> {
        let data = match serde_json::to_vec(msg) {
            Ok(data) => data,
            Err(e) => {
                let e: ConsumerError = e.into();
                return Either::A(future::failed(e.into()));
            }
        };

        Either::B(self.send_raw(topic, data, properties))
    }
}
