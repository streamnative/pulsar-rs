use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::RwLock;

use futures::{Future, future::{self, Either}};
use rand;
use serde::Serialize;
use serde_json;
use tokio::runtime::TaskExecutor;

use crate::connection::{Authentication, Connection, SerialId};
use crate::error::{ConnectionError, ProducerError};
use crate::message::proto;

type ProducerId = u64;
type ProducerName = String;

pub struct Producer {
    connection: Arc<Connection>,
    addr: String,
    topics: Arc<RwLock<BTreeMap<String, (ProducerId, ProducerName, SerialId)>>>,
    name: Option<String>,
}

impl Producer {
    pub fn new<S>(addr: S, name: Option<String>, auth: Option<Authentication>, proxy_to_broker_url: Option<String>,
                  executor: TaskExecutor) -> impl Future<Item=Producer, Error=ProducerError>
        where S: Into<String>
    {
        Connection::new(addr.into(), auth, proxy_to_broker_url, executor)
            .map_err(|e| e.into())
            .map(move |conn| Producer::from_connection(Arc::new(conn), name))
    }

    pub fn from_connection(connection: Arc<Connection>, name: Option<String>) -> Producer {
        Producer {
            addr: connection.addr().to_string(),
            connection,
            topics: Arc::new(RwLock::new(BTreeMap::new())),
            name,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.connection.is_valid()
    }

    pub fn check_connection(&self) -> impl Future<Item=(), Error=ConnectionError> {
        self.connection.sender().lookup_topic("test", false)
            .map(|_| ())
    }

    pub fn send_json<S: Into<String>, T: Serialize>(&mut self, topic: S, msg: &T) -> impl Future<Item=proto::CommandSendReceipt, Error=ProducerError> {
        let data = match serde_json::to_vec(msg) {
            Ok(data) => data,
            Err(e) => return Either::A(future::failed(e.into())),
        };
        Either::B(self.send_raw(topic, data))
    }

    pub fn send_raw<S: Into<String>>(&mut self, topic: S, data: Vec<u8>) -> impl Future<Item=proto::CommandSendReceipt, Error=ProducerError> {
        let topic = topic.into();

        let producer = self.topics.read().unwrap().get(&topic)
            .map(|(producer_id, name, sequence_id)| Either::A(future::finished((*producer_id, name.clone(), sequence_id.get()))))
            .unwrap_or_else(|| {
                let producer_id = rand::random();
                let sequence_ids = SerialId::new();
                let sequence_id = sequence_ids.get();
                let topics = self.topics.clone();

                let producer_name = self.name.clone();
                let sender = self.connection.sender().clone();
                Either::B(
                    self.connection.sender().lookup_topic(topic.clone(), false)
                        .and_then({
                            let topic = topic.clone();
                            move |_| sender.create_producer(topic.clone(), producer_id, producer_name)
                        })
                        .map(move |success| {
                            topics.write().unwrap().insert(topic, (producer_id, success.producer_name.clone(), sequence_ids));
                            (producer_id, success.producer_name, sequence_id)
                        })
                )
            });

        let mut sender = self.connection.sender().clone();
        producer.and_then(move |(producer_id, producer_name, sequence_id)| {
            sender.send(
                producer_id,
                producer_name,
                sequence_id,
                None,
                data,
            )
        }).map_err(|e| e.into())
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn error(&mut self) -> Option<ConnectionError> {
        self.connection.error()
    }
}


impl Drop for Producer {
    fn drop(&mut self) {
        if let Ok(topics) = self.topics.read() {
            for (producer_id, _, _) in topics.values() {
                let _ = self.connection.sender().close_producer(*producer_id);
            }
        }
    }
}