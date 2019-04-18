use crate::connection::{Connection, SerialId, Authentication};
use crate::error::{Error, ProducerError};
use crate::message::proto;
use futures::{Future, future::{self, Either}};
use rand;
use serde::Serialize;
use serde_json;
use std::collections::BTreeMap;
use tokio::runtime::TaskExecutor;


pub struct Producer {
    connection: Connection,
    addr: String,
    topics: BTreeMap<String, (u64, SerialId)>,
    name: String,
}

impl Producer {
    pub fn new<S1, S2>(addr: S1, name: S2, auth: Option<Authentication>, executor: TaskExecutor) -> impl Future<Item=Producer, Error=ProducerError>
        where S1: Into<String>, S2: Into<String>
    {
        Connection::new(addr.into(), auth, executor)
            .map_err(|e| e.into())
            .map(move |conn| Producer::from_connection(conn, name.into()))
    }

    pub fn from_connection(connection: Connection, name: String) -> Producer {
        Producer {
            addr: connection.addr().to_string(),
            connection,
            topics: BTreeMap::new(),
            name,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.connection.is_valid()
    }

    pub fn check_connection(&self) -> impl Future<Item=(), Error=Error> {
        self.connection.sender().lookup_topic("test")
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
        let producer_name = self.name.clone();

        let producer = self.topics.get(&topic)
            .map(|(producer_id, sequence_id)| Either::A(future::finished((*producer_id, sequence_id.get()))))
            .unwrap_or_else(|| {
                let producer_id = rand::random();
                let sequence_ids = SerialId::new();
                let sequence_id = sequence_ids.get();
                self.topics.insert(topic.clone(), (producer_id, sequence_ids));

                let producer_name = producer_name.clone();
                let sender = self.connection.sender().clone();
                Either::B(
                    self.connection.sender().lookup_topic(topic.clone())
                        .and_then(move |_| sender.create_producer(topic, producer_id, Some(producer_name)))
                        .map(move |_| (producer_id, sequence_id))
                )
            });

        let mut sender = self.connection.sender().clone();
        producer.and_then(move |(producer_id, sequence_id)| {
            sender.send(
                producer_id,
                producer_name,
                sequence_id,
                None,
                data
            )
        }).map_err(|e| e.into())
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn error(&mut self) -> Option<Error> {
        self.connection.error()
    }
}


impl Drop for Producer {
    fn drop(&mut self) {
        for (producer_id, _) in self.topics.values() {
            let _ = self.connection.sender().close_producer(*producer_id);
        }
    }
}