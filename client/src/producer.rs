use connection::Connection;
use error::Error;
use futures::{Future, future};
use rand;
use serde::Serialize;
use serde_json;
use message::proto;


pub struct Producer<T: Serialize> {
    connection: Connection,
    addr: String,
    topic: String,
    id: u64,
    name: String,
    data_type: ::std::marker::PhantomData<T>,
    sequence_id: u64,
}

impl<T: Serialize> Producer<T> {
    pub fn new<S1, S2>(addr: S1, topic: S2, name: Option<String>) -> impl Future<Item=(impl Future<Item=Producer<T>, Error=Error>, impl Future<Item=(), Error=()>, impl Future<Item=(), Error=()>), Error=Error>
        where S1: Into<String>, S2: Into<String>
    {
        let addr = addr.into();
        let topic = topic.into();
        Connection::new(addr.clone())
            .map(move |(conn, handler_a, handler_b)| {
                (Producer::from_connection(conn, topic, name), handler_a, handler_b)
            })
    }

    pub fn from_connection(mut conn: Connection, topic: String, name: Option<String>) -> impl Future<Item=Producer<T>, Error=Error> {
        let topic_= topic.clone();
        let producer_id = rand::random();

        conn.lookup_topic(topic.clone())
            .map(move |resp| (resp, conn))
            .and_then(move |(_, mut conn)|
                conn.create_producer(topic_, producer_id, name)
                    .map(move |resp| (resp, conn)))
            .map(move |(resp, conn)| {
                Producer {
                    addr: conn.addr().to_string(),
                    connection: conn,
                    topic,
                    id: producer_id,
                    name: resp.producer_name,
                    data_type: ::std::marker::PhantomData,
                    sequence_id: 0
                }
            })
    }

    pub fn send(&mut self, msg: &T) -> impl Future<Item=proto::CommandSendReceipt, Error=Error> {
        let data = match serde_json::to_vec(msg) {
            Ok(data) => data,
            Err(e) => return future::Either::A(future::failed(e.into()))
        };
        let sequence_id = self.sequence_id;
        self.sequence_id += 1;
        future::Either::B(self.connection.send(
            self.id,
            self.name.clone(),
            sequence_id,
            None,
            data
        ))
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn error(&mut self) -> Option<Error> {
        self.connection.error()
    }
}
