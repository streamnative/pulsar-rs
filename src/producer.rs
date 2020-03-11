use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::sync::oneshot;
use futures::{
    future::{self, Either},
    Future,
};
use rand;
use tokio::prelude::*;

use crate::client::SerializeMessage;
use crate::connection::{Authentication, Connection, SerialId};
use crate::error::ProducerError;
use crate::executor::PulsarExecutor;
use crate::message::proto::{self, EncryptionKeys, Schema};
use crate::{Error, Pulsar};

type ProducerId = u64;
type ProducerName = String;

#[derive(Debug, Clone, Default)]
pub struct Message {
    pub payload: Vec<u8>,
    pub properties: HashMap<String, String>,
    ///key to decide partition for the msg
    pub partition_key: ::std::option::Option<String>,
    /// Override namespace's replication
    pub replicate_to: ::std::vec::Vec<String>,
    pub compression: ::std::option::Option<i32>,
    pub uncompressed_size: ::std::option::Option<u32>,
    /// Removed below checksum field from Metadata as
    /// it should be part of send-command which keeps checksum of header + payload
    ///optional sfixed64 checksum = 10;
    /// differentiate single and batch message metadata
    pub num_messages_in_batch: ::std::option::Option<i32>,
    /// the timestamp that this event occurs. it is typically set by applications.
    /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    pub event_time: ::std::option::Option<u64>,
    /// Contains encryption key name, encrypted key and metadata to describe the key
    pub encryption_keys: ::std::vec::Vec<EncryptionKeys>,
    /// Algorithm used to encrypt data key
    pub encryption_algo: ::std::option::Option<String>,
    /// Additional parameters required by encryption
    pub encryption_param: ::std::option::Option<Vec<u8>>,
    pub schema_version: ::std::option::Option<Vec<u8>>,
}

#[derive(Clone, Default)]
pub struct ProducerOptions {
    pub encrypted: Option<bool>,
    pub metadata: BTreeMap<String, String>,
    pub schema: Option<Schema>,
    pub batch_size: Option<u32>,
}

#[derive(Clone)]
pub struct Producer {
    message_sender: UnboundedSender<ProducerMessage>,
}

impl Producer {
    pub fn new(pulsar: Pulsar, options: ProducerOptions) -> Producer {
        let (tx, rx) = unbounded();
        let executor = pulsar.executor().clone();
        executor.spawn(ProducerEngine {
            pulsar,
            inbound: rx,
            producers: BTreeMap::new(),
            new_producers: BTreeMap::new(),
            producer_options: options,
        });
        Producer { message_sender: tx }
    }

    pub fn send<T: SerializeMessage + ?Sized, S: Into<String>>(
        &self,
        topic: S,
        message: &T,
    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> {
        let topic = topic.into();
        match T::serialize_message(message) {
            Ok(message) => Either::A(self.send_message(topic, message)),
            Err(e) => Either::B(future::failed(e)),
        }
    }

    pub fn send_all<'a, 'b, T, S, I>(
        &self,
        topic: S,
        messages: I,
    ) -> impl Future<Item = Vec<proto::CommandSendReceipt>, Error = Error>
    where
        'b: 'a,
        T: 'b + SerializeMessage + ?Sized,
        I: IntoIterator<Item = &'a T>,
        S: Into<String>,
    {
        let topic = topic.into();
        // TODO determine whether to keep this approach or go with the partial send, but more mem friendly lazy approach.
        // serialize all messages before sending to avoid a partial send
        match messages
            .into_iter()
            .map(|m| T::serialize_message(m))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(messages) => Either::A(future::collect(
                messages
                    .into_iter()
                    .map(|m| self.send_message(topic.clone(), m))
                    .collect::<Vec<_>>(),
            )),
            Err(e) => Either::B(future::failed(e)),
        }
    }

    fn send_message<S: Into<String>>(
        &self,
        topic: S,
        message: Message,
    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> + 'static {
        let (resolver, future) = oneshot::channel();
        match self.message_sender.unbounded_send(ProducerMessage {
            topic: topic.into(),
            message,
            resolver,
        }) {
            Ok(_) => Either::A(future.then(|r| {
                match r {
                    Ok(Ok(data)) => Ok(data),
                    Ok(Err(e)) => Err(e),
                    Err(oneshot::Canceled) => Err(ProducerError::Custom(
                        "Unexpected error: pulsar producer engine unexpectedly dropped".to_owned(),
                    )
                    .into()),
                }
            })),
            Err(_) => Either::B(future::failed(
                ProducerError::Custom(
                    "Unexpected error: pulsar producer engine unexpectedly dropped".to_owned(),
                )
                .into(),
            )),
        }
    }
}

struct ProducerEngine {
    pulsar: Pulsar,
    inbound: UnboundedReceiver<ProducerMessage>,
    producers: BTreeMap<String, Arc<TopicProducer>>,
    new_producers: BTreeMap<String, oneshot::Receiver<Result<Arc<TopicProducer>, Error>>>,
    producer_options: ProducerOptions,
}

impl Future for ProducerEngine {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if !self.new_producers.is_empty() {
            let mut resolved_topics = Vec::new();
            for (topic, producer) in self.new_producers.iter_mut() {
                match producer.poll() {
                    Ok(Async::Ready(Ok(producer))) => {
                        self.producers.insert(producer.topic().to_owned(), producer);
                        resolved_topics.push(topic.clone());
                    }
                    Ok(Async::Ready(Err(_))) | Err(_) => resolved_topics.push(topic.clone()),
                    Ok(Async::NotReady) => {}
                }
            }
            for topic in resolved_topics {
                self.new_producers.remove(&topic);
            }
        }

        loop {
            match try_ready!(self.inbound.poll()) {
                Some(ProducerMessage {
                    topic,
                    message,
                    resolver,
                }) => {
                    match self.producers.get(&topic) {
                        Some(producer) => {
                            tokio::spawn(
                                producer
                                    .send_message(message, None)
                                    .then(|r| resolver.send(r).map_err(drop)),
                            );
                        }
                        None => {
                            let pending = self.new_producers.remove(&topic).unwrap_or_else(|| {
                                let (tx, rx) = oneshot::channel();
                                tokio::spawn({
                                    self.pulsar
                                        .create_producer(
                                            topic.clone(),
                                            None,
                                            self.producer_options.clone(),
                                        )
                                        .then(|r| tx.send(r.map(Arc::new)).map_err(drop))
                                });
                                rx
                            });
                            let (tx, rx) = oneshot::channel();
                            tokio::spawn(pending.map_err(drop).and_then(move |r| match r {
                                Ok(producer) => {
                                    let _ = tx.send(Ok(producer.clone()));
                                    Either::A(
                                        producer
                                            .send_message(message, None)
                                            .then(|r| resolver.send(r))
                                            .map_err(drop),
                                    )
                                }
                                Err(e) => {
                                    // TODO find better error propogation here
                                    let _ = resolver.send(Err(Error::Producer(
                                        ProducerError::Custom(e.to_string()),
                                    )));
                                    let _ = tx.send(Err(e));
                                    Either::B(future::failed(()))
                                }
                            }));
                            self.new_producers.insert(topic, rx);
                        }
                    }
                }
                None => return Ok(Async::Ready(())),
            }
        }
    }
}

struct ProducerMessage {
    topic: String,
    message: Message,
    resolver: oneshot::Sender<Result<proto::CommandSendReceipt, Error>>,
}

pub struct TopicProducer {
    connection: Arc<Connection>,
    id: ProducerId,
    name: ProducerName,
    topic: String,
    message_id: SerialId,
}

impl TopicProducer {
    pub fn new<S1, S2, E: PulsarExecutor>(
        addr: S1,
        topic: S2,
        name: Option<String>,
        auth: Option<Authentication>,
        proxy_to_broker_url: Option<String>,
        options: ProducerOptions,
        executor: E,
    ) -> impl Future<Item = TopicProducer, Error = Error>
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Connection::new(addr.into(), auth, proxy_to_broker_url, executor)
            .map_err(|e| e.into())
            .and_then(move |conn| {
                TopicProducer::from_connection::<_>(Arc::new(conn), topic.into(), name, options)
            })
    }

    pub fn from_connection<S: Into<String>>(
        connection: Arc<Connection>,
        topic: S,
        name: Option<String>,
        options: ProducerOptions,
    ) -> impl Future<Item = TopicProducer, Error = Error> {
        let topic = topic.into();
        let producer_id = rand::random();
        let sequence_ids = SerialId::new();

        let sender = connection.sender().clone();
        connection
            .sender()
            .lookup_topic(topic.clone(), false)
            .map_err(|e| e.into())
            .and_then({
                let topic = topic.clone();
                move |_| {
                    sender
                        .create_producer(topic.clone(), producer_id, name, options)
                        .map_err(|e| e.into())
                }
            })
            .map(move |success| TopicProducer {
                connection,
                id: producer_id,
                name: success.producer_name,
                topic,
                message_id: sequence_ids,
            })
    }

    pub fn is_valid(&self) -> bool {
        self.connection.is_valid()
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn check_connection(&self) -> impl Future<Item = (), Error = Error> {
        self.connection
            .sender()
            .lookup_topic("test", false)
            .map(|_| ())
            .map_err(|e| e.into())
    }

    pub fn send_raw(
        &self,
        message: Message,
    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> {
        self.connection
            .sender()
            .send(
                self.id,
                self.name.clone(),
                self.message_id.get(),
                None,
                message,
            )
            .map_err(|e| e.into())
    }

    pub fn send<T: SerializeMessage + ?Sized>(
        &self,
        message: &T,
        num_messages: Option<i32>,
    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> {
        match T::serialize_message(message) {
            Ok(message) => Either::A(self.send_message(message, num_messages)),
            Err(e) => Either::B(future::failed(e)),
        }
    }

    pub fn error(&self) -> Option<Error> {
        self.connection
            .error()
            .map(|e| ProducerError::Connection(e).into())
    }

    fn send_message(
        &self,
        message: Message,
        num_messages: Option<i32>,
    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> {
        self.connection
            .sender()
            .send(
                self.id,
                self.name.clone(),
                self.message_id.get(),
                num_messages,
                message,
            )
            .map_err(|e| ProducerError::Connection(e).into())
    }
}

impl Drop for TopicProducer {
    fn drop(&mut self) {
        let _ = self.connection.sender().close_producer(self.id);
    }
}
