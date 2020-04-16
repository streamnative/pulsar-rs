use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::pin::Pin;

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::{
    future::try_join_all,
    Future, FutureExt, TryFutureExt, Stream,
    task::{Context, Poll},
};
use rand;

use crate::client::SerializeMessage;
use crate::connection::{Authentication, Connection, SerialId};
use crate::error::ProducerError;
use crate::executor::Executor;
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
        executor.spawn(Box::pin(ProducerEngine {
            pulsar,
            inbound: Box::pin(rx),
            producers: BTreeMap::new(),
            new_producers: BTreeMap::new(),
            producer_options: options,
        }.map(|res| info!("FIXME: ProducerEngine returned {:?}", res))));
        Producer { message_sender: tx }
    }

    pub async fn send<T: SerializeMessage + ?Sized, S: Into<String>>(
        &self,
        topic: S,
        message: &T,
    ) -> Result<proto::CommandSendReceipt, Error> {
        let topic = topic.into();
        match T::serialize_message(message) {
            Ok(message) => self.send_message(topic, message).await,
            Err(e) => Err(e),
        }
    }

    pub async fn send_all<'a, 'b, T, S, I>(
        &self,
        topic: S,
        messages: I,
    ) -> Result<Vec<proto::CommandSendReceipt>, Error>
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
            Ok(messages) => {
                let mut v = vec![];
                for m in messages.into_iter() {
                    v.push(self.send_message(topic.clone(), m));
                }

                try_join_all(v).await
            }
            Err(e) => Err(e),
        }
    }

    async fn send_message<S: Into<String>>(
        &self,
        topic: S,
        message: Message,
    ) ->  Result<proto::CommandSendReceipt, Error> {
        let (resolver, future) = oneshot::channel();
        match self.message_sender.unbounded_send(ProducerMessage {
            topic: topic.into(),
            message,
            resolver,
        }) {
            Ok(_) => {
                match future.await {
                    Ok(Ok(data)) => Ok(data),
                    Ok(Err(e)) => Err(e),
                    Err(oneshot::Canceled) => Err(ProducerError::Custom(
                        "Unexpected error: pulsar producer engine unexpectedly dropped".to_owned(),
                    )
                    .into()),
                }
            },
            Err(_) => Err(
                ProducerError::Custom(
                    "Unexpected error: pulsar producer engine unexpectedly dropped".to_owned(),
                )
                .into(),
            ),
        }
    }
}

struct ProducerEngine {
    pulsar: Pulsar,
    inbound: Pin<Box<UnboundedReceiver<ProducerMessage>>>,
    producers: BTreeMap<String, Arc<TopicProducer>>,
    new_producers: BTreeMap<String, Pin<Box<oneshot::Receiver<Result<Arc<TopicProducer>, Error>>>>>,
    producer_options: ProducerOptions,
}

impl Future for ProducerEngine {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        if !self.new_producers.is_empty() {
            let mut resolved_topics = Vec::new();
            let mut received_producers = vec![];
            for (topic, producer) in self.new_producers.iter_mut() {
                match producer.as_mut().poll(cx) {
                    Poll::Ready(Ok(Ok(producer))) => {
                        received_producers.push(producer);
                        resolved_topics.push(topic.clone());
                    }
                    Poll::Ready(Ok(Err(_))) | Poll::Ready(Err(_)) => resolved_topics.push(topic.clone()),
                    Poll::Pending => {}
                }
            }

            for producer in received_producers.drain(..) {
                self.producers.insert(producer.topic().to_owned(), producer);
            }
            for topic in resolved_topics {
                self.new_producers.remove(&topic);
            }
        }

        loop {
            let r = match self.inbound.as_mut().poll_next(cx) {
              Poll::Pending => return Poll::Pending,
              Poll::Ready(t) => t,
            };

            match r {
                Some(ProducerMessage {
                    topic,
                    message,
                    resolver,
                }) => {
                    match self.producers.get(&topic) {
                        Some(producer) => {
                            let p = producer.clone();
                            let f = async move {
                                p
                                    .send_message(message, None)
                                    .map(|r| {
                                        resolver.send(r).expect("FIXME")
                                    }).await;//.map_err(drop)),
                            };
                            self.pulsar.executor().spawn(Box::pin(f));
                        }
                        None => {
                            let pending = self.new_producers.remove(&topic).unwrap_or_else(|| {
                                let (tx, rx) = oneshot::channel::<Result<Arc<TopicProducer>, Error>>();
                                let pulsar = self.pulsar.clone();
                                let topic = topic.clone();
                                let producer_options = self.producer_options.clone();
                                let f = async move {
                                   pulsar
                                        .create_producer(
                                            topic,
                                            None,
                                            producer_options,
                                        )
                                        .map(|r| {
                                            tx.send(r.map(Arc::new)).map_err(drop).expect("FIXME")
                                        }).await;
                                };
                                self.pulsar.executor().spawn(Box::pin(f));
                                Box::pin(rx)
                            });
                            let (tx, rx) = oneshot::channel::<Result<Arc<TopicProducer>, Error>>();
                            let f = async {
                                match pending//.map_err(drop)
                                    .await {
                                    Ok(Ok(producer)) => {
                                        let _ = tx.send(Ok(producer.clone()));
                                            producer
                                            .send_message(message, None)
                                            .map(|r| resolver.send(r).expect("FIXME")).await
                                            //.map_err(drop),
                                    },
                                    Ok(Err(e)) => {
                                        // TODO find better error propagation here
                                        let _ = resolver.send(Err(Error::Producer(
                                                    //FIXME
                                                    //ProducerError::Custom(e.to_string()),
                                                    ProducerError::Custom("()".to_string()),
                                                    )));
                                        let _ = tx.send(Err(Error::Producer(
                                                    //FIXME
                                                    //ProducerError::Custom(e.to_string()),
                                                    ProducerError::Custom("()".to_string()),
                                                    )));
                                    }
                                    Err(e) => {
                                        // TODO find better error propagation here
                                        let _ = resolver.send(Err(Error::Producer(
                                                    //FIXME
                                                    //ProducerError::Custom(e.to_string()),
                                                    ProducerError::Custom("()".to_string()),
                                                    )));
                                        let _ = tx.send(Err(Error::Producer(
                                                    //FIXME
                                                    //ProducerError::Custom(e.to_string()),
                                                    ProducerError::Custom("()".to_string()),
                                                    )));
                                    }
                                }
                            };
                            tokio::spawn(f);
                            self.new_producers.insert(topic, Box::pin(rx));
                        }
                    }
                }
                None => return Poll::Ready(Ok(())),
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
    pub fn new<S1, S2, E: Executor+'static>(
        addr: S1,
        topic: S2,
        name: Option<String>,
        auth: Option<Authentication>,
        proxy_to_broker_url: Option<String>,
        options: ProducerOptions,
        executor: E,
    ) -> impl Future<Output = Result<TopicProducer, Error>>
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

    pub async fn from_connection<S: Into<String>>(
        connection: Arc<Connection>,
        topic: S,
        name: Option<String>,
        options: ProducerOptions,
    ) -> Result<TopicProducer, Error> {
        let topic = topic.into();
        let producer_id = rand::random();
        let sequence_ids = SerialId::new();

        let sender = connection.sender().clone();
        let _ = connection
            .sender()
            .lookup_topic(topic.clone(), false).await?;

        let topic = topic.clone();
        let success = sender
            .create_producer(topic.clone(), producer_id, name, options).await?;
        Ok(TopicProducer {
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

    pub async fn check_connection(&self) -> Result<(), Error> {
        self.connection
            .sender()
            .lookup_topic("test", false).await?;
        Ok(())
    }

    pub async fn send_raw(
        &self,
        message: Message,
    ) -> Result<proto::CommandSendReceipt, Error> {
        self.connection
            .sender()
            .send(
                self.id,
                self.name.clone(),
                self.message_id.get(),
                None,
                message,
            ).await
            .map_err(|e| e.into())
    }

    pub async fn send<T: SerializeMessage + ?Sized>(
        &self,
        message: &T,
        num_messages: Option<i32>,
    ) -> Result<proto::CommandSendReceipt, Error> {
        match T::serialize_message(message) {
            Ok(message) => self.send_message(message, num_messages).await,
            Err(e) => Err(e),
        }
    }

    pub fn error(&self) -> Option<Error> {
        self.connection
            .error()
            .map(|e| ProducerError::Connection(e).into())
    }

    async fn send_message(
        &self,
        message: Message,
        num_messages: Option<i32>,
    ) -> Result<proto::CommandSendReceipt, Error> {
        self.connection
            .sender()
            .send(
                self.id,
                self.name.clone(),
                self.message_id.get(),
                num_messages,
                message,
            ).await
            .map_err(|e| ProducerError::Connection(e).into())
    }
}

impl Drop for TopicProducer {
    fn drop(&mut self) {
        let _ = self.connection.sender().close_producer(self.id);
    }
}
