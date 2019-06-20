use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use futures::{Async, Stream, sync::mpsc};
use futures::Future;
use rand;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde_json;
use tokio::runtime::TaskExecutor;

use reconnecting::ReconnectingStream;

use crate::connection::{Authentication, Connection};
use crate::error::{ConnectionError, ConsumerError, Error};
use crate::message::{Message, Payload, proto::{self, command_subscribe::SubType}};
use crate::Pulsar;
use crate::reconnecting;

pub struct Consumer<T> {
    connection: Arc<Connection>,
    id: u64,
    messages: mpsc::UnboundedReceiver<Message>,
    deserialize: Box<dyn Fn(Payload) -> Result<T, ConsumerError> + Send>,
    batch_size: u32,
    remaining_messages: u32,
}

impl<T: DeserializeOwned> Consumer<T> {
    pub fn new(
        addr: String,
        topic: String,
        subscription: String,
        sub_type: SubType,
        consumer_id: Option<u64>,
        consumer_name: Option<String>,
        auth_data: Option<Authentication>,
        proxy_to_broker_url: Option<String>,
        executor: TaskExecutor,
        deserialize: Box<dyn Fn(Payload) -> Result<T, ConsumerError> + Send>,
        batch_size: Option<u32>,
    ) -> impl Future<Item=Consumer<T>, Error=ConsumerError> {
        let consumer_id = consumer_id.unwrap_or_else(rand::random);
        let (resolver, messages) = mpsc::unbounded();
        let batch_size = batch_size.unwrap_or(1000);

        Connection::new(addr, auth_data, proxy_to_broker_url, executor.clone())
            .and_then(move |conn|
                conn.sender().subscribe(resolver, topic, subscription, sub_type, consumer_id, consumer_name)
                    .map(move |resp| (resp, conn)))
            .and_then(move |(_, conn)| {
                conn.sender().send_flow(consumer_id, batch_size)
                    .map(move |()| conn)
            })
            .map_err(|e| e.into())
            .map(move |connection| {
                Consumer {
                    connection: Arc::new(connection),
                    id: consumer_id,
                    messages,
                    deserialize,
                    batch_size,
                    remaining_messages: batch_size,
                }
            })
    }

    pub fn from_connection<F>(
        conn: Arc<Connection>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        consumer_id: Option<u64>,
        consumer_name: Option<String>,
        deserialize: F,
        batch_size: Option<u32>,
    ) -> impl Future<Item=Consumer<T>, Error=ConsumerError>
        where F: Fn(Payload) -> Result<T, ConsumerError> + Send + 'static
    {
        let consumer_id = consumer_id.unwrap_or_else(rand::random);
        let (resolver, messages) = mpsc::unbounded();
        let batch_size = batch_size.unwrap_or(1000);

        conn.sender().subscribe(resolver, topic, subscription, sub_type, consumer_id, consumer_name)
            .map(move |resp| (resp, conn))
            .and_then(move |(_, conn)| {
                conn.sender().send_flow(consumer_id, batch_size)
                    .map(move |()| conn)
            })
            .map_err(|e| e.into())
            .map(move |connection| {
                Consumer {
                    connection,
                    id: consumer_id,
                    messages,
                    deserialize: Box::new(deserialize),
                    batch_size,
                    remaining_messages: batch_size,
                }
            })
    }
}

pub struct Ack {
    consumer_id: u64,
    message_id: Vec<proto::MessageIdData>,
    connection: Arc<Connection>,
}

impl Ack {
    pub fn new(consumer_id: u64, msg: proto::MessageIdData, connection: Arc<Connection>) -> Ack {
        Ack { consumer_id, message_id: vec![msg], connection }
    }

    pub fn join(mut self, other: Ack) -> Self {
        self.message_id.extend(other.message_id);
        self
    }

    pub fn extend<I: IntoIterator<Item=Ack>>(mut self, others: I) -> Self {
        self.message_id.extend(others.into_iter().flat_map(|ack| ack.message_id));
        self
    }

    pub fn ack(self) {
        let _ = self.connection.sender().send_ack(self.consumer_id, self.message_id);
    }
}

impl<T> Stream for Consumer<T> {
    type Item = (Result<T, ConsumerError>, Ack);
    type Error = ConsumerError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if !self.connection.is_valid() {
            if let Some(err) = self.connection.error() {
                return Err(err.into());
            }
        }

        if self.remaining_messages <= self.batch_size / 2 {
            self.connection.sender().send_flow(self.id, self.batch_size - self.remaining_messages)?;
            self.remaining_messages = self.batch_size;
        }

        let message: Option<Option<(proto::CommandMessage, Payload)>> = try_ready!(self.messages.poll().map_err(|_| ConnectionError::Disconnected))
            .map(|Message { command, payload }: Message|
                command.message
                    .and_then(move |msg| payload
                        .map(move |payload| (msg, payload))));

        if message.is_some() {
            self.remaining_messages -= 1;
        }

        match message {
            Some(Some((message, payload))) => {
                Ok(Async::Ready(Some(
                    (
                        (&self.deserialize)(payload),
                        Ack::new(self.id, message.message_id, self.connection.clone())
                    )
                )))
            }
            Some(None) => Err(ConsumerError::MissingPayload(format!("Missing payload for message {:?}", message))),
            None => Ok(Async::Ready(None))
        }
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        let _ = self.connection.sender().close_consumer(self.id);
    }
}

pub struct Set<T>(T);

pub struct Unset;

pub struct ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
    addr: String,
    topic: Topic,
    subscription: Subscription,
    subscription_type: SubscriptionType,
    consumer_id: Option<u64>,
    consumer_name: Option<String>,
    authentication: Option<Authentication>,
    proxy_to_broker_url: Option<String>,
    executor: TaskExecutor,
    deserialize: Option<Box<dyn Fn(Payload) -> Result<DataType, ConsumerError> + Send>>,
    batch_size: Option<u32>,
}

impl ConsumerBuilder<Unset, Unset, Unset, Unset> {
    pub fn new<S: Into<String>>(addr: S, executor: TaskExecutor) -> Self {
        ConsumerBuilder {
            addr: addr.into(),
            topic: Unset,
            subscription: Unset,
            subscription_type: Unset,
            consumer_id: None,
            consumer_name: None,
            authentication: None,
            proxy_to_broker_url: None,
            executor,
            deserialize: None,
            batch_size: None,
        }
    }
}

impl<Subscription, SubscriptionType, DataType> ConsumerBuilder<Unset, Subscription, SubscriptionType, DataType> {
    pub fn with_topic<S: Into<String>>(self, topic: S) -> ConsumerBuilder<Set<String>, Subscription, SubscriptionType, DataType> {
        ConsumerBuilder {
            topic: Set(topic.into()),
            addr: self.addr,
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            authentication: self.authentication,
            proxy_to_broker_url: self.proxy_to_broker_url,
            executor: self.executor,
            deserialize: self.deserialize,
            batch_size: self.batch_size,
        }
    }
}

impl<Topic, SubscriptionType, DataType> ConsumerBuilder<Topic, Unset, SubscriptionType, DataType> {
    pub fn with_subscription<S: Into<String>>(self, subscription: S) -> ConsumerBuilder<Topic, Set<String>, SubscriptionType, DataType> {
        ConsumerBuilder {
            subscription: Set(subscription.into()),
            topic: self.topic,
            addr: self.addr,
            subscription_type: self.subscription_type,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            authentication: self.authentication,
            proxy_to_broker_url: self.proxy_to_broker_url,
            executor: self.executor,
            deserialize: self.deserialize,
            batch_size: self.batch_size,
        }
    }
}

impl<Topic, Subscription, DataType> ConsumerBuilder<Topic, Subscription, Unset, DataType> {
    pub fn with_subscription_type(self, subscription_type: SubType) -> ConsumerBuilder<Topic, Subscription, Set<SubType>, DataType> {
        ConsumerBuilder {
            subscription_type: Set(subscription_type),
            topic: self.topic,
            addr: self.addr,
            subscription: self.subscription,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            authentication: self.authentication,
            proxy_to_broker_url: self.proxy_to_broker_url,
            executor: self.executor,
            deserialize: self.deserialize,
            batch_size: self.batch_size,
        }
    }
}

impl<Topic, Subscription, SubscriptionType> ConsumerBuilder<Topic, Subscription, SubscriptionType, Unset> {
    pub fn with_deserializer<T, F>(self, deserializer: F) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, T>
        where F: Fn(Payload) -> Result<T, ConsumerError> + Send + 'static
    {
        ConsumerBuilder {
            deserialize: Some(Box::new(deserializer)),
            topic: self.topic,
            addr: self.addr,
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_name: self.consumer_name,
            consumer_id: self.consumer_id,
            authentication: self.authentication,
            proxy_to_broker_url: self.proxy_to_broker_url,
            executor: self.executor,
            batch_size: self.batch_size,
        }
    }
}

impl<Topic, Subscription, SubscriptionType, DataType> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
    pub fn with_consumer_id(mut self, consumer_id: u64) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
        self.consumer_id = Some(consumer_id);
        self
    }

    pub fn with_consumer_name<S: Into<String>>(mut self, consumer_name: S) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    pub fn with_batch_size(mut self, batch_size: u32) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn authenticate(mut self, method: String, data: Vec<u8>) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
        self.authentication = Some(Authentication {
            name: method,
            data,
        });
        self
    }

    pub fn with_proxy_to_broker_url<S: Into<String>>(mut self, url: S) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
        self.proxy_to_broker_url = Some(url.into());
        self
    }
}

impl ConsumerBuilder<Set<String>, Set<String>, Set<SubType>, Unset> {
    pub fn build<T: DeserializeOwned>(self) -> impl Future<Item=Consumer<T>, Error=ConsumerError> {
        let deserialize = Box::new(|payload: Payload| {
            serde_json::from_slice(&payload.data).map_err(|e| e.into())
        });
        let ConsumerBuilder {
            addr,
            topic: Set(topic),
            subscription: Set(subscription),
            subscription_type: Set(sub_type),
            consumer_id,
            consumer_name,
            authentication,
            proxy_to_broker_url,
            executor,
            batch_size,
            ..
        } = self;
        Consumer::new(addr, topic, subscription, sub_type, consumer_id, consumer_name, authentication, proxy_to_broker_url, executor, deserialize, batch_size)
    }
}

impl<T: DeserializeOwned> ConsumerBuilder<Set<String>, Set<String>, Set<SubType>, T> {
    pub fn build(self) -> impl Future<Item=Consumer<T>, Error=ConsumerError> {
        let ConsumerBuilder {
            addr,
            topic: Set(topic),
            subscription: Set(subscription),
            subscription_type: Set(sub_type),
            consumer_id,
            consumer_name,
            authentication,
            proxy_to_broker_url,
            executor,
            deserialize,
            batch_size,
        } = self;
        let deserialize = deserialize.unwrap();
        Consumer::new(addr, topic, subscription, sub_type, consumer_id, consumer_name, authentication, proxy_to_broker_url, executor, deserialize, batch_size)
    }
}

impl From<Error> for reconnecting::Error<Error> {
    fn from(e: Error) -> Self {
        reconnecting::Error::Reconnect(e)
    }
}

pub struct MultiTopicConsumer<T> {
    namespace: String,
    topic_regex: Regex,
    pulsar: Pulsar,
    consumers: BTreeMap<String, ReconnectingStream<(Result<T, Error>, Ack), Error>>,
    topics: VecDeque<String>,
    new_consumers: Option<Box<dyn Future<Item=Vec<(String, ReconnectingStream<(Result<T, Error>, Ack), Error>)>, Error=Error>>>,
    refresh: Box<dyn Stream<Item=(), Error=()>>,
    subscription: String,
    sub_type: SubType,
    deserialize: Arc<dyn Fn(Payload) -> Result<T, ConsumerError> + Send + Sync + 'static>,
    max_retries: u32,
    max_backoff: Duration,
}

impl<T> Stream for MultiTopicConsumer<T>
    where T: 'static, for<'de> T: serde::de::Deserialize<'de>
{
    type Item = (Result<T, Error>, Ack);
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if let Some(mut new_consumers) = self.new_consumers.take() {
            match new_consumers.poll() {
                Ok(Async::Ready(new_consumers)) => {
                    for (topic, consumer) in new_consumers {
                        self.consumers.insert(topic.clone(), consumer);
                        self.topics.push_back(topic);
                    }
                }
                Ok(Async::NotReady) => {
                    self.new_consumers = Some(new_consumers);
                }
                Err(e) => {
                    println!("Error connecting to pulsar topic {}", e);
                }
            }
        }

        if let Ok(Async::Ready(_)) = self.refresh.poll() {
            let regex = self.topic_regex.clone();
            let pulsar = self.pulsar.clone();
            let subscription = self.subscription.clone();
            let sub_type = self.sub_type;
            let deserialize = self.deserialize.clone();
            let max_retries = self.max_retries;
            let max_backoff = self.max_backoff;
            let new_consumers = Box::new(self.pulsar.get_topics_of_namespace(self.namespace.clone())
                .and_then(move |topics: Vec<String>| {
                    futures::future::collect(topics.into_iter()
                        .filter(move |topic| regex.is_match(topic.as_str()))
                        .map(move |topic| {
                            let pulsar = pulsar.clone();
                            let deserialize = deserialize.clone();
                            let subscription = subscription.clone();
                            let topic_ = topic.clone();
                            ReconnectingStream::new(
                                move || Box::new({
                                    let deserialize = deserialize.clone();
                                    let topic = topic_.clone();
                                    pulsar.create_consumer(topic, subscription.clone(), sub_type, move |payload| deserialize(payload))
                                        .map(|stream| {
                                            let stream = stream
                                                .map(|(r, ack)| (r.map_err(|e| Error::Consumer(e)), ack))
                                                .map_err(|e| Error::Consumer(e));
                                            Box::new(stream) as Box<dyn Stream<Item=_, Error=_>>
                                        })
                                }),
                                max_retries,
                                max_backoff,
                            ).map(move |stream| (topic, stream))
                        })
                    )
                }));
            self.new_consumers = Some(new_consumers);
        }

        for _ in 0..self.topics.len() {
            let topic = self.topics.pop_front().unwrap();
            let mut result = None;
            if let Some(item) = self.consumers.get_mut(&topic).map(|c| c.poll()) {
                match item {
                    Ok(Async::NotReady) => {}
                    Ok(Async::Ready(Some(data))) => result = Some(data),
                    Ok(Async::Ready(None)) => {
                        println!("Unexpected end of stream for pulsar topic {}", topic);
                        self.consumers.remove(&topic);
                        continue; //continue to avoid re-adding topic to list of topics
                    }
                    Err(e) => {
                        println!("Unexpected error consuming from pulsar topic {}: {}", topic, e);
                        self.consumers.remove(&topic);
                        continue; //continue to avoid re-adding topic to list of topics
                    }
                }
            } else {
                println!("BUG: Missing consumer for topic {}", &topic);
            }
            self.topics.push_back(topic);
            if let Some(result) = result {
                return Ok(Async::Ready(Some(result)));
            }
        }

        Ok(Async::NotReady)
    }
}