use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Async, Stream, sync::mpsc};
use futures::Future;
use rand;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde_json;
use tokio::runtime::TaskExecutor;
use tokio::timer::Interval;

use crate::connection::{Authentication, Connection};
use crate::error::{ConnectionError, ConsumerError, Error};
use crate::message::{Message, Payload, proto::{self, command_subscribe::SubType}};
use crate::Pulsar;

pub struct Consumer<T, E> {
    connection: Arc<Connection>,
    id: u64,
    messages: mpsc::UnboundedReceiver<Message>,
    deserialize: Box<dyn Fn(Payload) -> Result<T, E> + Send>,
    batch_size: u32,
    remaining_messages: u32,
}

impl<T, E> Consumer<T, E> {
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
        deserialize: Box<dyn Fn(Payload) -> Result<T, E> + Send>,
        batch_size: Option<u32>,
    ) -> impl Future<Item=Consumer<T, E>, Error=ConsumerError> {
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
    ) -> impl Future<Item=Consumer<T, E>, Error=ConsumerError>
        where F: Fn(Payload) -> Result<T, E> + Send + 'static
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

impl<T, E> Stream for Consumer<T, E> {
    type Item = (Result<T, E>, Ack);
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

impl<T, E> Drop for Consumer<T, E> {
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
    pub fn build<T: DeserializeOwned>(self) -> impl Future<Item=Consumer<T, ConsumerError>, Error=ConsumerError> {
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
    pub fn build(self) -> impl Future<Item=Consumer<T, ConsumerError>, Error=ConsumerError> {
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

pub struct MultiTopicConsumer<T, E> {
    namespace: String,
    topic_regex: Regex,
    pulsar: Pulsar,
    consumers: BTreeMap<String, Consumer<T, E>>,
    topics: VecDeque<String>,
    new_consumers: Option<Box<dyn Future<Item=Vec<(String, Consumer<T, E>)>, Error=Error> + Send>>,
    refresh: Box<dyn Stream<Item=(), Error=()> + Send>,
    subscription: String,
    sub_type: SubType,
    deserialize: Arc<dyn Fn(Payload) -> Result<T, E> + Send + Sync + 'static>,
}

impl<T, E> MultiTopicConsumer<T, E> {
    //TODO: Expose builder API
    pub fn new<S1, S2, F>(
        pulsar: Pulsar,
        namespace: S1,
        topic_regex: Regex,
        subscription: S2,
        sub_type: SubType,
        deserialize: F,
        topic_refresh: Duration,
    ) -> Self
        where S1: Into<String>,
              S2: Into<String>,
              F: Fn(Payload) -> Result<T, E> + Send + Sync + 'static
    {
        MultiTopicConsumer {
            namespace: namespace.into(),
            topic_regex,
            pulsar,
            consumers: BTreeMap::new(),
            topics: VecDeque::new(),
            new_consumers: None,
            refresh: Box::new(Interval::new(Instant::now(), topic_refresh)
                .map(drop)
                .map_err(|e| panic!("error creating referesh timer: {}", e))),
            subscription: subscription.into(),
            sub_type,
            deserialize: Arc::new(deserialize),
        }
    }
}

impl<T, E> Debug for MultiTopicConsumer<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MultiTopicConsumer({:?}, {:?})", &self.namespace, &self.topic_regex)
    }
}

impl<T, E> Stream for MultiTopicConsumer<T, E>
    where T: 'static, E: 'static
{
    type Item = (Result<T, E>, Ack);
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
                    error!("Error creating pulsar consumers: {}", e);
                    // don't return error here; could be intermittent connection failure and we want
                    // to retry
                }
            }
        }

        if let Ok(Async::Ready(_)) = self.refresh.poll() {
            let regex = self.topic_regex.clone();
            let pulsar = self.pulsar.clone();
            let subscription = self.subscription.clone();
            let sub_type = self.sub_type;
            let deserialize = self.deserialize.clone();
            let new_consumers = Box::new(self.pulsar.get_topics_of_namespace(self.namespace.clone())
                .and_then(move |topics: Vec<String>| {
                    trace!("fetched topics: {:?}", &topics);
                    futures::future::collect(topics.into_iter()
                        .filter(move |topic| regex.is_match(topic.as_str()))
                        .map(move |topic| {
                            trace!("creating consumer for topic {}", topic);
                            let pulsar = pulsar.clone();
                            let deserialize = deserialize.clone();
                            let subscription = subscription.clone();
                            pulsar.create_consumer(topic.clone(), subscription, sub_type, move |payload| deserialize(payload))
                                .map(|c| (topic, c))
                        })
                    )
                }));
            self.new_consumers = Some(new_consumers);
            return self.poll();
        }

        for _ in 0..self.topics.len() {
            let topic = self.topics.pop_front().unwrap();
            let mut result = None;
            if let Some(item) = self.consumers.get_mut(&topic).map(|c| c.poll()) {
                match item {
                    Ok(Async::NotReady) => {}
                    Ok(Async::Ready(Some(data))) => result = Some(data),
                    Ok(Async::Ready(None)) => {
                        error!("Unexpected end of stream for pulsar topic {}", topic);
                        self.consumers.remove(&topic);
                        continue; //continue to avoid re-adding topic to list of topics
                    }
                    Err(e) => {
                        error!("Unexpected error consuming from pulsar topic {}: {}", topic, e);
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;
    use std::thread;

    use regex::Regex;
    use serde_json::json;
    use tokio::prelude::*;
    use tokio::runtime::Runtime;

    use crate::Pulsar;

    use super::*;

    #[test]
    #[ignore]
    fn multi_consumer() {
        let addr = "127.0.0.1:6650".parse().unwrap();
        let rt = Runtime::new().unwrap();

        let namespace = "public/default";
        let topic1 = "mt_test_a";
        let topic2 = "mt_test_b";

        let data1 = json!({"topic": "a", "msg": 1});
        let data2 = json!({"topic": "a", "msg": 2});
        let data3 = json!({"topic": "b", "msg": 1});
        let data4 = json!({"topic": "b", "msg": 2});

        let client: Pulsar = Pulsar::new(addr, None, rt.executor()).wait().unwrap();

        client.send_json(topic1, &data1, None).wait().unwrap();
        client.send_json(topic1, &data2, None).wait().unwrap();
        client.send_json(topic2, &data3, None).wait().unwrap();
        client.send_json(topic2, &data4, None).wait().unwrap();

        let data = vec![data1, data2, data3, data4];

        let consumer: MultiTopicConsumer<serde_json::Value, serde_json::Error> = client.create_multi_topic_consumer(
            Regex::new("mt_test_[ab]").unwrap(),
            "test_sub",
            namespace,
            SubType::Shared,
            |payload| serde_json::from_slice(&payload.data),
            Duration::from_secs(1),
        );

        let error: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let successes = Arc::new(AtomicUsize::new(0));

        rt.executor().spawn({
            let successes = successes.clone();
            consumer.take(4)
                .for_each(move |(msg, ack)| {
                    ack.ack();
                    let msg = msg.unwrap();
                    if !data.contains(&msg) {
                        Err(Error::Custom(format!("Unexpected message: {:?}", &msg)))
                    } else {
                        successes.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    }
                })
                .map_err({
                    let error = error.clone();
                    move |e| {
                        let mut error = error.lock().unwrap();
                        *error = Some(e);
                    }
                })
        });

        let start = Instant::now();
        loop {
            let success_count = successes.load(Ordering::Relaxed);
            if success_count == 4 {
                break;
            } else if start.elapsed() > Duration::from_secs(3) {
                panic!("Messages not received within timeout");
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}