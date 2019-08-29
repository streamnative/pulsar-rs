use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::{Async, Stream, sync::mpsc};
use futures::Future;
use futures::sync::mpsc::{unbounded, UnboundedSender};
use rand;
use regex::Regex;
use tokio::runtime::TaskExecutor;
use tokio::timer::Interval;

use crate::{DeserializeMessage, Pulsar};
use crate::connection::{Authentication, Connection};
use crate::error::{ConnectionError, ConsumerError, Error};
use crate::message::{Message as RawMessage, Payload, proto::{self, command_subscribe::SubType, Schema, MessageIdData}};

#[derive(Clone, Default)]
pub struct ConsumerOptions {
  pub priority_level: Option<i32>,
  pub durable: Option<bool>,
  pub start_message_id: Option<MessageIdData>,
  pub metadata: BTreeMap<String, String>,
  pub read_compacted: Option<bool>,
  pub schema: Option<Schema>,
  pub initial_position: Option<i32>,

}

pub struct Consumer<T: DeserializeMessage> {
    connection: Arc<Connection>,
    topic: String,
    id: u64,
    messages: mpsc::UnboundedReceiver<RawMessage>,
    batch_size: u32,
    remaining_messages: u32,
    #[allow(unused)]
    data_type: PhantomData<fn(Payload) -> T::Output>,
    options: ConsumerOptions,
}

impl<T: DeserializeMessage> Consumer<T> {
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
        batch_size: Option<u32>,
        options: ConsumerOptions,
    ) -> impl Future<Item=Consumer<T>, Error=Error> {
        let consumer_id = consumer_id.unwrap_or_else(rand::random);
        let (resolver, messages) = mpsc::unbounded();
        let batch_size = batch_size.unwrap_or(1000);

        let opt = options.clone();
        Connection::new(addr, auth_data, proxy_to_broker_url, executor.clone())
            .and_then({
                let topic = topic.clone();
                move |conn|
                    conn.sender().subscribe(resolver, topic, subscription, sub_type, consumer_id, consumer_name, opt)
                        .map(move |resp| (resp, conn))
            })
            .and_then(move |(_, conn)| {
                conn.sender().send_flow(consumer_id, batch_size)
                    .map(move |()| conn)
            })
            .map_err(|e| e.into())
            .map(move |connection| {
                Consumer {
                    connection: Arc::new(connection),
                    topic,
                    id: consumer_id,
                    messages,
                    batch_size,
                    remaining_messages: batch_size,
                    data_type: PhantomData,
                    options,
                }
            })
    }

    pub fn from_connection(
        conn: Arc<Connection>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        consumer_id: Option<u64>,
        consumer_name: Option<String>,
        batch_size: Option<u32>,
        options: ConsumerOptions,
    ) -> impl Future<Item=Consumer<T>, Error=Error> {
        let consumer_id = consumer_id.unwrap_or_else(rand::random);
        let (resolver, messages) = mpsc::unbounded();
        let batch_size = batch_size.unwrap_or(1000);

        conn.sender().subscribe(resolver, topic.clone(), subscription, sub_type, consumer_id, consumer_name, options.clone())
            .map(move |resp| (resp, conn))
            .and_then(move |(_, conn)| {
                conn.sender().send_flow(consumer_id, batch_size)
                    .map(move |()| conn)
            })
            .map_err(|e| Error::Consumer(ConsumerError::Connection(e)))
            .map(move |connection| {
                Consumer {
                    connection,
                    topic,
                    id: consumer_id,
                    messages,
                    batch_size,
                    remaining_messages: batch_size,
                    data_type: PhantomData,
                    options,
                }
            })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn options(&self) -> &ConsumerOptions {
        &self.options
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
        let _ = self.connection.sender().send_ack(self.consumer_id, self.message_id, false);
    }

    pub fn cumulative_ack(self) {
        let _ = self.connection.sender().send_ack(self.consumer_id, self.message_id, true);
    }
}

impl<T: DeserializeMessage> Stream for Consumer<T> {
    type Item = Message<T::Output>;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if !self.connection.is_valid() {
            if let Some(err) = self.connection.error() {
                return Err(Error::Consumer(ConsumerError::Connection(err)));
            }
        }

        if self.remaining_messages <= self.batch_size / 2 {
            self.connection.sender().send_flow(self.id, self.batch_size - self.remaining_messages)?;
            self.remaining_messages = self.batch_size;
        }

        let message: Option<Option<(proto::CommandMessage, Payload)>> = try_ready!(self.messages.poll().map_err(|_| ConnectionError::Disconnected))
            .map(|RawMessage { command, payload }|
                command.message
                    .and_then(move |msg| payload
                        .map(move |payload| (msg, payload))));

        if message.is_some() {
            self.remaining_messages -= 1;
        }

        match message {
            Some(Some((message, payload))) => {
                Ok(Async::Ready(Some(Message {
                    topic: self.topic.clone(),
                    payload: T::deserialize_message(payload),
                    ack: Ack::new(self.id, message.message_id, self.connection.clone()),
                })))
            }
            Some(None) => Err(Error::Consumer(ConsumerError::MissingPayload(format!("Missing payload for message {:?}", message)))),
            None => Ok(Async::Ready(None))
        }
    }
}

impl<T: DeserializeMessage> Drop for Consumer<T> {
    fn drop(&mut self) {
        let _ = self.connection.sender().close_consumer(self.id);
    }
}

pub struct Set<T>(T);

pub struct Unset;


pub struct ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
    pulsar: &'a Pulsar,
    topic: Topic,
    subscription: Subscription,
    subscription_type: SubscriptionType,
    consumer_id: Option<u64>,
    consumer_name: Option<String>,
    batch_size: Option<u32>,
    consumer_options: Option<ConsumerOptions>,

    // Currently only used for multi-topic
    namespace: Option<String>,
    topic_refresh: Option<Duration>,
}

impl<'a> ConsumerBuilder<'a, Unset, Unset, Unset> {
    pub fn new(pulsar: &'a Pulsar) -> Self {
        ConsumerBuilder {
            pulsar,
            topic: Unset,
            subscription: Unset,
            subscription_type: Unset,
            consumer_id: None,
            consumer_name: None,
            batch_size: None,
            consumer_options: None,
            namespace: None,
            topic_refresh: None,
        }
    }
}

impl<'a, Subscription, SubscriptionType> ConsumerBuilder<'a, Unset, Subscription, SubscriptionType> {
    pub fn with_topic<S: Into<String>>(self, topic: S) -> ConsumerBuilder<'a, Set<String>, Subscription, SubscriptionType> {
        ConsumerBuilder {
            pulsar: self.pulsar,
            topic: Set(topic.into()),
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            consumer_options: self.consumer_options,
            batch_size: self.batch_size,
            namespace: self.namespace,
            topic_refresh: self.topic_refresh,
        }
    }

    pub fn multi_topic(self, regex: Regex) -> ConsumerBuilder<'a, Set<Regex>, Subscription, SubscriptionType> {
        ConsumerBuilder {
            pulsar: self.pulsar,
            topic: Set(regex),
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            consumer_options: self.consumer_options,
            batch_size: self.batch_size,
            namespace: self.namespace,
            topic_refresh: self.topic_refresh,
        }
    }
}

impl<'a, Topic, SubscriptionType> ConsumerBuilder<'a, Topic, Unset, SubscriptionType> {
    pub fn with_subscription<S: Into<String>>(self, subscription: S) -> ConsumerBuilder<'a, Topic, Set<String>, SubscriptionType> {
        ConsumerBuilder {
            pulsar: self.pulsar,
            subscription: Set(subscription.into()),
            topic: self.topic,
            subscription_type: self.subscription_type,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            consumer_options: self.consumer_options,
            batch_size: self.batch_size,
            namespace: self.namespace,
            topic_refresh: self.topic_refresh,
        }
    }
}

impl<'a, Topic, Subscription> ConsumerBuilder<'a, Topic, Subscription, Unset> {
    pub fn with_subscription_type(self, subscription_type: SubType) -> ConsumerBuilder<'a, Topic, Subscription, Set<SubType>> {
        ConsumerBuilder {
            pulsar: self.pulsar,
            subscription_type: Set(subscription_type),
            topic: self.topic,
            subscription: self.subscription,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            consumer_options: self.consumer_options,
            batch_size: self.batch_size,
            namespace: self.namespace,
            topic_refresh: self.topic_refresh,
        }
    }
}

impl<'a, Subscription, SubscriptionType> ConsumerBuilder<'a, Set<Regex>, Subscription, SubscriptionType> {
    pub fn with_namespace<S: Into<String>>(self, namespace: S) -> ConsumerBuilder<'a, Set<Regex>, Subscription, SubscriptionType> {
        ConsumerBuilder {
            pulsar: self.pulsar,
            topic: self.topic,
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_name: self.consumer_name,
            consumer_id: self.consumer_id,
            consumer_options: self.consumer_options,
            batch_size: self.batch_size,
            namespace: Some(namespace.into()),
            topic_refresh: self.topic_refresh,
        }
    }

    pub fn with_topic_refresh(self, refresh_interval: Duration) -> ConsumerBuilder<'a, Set<Regex>, Subscription, SubscriptionType> {
        ConsumerBuilder {
            pulsar: self.pulsar,
            topic: self.topic,
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_name: self.consumer_name,
            consumer_id: self.consumer_id,
            consumer_options: self.consumer_options,
            batch_size: self.batch_size,
            namespace: self.namespace,
            topic_refresh: Some(refresh_interval),
        }
    }
}

impl<'a, Topic, Subscription, SubscriptionType> ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
    pub fn with_consumer_id(mut self, consumer_id: u64) -> ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
        self.consumer_id = Some(consumer_id);
        self
    }

    pub fn with_consumer_name<S: Into<String>>(mut self, consumer_name: S) -> ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    pub fn with_batch_size(mut self, batch_size: u32) -> ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn with_options(mut self, options: ConsumerOptions) -> ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
        self.consumer_options = Some(options);
        self
    }
}

impl<'a> ConsumerBuilder<'a, Set<String>, Set<String>, Set<SubType>> {
    pub fn build<T: DeserializeMessage>(self) -> impl Future<Item=Consumer<T>, Error=Error> {
        let ConsumerBuilder {
            pulsar,
            topic: Set(topic),
            subscription: Set(subscription),
            subscription_type: Set(sub_type),
            consumer_id,
            consumer_name,
            consumer_options,
            batch_size,
            ..
        } = self;

        pulsar.create_consumer(topic, subscription, sub_type, batch_size, consumer_name, consumer_id,
          consumer_options.unwrap_or_else(ConsumerOptions::default))
    }
}

impl<'a> ConsumerBuilder<'a, Set<Regex>, Set<String>, Set<SubType>> {
    pub fn build<T: DeserializeMessage>(self) -> MultiTopicConsumer<T> {
        let ConsumerBuilder {
            pulsar,
            topic: Set(topic),
            subscription: Set(subscription),
            subscription_type: Set(sub_type),
            consumer_id,
            consumer_name,
            batch_size,
            topic_refresh,
            namespace,
            ..
        } = self;
        if consumer_id.is_none() {
            warn!("Multi-topic consumers cannot have a set consumer ID; ignoring.");
        }
        if consumer_name.is_none() {
            warn!("Consumer name not currently supported for Multi-topic consumers; ignoring.");
        }
        if batch_size.is_none() {
            warn!("Batch size not currently supported for Multi-topic consumers; ignoring.");
        }
        let namespace = namespace.unwrap_or_else(|| "public/default".to_owned());
        let topic_refresh = topic_refresh.unwrap_or_else(|| Duration::from_secs(30));

        pulsar.create_multi_topic_consumer(topic, subscription, namespace, sub_type, topic_refresh, ConsumerOptions::default())
    }
}

/// Details about the current state of the Consumer
#[derive(Debug, Clone)]
pub struct ConsumerState {
    pub connected_topics: Vec<String>,
    pub last_message_received: Option<DateTime<Utc>>,
    pub messages_received: u64,
}

pub struct MultiTopicConsumer<T: DeserializeMessage> {
    namespace: String,
    topic_regex: Regex,
    pulsar: Pulsar,
    consumers: BTreeMap<String, Consumer<T>>,
    topics: VecDeque<String>,
    new_consumers: Option<Box<dyn Future<Item=Vec<Consumer<T>>, Error=Error> + Send>>,
    refresh: Box<dyn Stream<Item=(), Error=()> + Send>,
    subscription: String,
    sub_type: SubType,
    options: ConsumerOptions,
    last_message_received: Option<DateTime<Utc>>,
    messages_received: u64,
    state_streams: Vec<UnboundedSender<ConsumerState>>,
}

impl<T: DeserializeMessage> MultiTopicConsumer<T> {
    pub fn new<S1, S2>(
        pulsar: Pulsar,
        namespace: S1,
        topic_regex: Regex,
        subscription: S2,
        sub_type: SubType,
        topic_refresh: Duration,
        options: ConsumerOptions,
    ) -> Self
        where S1: Into<String>,
              S2: Into<String>,
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
            last_message_received: None,
            messages_received: 0,
            state_streams: vec![],
            options,
        }
    }

    pub fn start_state_stream(&mut self) -> impl Stream<Item=ConsumerState, Error=()> {
        let (tx, rx) = unbounded();
        self.state_streams.push(tx);
        rx
    }

    fn send_state(&mut self) {
        if !self.state_streams.is_empty() {
            let state = ConsumerState {
                connected_topics: self.consumers.keys().cloned().collect(),
                last_message_received: self.last_message_received,
                messages_received: self.messages_received,
            };
            self.state_streams.retain(|s| {
                s.unbounded_send(state.clone()).is_ok()
            });
        }
    }

    fn record_message(&mut self) {
        self.last_message_received = Some(Utc::now());
        self.messages_received += 1;
        self.send_state();
    }

    fn add_consumers<I: IntoIterator<Item=Consumer<T>>>(&mut self, consumers: I) {
        for consumer in consumers {
            let topic = consumer.topic().to_owned();
            self.consumers.insert(topic.clone(), consumer);
            self.topics.push_back(topic);
        }

        self.send_state();
    }

    fn remove_consumers(&mut self, topics: &[String]) {
        self.topics.retain(|t| !topics.contains(t));
        for topic in topics {
            self.consumers.remove(topic);
        }
        self.send_state();
    }
}

pub struct Message<T> {
    pub topic: String,
    pub payload: T,
    pub ack: Ack,
}

impl<T: DeserializeMessage> Debug for MultiTopicConsumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MultiTopicConsumer({:?}, {:?})", &self.namespace, &self.topic_regex)
    }
}

impl<T: 'static + DeserializeMessage> Stream for MultiTopicConsumer<T> {
    type Item = Message<T::Output>;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if let Some(mut new_consumers) = self.new_consumers.take() {
            match new_consumers.poll() {
                Ok(Async::Ready(new_consumers)) => {
                    self.add_consumers(new_consumers);
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
            let existing_topics: BTreeSet<String> = self.consumers.keys().cloned().collect();
            let options = self.options.clone();

            let new_consumers = Box::new(self.pulsar.get_topics_of_namespace(self.namespace.clone())
                .and_then(move |topics: Vec<String>| {
                    trace!("fetched topics: {:?}", &topics);
                    futures::future::collect(topics.into_iter()
                        .filter(move |topic| !existing_topics.contains(topic) && regex.is_match(topic.as_str()))
                        .map(move |topic| {
                            trace!("creating consumer for topic {}", topic);
                            let pulsar = pulsar.clone();
                            let subscription = subscription.clone();
                            pulsar.create_consumer(topic, subscription, sub_type, None, None, None, options.clone())
                        })
                    )
                }));
            self.new_consumers = Some(new_consumers);
            return self.poll();
        }

        let mut topics_to_remove = Vec::new();
        let mut result = None;
        for _ in 0..self.topics.len() {
            if result.is_some() {
                break;
            }
            let topic = self.topics.pop_front().unwrap();
            if let Some(item) = self.consumers.get_mut(&topic).map(|c| c.poll()) {
                match item {
                    Ok(Async::NotReady) => {}
                    Ok(Async::Ready(Some(msg))) => result = Some(msg),
                    Ok(Async::Ready(None)) => {
                        error!("Unexpected end of stream for pulsar topic {}", &topic);
                        topics_to_remove.push(topic.clone());
                    }
                    Err(e) => {
                        error!("Unexpected error consuming from pulsar topic {}: {}", &topic, e);
                        topics_to_remove.push(topic.clone());
                    }
                }
            } else {
                println!("BUG: Missing consumer for topic {}", &topic);
            }
            self.topics.push_back(topic);
        }
        self.remove_consumers(&topics_to_remove);
        if let Some(result) = result {
            self.record_message();
            return Ok(Async::Ready(Some(result)));
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
    use crate::producer::ProducerOptions;

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

        let send_start = Utc::now();
        client.send_json(topic1, &data1, None, ProducerOptions::default()).wait().unwrap();
        client.send_json(topic1, &data2, None, ProducerOptions::default()).wait().unwrap();
        client.send_json(topic2, &data3, None, ProducerOptions::default()).wait().unwrap();
        client.send_json(topic2, &data4, None, ProducerOptions::default()).wait().unwrap();

        let data = vec![data1, data2, data3, data4];

        let mut consumer: MultiTopicConsumer<serde_json::Value> = client.consumer()
            .multi_topic(Regex::new("mt_test_[ab]").unwrap())
            .with_namespace(namespace)
            .with_subscription("test_sub")
            .with_subscription_type(SubType::Shared)
            .with_topic_refresh(Duration::from_secs(1))
            .build();

        let consumer_state = consumer.start_state_stream();

        let error: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let successes = Arc::new(AtomicUsize::new(0));

        rt.executor().spawn({
            let successes = successes.clone();
            consumer.take(4)
                .for_each(move |Message { payload, ack, .. }| {
                    ack.ack();
                    let msg = payload.unwrap();
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

        let consumer_state: Vec<ConsumerState> = consumer_state.collect()
            .wait()
            .unwrap();
        let latest_state = consumer_state.last().unwrap();
        assert!(latest_state.messages_received >= 4);
        assert!(latest_state.connected_topics.len() >= 2);
        assert!(latest_state.last_message_received.unwrap() >= send_start);
    }
}
