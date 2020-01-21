use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::Future;
use futures::{sync::mpsc, Async, Stream};
use rand;
use regex::Regex;
use tokio::runtime::TaskExecutor;
use tokio::timer::Interval;

use crate::connection::{Authentication, Connection};
use crate::error::{ConnectionError, ConsumerError, Error};
use crate::message::{
    proto::{self, command_subscribe::SubType, MessageIdData, Schema},
    Message as RawMessage, Payload,
};
use crate::{DeserializeMessage, Pulsar};
use nom::lib::std::cmp::Ordering;
use nom::lib::std::collections::BinaryHeap;
use tokio::prelude::Poll;

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
    ack_handler: UnboundedSender<AckMessage>,
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
        unacked_message_redelivery_delay: Option<Duration>,
        options: ConsumerOptions,
    ) -> impl Future<Item = Consumer<T>, Error = Error> {
        Connection::new(addr, auth_data, proxy_to_broker_url, executor.clone())
            .from_err()
            .and_then(move |conn| {
                Consumer::from_connection(
                    Arc::new(conn),
                    topic,
                    subscription,
                    sub_type,
                    consumer_id,
                    consumer_name,
                    batch_size,
                    unacked_message_redelivery_delay,
                    options,
                )
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
        unacked_message_redelivery_delay: Option<Duration>,
        options: ConsumerOptions,
    ) -> impl Future<Item = Consumer<T>, Error = Error> {
        let consumer_id = consumer_id.unwrap_or_else(rand::random);
        let (resolver, messages) = mpsc::unbounded();
        let batch_size = batch_size.unwrap_or(1000);

        conn.sender()
            .subscribe(
                resolver,
                topic.clone(),
                subscription,
                sub_type,
                consumer_id,
                consumer_name,
                options.clone(),
            )
            .map(move |resp| (resp, conn))
            .and_then(move |(_, conn)| {
                conn.sender()
                    .send_flow(consumer_id, batch_size)
                    .map(move |()| conn)
            })
            .map_err(|e| Error::Consumer(ConsumerError::Connection(e)))
            .map(move |connection| {
                //TODO this should be shared among all consumers when using the client
                //TODO make tick_delay configurable
                let tick_delay = Duration::from_millis(500);
                let ack_handler = AckHandler::new(
                    connection.clone(),
                    unacked_message_redelivery_delay,
                    tick_delay,
                    &connection.executor(),
                );
                Consumer {
                    connection,
                    topic,
                    id: consumer_id,
                    messages,
                    ack_handler,
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
    message_ids: BTreeMap<u64, Vec<proto::MessageIdData>>,
    sender: UnboundedSender<AckMessage>,
}

impl Ack {
    fn new(
        consumer_id: u64,
        msg: proto::MessageIdData,
        sender: UnboundedSender<AckMessage>,
    ) -> Ack {
        let mut message_ids = BTreeMap::new();
        message_ids.insert(consumer_id, vec![msg]);
        Ack {
            message_ids,
            sender,
        }
    }

    pub fn join(mut self, mut other: Ack) -> Self {
        for (consumer_id, message_ids) in other.take_message_ids() {
            self.message_ids
                .entry(consumer_id)
                .or_insert_with(Vec::new)
                .extend(message_ids);
        }
        self
    }

    pub fn extend<I: IntoIterator<Item = Ack>>(mut self, others: I) -> Self {
        others
            .into_iter()
            .flat_map(|mut o| o.take_message_ids())
            .for_each(|(consumer_id, message_ids)| {
                self.message_ids
                    .entry(consumer_id)
                    .or_insert_with(Vec::new)
                    .extend(message_ids);
            });
        self
    }

    pub fn ack(mut self) {
        for (consumer_id, message_ids) in self.take_message_ids() {
            let _ = self.sender.unbounded_send(AckMessage::Ack {
                consumer_id,
                message_ids,
                cumulative: false,
            });
        }
    }

    pub fn cumulative_ack(mut self) {
        for (consumer_id, message_ids) in self.take_message_ids() {
            let _ = self.sender.unbounded_send(AckMessage::Ack {
                consumer_id,
                message_ids,
                cumulative: true,
            });
        }
    }

    pub fn nack(mut self) {
        self.send_nack();
    }

    fn take_message_ids(&mut self) -> BTreeMap<u64, Vec<proto::MessageIdData>> {
        let mut message_ids = BTreeMap::new();
        std::mem::swap(&mut message_ids, &mut self.message_ids);
        message_ids
    }

    fn send_nack(&mut self) {
        for (consumer_id, message_ids) in self.take_message_ids() {
            let _ = self.sender.unbounded_send(AckMessage::Nack {
                consumer_id,
                message_ids,
            });
        }
    }
}

impl Drop for Ack {
    fn drop(&mut self) {
        if !self.message_ids.is_empty() {
            self.send_nack();
        }
    }
}

enum AckMessage {
    Ack {
        consumer_id: u64,
        message_ids: Vec<MessageIdData>,
        cumulative: bool,
    },
    Nack {
        consumer_id: u64,
        message_ids: Vec<MessageIdData>,
    },
}

#[derive(Debug, PartialEq, Eq)]
struct MessageResend {
    when: Instant,
    consumer_id: u64,
    message_ids: Vec<MessageIdData>,
}

impl PartialOrd for MessageResend {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Ordering is defined for use in a BinaryHeap (a max heap), so ordering is reversed to cause
// earlier `when`s to be at the front of the queue
impl Ord for MessageResend {
    fn cmp(&self, other: &Self) -> Ordering {
        self.when.cmp(&other.when).reverse()
    }
}

struct AckHandler {
    pending_nacks: BinaryHeap<MessageResend>,
    conn: Arc<Connection>,
    inbound: Option<UnboundedReceiver<AckMessage>>,
    unack_redelivery_delay: Option<Duration>,
    tick_timer: tokio::timer::Interval,
}

impl AckHandler {
    /// Create and spawn a new AckHandler future, which will run until the connection fails, or all
    /// inbound senders are dropped and any pending redelivery messages have been sent
    pub fn new(
        conn: Arc<Connection>,
        redelivery_delay: Option<Duration>,
        tick_delay: Duration,
        executor: &TaskExecutor,
    ) -> UnboundedSender<AckMessage> {
        let (tx, rx) = mpsc::unbounded();
        executor.spawn(AckHandler {
            pending_nacks: BinaryHeap::new(),
            conn,
            inbound: Some(rx),
            unack_redelivery_delay: redelivery_delay,
            tick_timer: Interval::new_interval(tick_delay),
        });
        tx
    }
    fn next_ready_resend(&mut self) -> Option<MessageResend> {
        if let Some(resend) = self.pending_nacks.peek() {
            if resend.when <= Instant::now() {
                return self.pending_nacks.pop();
            }
        }
        None
    }
    fn next_inbound(&mut self) -> Option<AckMessage> {
        if let Some(inbound) = &mut self.inbound {
            match inbound.poll() {
                Ok(Async::Ready(Some(msg))) => Some(msg),
                Ok(Async::NotReady) => None,
                Ok(Async::Ready(None)) | Err(_) => {
                    self.inbound = None;
                    None
                }
            }
        } else {
            None
        }
    }
}

impl Future for AckHandler {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut acks: BTreeMap<(u64, bool), Vec<MessageIdData>> = BTreeMap::new();
        while let Some(msg) = self.next_inbound() {
            match msg {
                AckMessage::Ack {
                    consumer_id,
                    message_ids,
                    cumulative,
                } => {
                    acks.entry((consumer_id, cumulative))
                        .or_insert_with(Vec::new)
                        .extend(message_ids);
                }
                AckMessage::Nack {
                    consumer_id,
                    message_ids,
                } => {
                    // if timeout is not set, messages will only be redelivered on reconnect,
                    // so we don't manually send redelivery request
                    if let Some(nack_timeout) = self.unack_redelivery_delay {
                        self.pending_nacks.push(MessageResend {
                            consumer_id,
                            when: Instant::now() + nack_timeout,
                            message_ids,
                        });
                    }
                }
            }
        }
        //TODO should this be batched with the tick timer?
        for ((consumer_id, cumulative), message_ids) in acks {
            //TODO this should be resilient to reconnects
            let send_result = self
                .conn
                .sender()
                .send_ack(consumer_id, message_ids, cumulative);
            if send_result.is_err() {
                return Err(());
            }
        }
        loop {
            match self.tick_timer.poll() {
                Ok(Async::Ready(Some(_))) => {
                    let mut resends: BTreeMap<u64, Vec<MessageIdData>> = BTreeMap::new();
                    while let Some(ready) = self.next_ready_resend() {
                        resends
                            .entry(ready.consumer_id)
                            .or_insert_with(Vec::new)
                            .extend(ready.message_ids);
                    }
                    for (consumer_id, message_ids) in resends {
                        //TODO this should be resilient to reconnects
                        let send_result = self
                            .conn
                            .sender()
                            .send_redeliver_unacknowleged_messages(consumer_id, message_ids);
                        if send_result.is_err() {
                            return Err(());
                        }
                    }
                }
                Ok(Async::NotReady) => {
                    if self.inbound.is_none() && self.pending_nacks.is_empty() {
                        return Ok(Async::Ready(()));
                    } else {
                        return Ok(Async::NotReady);
                    }
                }
                Ok(Async::Ready(None)) | Err(_) => return Err(()),
            }
        }
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
            self.connection
                .sender()
                .send_flow(self.id, self.batch_size - self.remaining_messages)?;
            self.remaining_messages = self.batch_size;
        }

        let message: Option<Option<(proto::CommandMessage, Payload)>> = try_ready!(self
            .messages
            .poll()
            .map_err(|_| ConnectionError::Disconnected))
        .map(|RawMessage { command, payload }| {
            command
                .message
                .and_then(move |msg| payload.map(move |payload| (msg, payload)))
        });

        if message.is_some() {
            self.remaining_messages -= 1;
        }

        match message {
            Some(Some((message, payload))) => Ok(Async::Ready(Some(Message {
                topic: self.topic.clone(),
                payload: T::deserialize_message(payload),
                ack: Ack::new(self.id, message.message_id, self.ack_handler.clone()),
            }))),
            Some(None) => Err(Error::Consumer(ConsumerError::MissingPayload(format!(
                "Missing payload for message {:?}",
                message
            )))),
            None => Ok(Async::Ready(None)),
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
    unacked_message_resend_delay: Option<Duration>,
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
            //TODO what should this default to? None seems incorrect..
            unacked_message_resend_delay: None,
            consumer_options: None,
            namespace: None,
            topic_refresh: None,
        }
    }
}

impl<'a, Subscription, SubscriptionType>
    ConsumerBuilder<'a, Unset, Subscription, SubscriptionType>
{
    pub fn with_topic<S: Into<String>>(
        self,
        topic: S,
    ) -> ConsumerBuilder<'a, Set<String>, Subscription, SubscriptionType> {
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
            unacked_message_resend_delay: self.unacked_message_resend_delay,
        }
    }

    pub fn multi_topic(
        self,
        regex: Regex,
    ) -> ConsumerBuilder<'a, Set<Regex>, Subscription, SubscriptionType> {
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
            unacked_message_resend_delay: self.unacked_message_resend_delay,
        }
    }
}

impl<'a, Topic, SubscriptionType> ConsumerBuilder<'a, Topic, Unset, SubscriptionType> {
    pub fn with_subscription<S: Into<String>>(
        self,
        subscription: S,
    ) -> ConsumerBuilder<'a, Topic, Set<String>, SubscriptionType> {
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
            unacked_message_resend_delay: self.unacked_message_resend_delay,
        }
    }
}

impl<'a, Topic, Subscription> ConsumerBuilder<'a, Topic, Subscription, Unset> {
    pub fn with_subscription_type(
        self,
        subscription_type: SubType,
    ) -> ConsumerBuilder<'a, Topic, Subscription, Set<SubType>> {
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
            unacked_message_resend_delay: self.unacked_message_resend_delay,
        }
    }
}

impl<'a, Subscription, SubscriptionType>
    ConsumerBuilder<'a, Set<Regex>, Subscription, SubscriptionType>
{
    pub fn with_namespace<S: Into<String>>(
        self,
        namespace: S,
    ) -> ConsumerBuilder<'a, Set<Regex>, Subscription, SubscriptionType> {
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
            unacked_message_resend_delay: self.unacked_message_resend_delay,
        }
    }

    pub fn with_topic_refresh(
        self,
        refresh_interval: Duration,
    ) -> ConsumerBuilder<'a, Set<Regex>, Subscription, SubscriptionType> {
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
            unacked_message_resend_delay: self.unacked_message_resend_delay,
        }
    }
}

impl<'a, Topic, Subscription, SubscriptionType>
    ConsumerBuilder<'a, Topic, Subscription, SubscriptionType>
{
    pub fn with_consumer_id(
        mut self,
        consumer_id: u64,
    ) -> ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
        self.consumer_id = Some(consumer_id);
        self
    }

    pub fn with_consumer_name<S: Into<String>>(
        mut self,
        consumer_name: S,
    ) -> ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    pub fn with_batch_size(
        mut self,
        batch_size: u32,
    ) -> ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn with_options(
        mut self,
        options: ConsumerOptions,
    ) -> ConsumerBuilder<'a, Topic, Subscription, SubscriptionType> {
        self.consumer_options = Some(options);
        self
    }

    /// The time after which a message is dropped without being acknowledged or nacked
    /// that the message is resent. If `None`, messages will only be resent when a
    /// consumer disconnects with pending unacknowledged messages.
    pub fn with_unacked_message_resend_delay(mut self, delay: Option<Duration>) -> Self {
        self.unacked_message_resend_delay = delay;
        self
    }
}

impl<'a> ConsumerBuilder<'a, Set<String>, Set<String>, Set<SubType>> {
    pub fn build<T: DeserializeMessage>(self) -> impl Future<Item = Consumer<T>, Error = Error> {
        let ConsumerBuilder {
            pulsar,
            topic: Set(topic),
            subscription: Set(subscription),
            subscription_type: Set(sub_type),
            consumer_id,
            consumer_name,
            consumer_options,
            batch_size,
            unacked_message_resend_delay,
            ..
        } = self;

        pulsar.create_consumer(
            topic,
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_resend_delay,
            consumer_options.unwrap_or_else(ConsumerOptions::default),
        )
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
            unacked_message_resend_delay,
            ..
        } = self;
        if consumer_id.is_some() {
            warn!("Multi-topic consumers cannot have a set consumer ID; ignoring.");
        }
        if consumer_name.is_some() {
            warn!("Consumer name not currently supported for Multi-topic consumers; ignoring.");
        }
        if batch_size.is_some() {
            warn!("Batch size not currently supported for Multi-topic consumers; ignoring.");
        }
        let namespace = namespace.unwrap_or_else(|| "public/default".to_owned());
        let topic_refresh = topic_refresh.unwrap_or_else(|| Duration::from_secs(30));

        pulsar.create_multi_topic_consumer(
            topic,
            subscription,
            namespace,
            sub_type,
            topic_refresh,
            unacked_message_resend_delay,
            ConsumerOptions::default(),
        )
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
    unacked_message_resend_delay: Option<Duration>,
    consumers: BTreeMap<String, Consumer<T>>,
    topics: VecDeque<String>,
    new_consumers: Option<Box<dyn Future<Item = Vec<Consumer<T>>, Error = Error> + Send>>,
    refresh: Box<dyn Stream<Item = (), Error = ()> + Send>,
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
        unacked_message_resend_delay: Option<Duration>,
        options: ConsumerOptions,
    ) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        MultiTopicConsumer {
            namespace: namespace.into(),
            topic_regex,
            pulsar,
            unacked_message_resend_delay,
            consumers: BTreeMap::new(),
            topics: VecDeque::new(),
            new_consumers: None,
            refresh: Box::new(
                Interval::new(Instant::now(), topic_refresh)
                    .map(drop)
                    .map_err(|e| panic!("error creating referesh timer: {}", e)),
            ),
            subscription: subscription.into(),
            sub_type,
            last_message_received: None,
            messages_received: 0,
            state_streams: vec![],
            options,
        }
    }

    pub fn start_state_stream(&mut self) -> impl Stream<Item = ConsumerState, Error = ()> {
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
            self.state_streams
                .retain(|s| s.unbounded_send(state.clone()).is_ok());
        }
    }

    fn record_message(&mut self) {
        self.last_message_received = Some(Utc::now());
        self.messages_received += 1;
        self.send_state();
    }

    fn add_consumers<I: IntoIterator<Item = Consumer<T>>>(&mut self, consumers: I) {
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
        write!(
            f,
            "MultiTopicConsumer({:?}, {:?})",
            &self.namespace, &self.topic_regex
        )
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
            let unacked_message_resend_delay = self.unacked_message_resend_delay;

            let new_consumers = Box::new(
                self.pulsar
                    .get_topics_of_namespace(self.namespace.clone(), proto::get_topics::Mode::All)
                    .and_then(move |topics: Vec<String>| {
                        trace!("fetched topics: {:?}", &topics);
                        futures::future::collect(
                            topics
                                .into_iter()
                                .filter(move |topic| {
                                    !existing_topics.contains(topic)
                                        && regex.is_match(topic.as_str())
                                })
                                .map(move |topic| {
                                    trace!("creating consumer for topic {}", topic);
                                    let pulsar = pulsar.clone();
                                    let subscription = subscription.clone();
                                    pulsar.create_consumer(
                                        topic,
                                        subscription,
                                        sub_type,
                                        None,
                                        None,
                                        None,
                                        unacked_message_resend_delay,
                                        options.clone(),
                                    )
                                }),
                        )
                    }),
            );
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
                        error!(
                            "Unexpected error consuming from pulsar topic {}: {}",
                            &topic, e
                        );
                        topics_to_remove.push(topic.clone());
                    }
                }
            } else {
                eprintln!("BUG: Missing consumer for topic {}", &topic);
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
    use tokio::prelude::*;
    use tokio::runtime::Runtime;

    use crate::{producer, Pulsar, SerializeMessage};

    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TestData {
        topic: String,
        msg: u32,
    }

    impl SerializeMessage for TestData {
        fn serialize_message(input: &Self) -> Result<producer::Message, Error> {
            let payload = serde_json::to_vec(input).map_err(|e| Error::Custom(e.to_string()))?;
            Ok(producer::Message {
                payload,
                ..Default::default()
            })
        }
    }

    impl DeserializeMessage for TestData {
        type Output = Result<TestData, serde_json::Error>;

        fn deserialize_message(payload: Payload) -> Self::Output {
            serde_json::from_slice(&payload.data)
        }
    }

    #[test]
    #[ignore]
    fn multi_consumer() {
        let addr = "127.0.0.1:6650".parse().unwrap();
        let rt = Runtime::new().unwrap();

        let namespace = "public/default";
        let topic1 = "mt_test_a";
        let topic2 = "mt_test_b";

        let data1 = TestData {
            topic: "a".to_owned(),
            msg: 1,
        };
        let data2 = TestData {
            topic: "a".to_owned(),
            msg: 2,
        };
        let data3 = TestData {
            topic: "b".to_owned(),
            msg: 1,
        };
        let data4 = TestData {
            topic: "b".to_owned(),
            msg: 2,
        };

        let client: Pulsar = Pulsar::new(addr, None, rt.executor()).wait().unwrap();
        let producer = client.producer(None);

        let send_start = Utc::now();
        producer.send(topic1, &data1).wait().unwrap();
        producer.send(topic1, &data2).wait().unwrap();
        producer.send(topic2, &data3).wait().unwrap();
        producer.send(topic2, &data4).wait().unwrap();

        let data = vec![data1, data2, data3, data4];

        let mut consumer: MultiTopicConsumer<TestData> = client
            .consumer()
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
            consumer
                .take(4)
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

        let consumer_state: Vec<ConsumerState> = consumer_state.collect().wait().unwrap();
        let latest_state = consumer_state.last().unwrap();
        assert!(latest_state.messages_received >= 4);
        assert!(latest_state.connected_topics.len() >= 2);
        assert!(latest_state.last_message_received.unwrap() >= send_start);
    }
}
