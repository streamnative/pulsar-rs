//! Topic subscriptions
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::channel::mpsc::unbounded;
use futures::task::{Context, Poll};
use futures::{
    channel::{mpsc, oneshot},
    future::{select, try_join_all, Either},
    pin_mut, Future, FutureExt, SinkExt, Stream, StreamExt,
};
use regex::Regex;

use crate::connection::Connection;
use crate::error::{ConnectionError, ConsumerError, Error};
use crate::executor::Executor;
use crate::message::proto::CommandMessage;
use crate::message::{
    parse_batched_message,
    proto::{self, command_subscribe::SubType, MessageIdData, MessageMetadata, Schema},
    BatchedMessage, Message as RawMessage, Metadata, Payload,
};
use crate::proto::{BaseCommand, CommandCloseConsumer, CommandGetLastMessageIdResponse};
use crate::reader::{Reader, State};
use crate::{BrokerAddress, DeserializeMessage, Pulsar};
use core::iter;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::convert::TryFrom;
use url::Url;

/// Configuration options for consumers
#[derive(Clone, Default, Debug)]
pub struct ConsumerOptions {
    pub priority_level: Option<i32>,
    /// Signal wether the subscription should be backed by a
    /// durable cursor or not
    pub durable: Option<bool>,
    /// If specified, the subscription will position the cursor
    /// marked-delete position on the particular message id and
    /// will send messages from that point
    pub start_message_id: Option<MessageIdData>,
    /// Add optional metadata key=value to this consumer
    pub metadata: BTreeMap<String, String>,
    pub read_compacted: Option<bool>,
    pub schema: Option<Schema>,
    /// Signal whether the subscription will initialize on latest
    /// or earliest message (default on latest)
    ///
    /// an enum can be used to initialize it:
    ///
    /// ```rust,ignore
    /// ConsumerOptions {
    ///     initial_position: InitialPosition::Earliest,
    /// }
    /// ```
    pub initial_position: InitialPosition,
}

impl ConsumerOptions {
    /// within options, sets the priority level
    pub fn with_priority_level(mut self, priority_level: i32) -> Self {
        self.priority_level = Some(priority_level);
        self
    }

    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = Some(durable);
        self
    }

    pub fn starting_on_message(mut self, message_id_data: MessageIdData) -> Self {
        self.start_message_id = Some(message_id_data);
        self
    }

    pub fn with_metadata(mut self, metadata: BTreeMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn read_compacted(mut self, read_compacted: bool) -> Self {
        self.read_compacted = Some(read_compacted);
        self
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn with_initial_position(mut self, initial_position: InitialPosition) -> Self {
        self.initial_position = initial_position;
        self
    }
}

#[derive(Debug, Clone)]
pub struct DeadLetterPolicy {
    /// Maximum number of times that a message will be redelivered before being sent to the dead letter queue.
    pub max_redeliver_count: usize,
    /// Name of the dead topic where the failing messages will be sent.
    pub dead_letter_topic: String,
}

/// position of the first message that will be consumed
#[derive(Clone, Debug)]
pub enum InitialPosition {
    /// start at the oldest message
    Earliest,
    /// start at the most recent message
    Latest,
}

impl Default for InitialPosition {
    fn default() -> Self {
        InitialPosition::Latest
    }
}
impl From<InitialPosition> for i32 {
    fn from(i: InitialPosition) -> Self {
        match i {
            InitialPosition::Earliest => 1,
            InitialPosition::Latest => 0,
        }
    }
}

/// the consumer is used to subscribe to a topic
///
/// ```rust,no_run
/// use pulsar::{Consumer, SubType};
/// use futures::StreamExt;
///
/// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
/// # type TestData = String;
/// let mut consumer: Consumer<TestData, _> = pulsar
///     .consumer()
///     .with_topic("non-persistent://public/default/test")
///     .with_consumer_name("test_consumer")
///     .with_subscription_type(SubType::Exclusive)
///     .with_subscription("test_subscription")
///     .build()
///     .await?;
///
/// let mut counter = 0usize;
/// while let Some(Ok(msg)) = consumer.next().await {
///     consumer.ack(&msg).await?;
///     let data = match msg.deserialize() {
///         Ok(data) => data,
///         Err(e) => {
///             log::error!("could not deserialize message: {:?}", e);
///             break;
///         }
///     };
///
///     counter += 1;
///     log::info!("got {} messages", counter);
/// }
/// # Ok(())
/// # }
/// ```
pub struct Consumer<T: DeserializeMessage, Exe: Executor> {
    inner: InnerConsumer<T, Exe>,
}
impl<T: DeserializeMessage, Exe: Executor> Consumer<T, Exe> {
    /// creates a [ConsumerBuilder] from a client instance
    pub fn builder(pulsar: &Pulsar<Exe>) -> ConsumerBuilder<Exe> {
        ConsumerBuilder::new(pulsar)
    }

    /// test that the connections to the Pulsar brokers are still valid
    pub async fn check_connection(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.check_connection().await,
            InnerConsumer::Multi(c) => c.check_connections().await,
        }
    }

    /// acknowledges a single message
    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.ack(msg).await,
            InnerConsumer::Multi(c) => c.ack(msg).await,
        }
    }

    /// acknowledges a message and all the preceding messages
    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.cumulative_ack(msg).await,
            InnerConsumer::Multi(c) => c.cumulative_ack(msg).await,
        }
    }

    /// negative acknowledgement
    ///
    /// the message will be sent again on the subscription
    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.nack(msg).await,
            InnerConsumer::Multi(c) => c.nack(msg).await,
        }
    }

    /// seek currently destroys the existing consumer and creates a new one
    /// this is how java and cpp pulsar client implement this feature mainly because
    /// there are many minor problems with flushing existing messages and receiving new ones
    pub async fn seek(
        &mut self,
        consumer_ids: Option<Vec<String>>,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
        client: Pulsar<Exe>,
    ) -> Result<(), Error> {
        let inner_consumer: InnerConsumer<T, Exe> = match &mut self.inner {
            InnerConsumer::Single(c) => {
                c.seek(message_id, timestamp).await?;
                let topic = c.topic().to_string();
                let addr = client.lookup_topic(&topic).await?;
                let config = c.config().clone();
                InnerConsumer::Single(TopicConsumer::new(client, topic, addr, config).await?)
            }
            InnerConsumer::Multi(c) => {
                c.seek(consumer_ids, message_id, timestamp).await?;
                let topics = c.topics();

                //currently, pulsar only supports seek for non partitioned topics
                let addrs =
                    try_join_all(topics.into_iter().map(|topic| client.lookup_topic(topic)))
                        .await?;

                let topic_addr_pair = c.topics.iter().cloned().zip(addrs.iter().cloned());

                let consumers = try_join_all(topic_addr_pair.map(|(topic, addr)| {
                    TopicConsumer::new(client.clone(), topic, addr, c.config().clone())
                }))
                .await?;

                let consumers: BTreeMap<_, _> = consumers
                    .into_iter()
                    .map(|c| (c.topic(), Box::pin(c)))
                    .collect();
                let topics = consumers.keys().cloned().collect();
                let topic_refresh = Duration::from_secs(30);
                let refresh = Box::pin(client.executor.interval(topic_refresh).map(drop));
                let namespace = c.namespace.clone();
                let config = c.config().clone();
                let topic_regex = c.topic_regex.clone();
                InnerConsumer::Multi(MultiTopicConsumer {
                    namespace,
                    topic_regex,
                    pulsar: client,
                    consumers,
                    topics,
                    new_consumers: None,
                    refresh,
                    config,
                    disc_last_message_received: None,
                    disc_messages_received: 0,
                })
            }
        };

        self.inner = inner_consumer;
        Ok(())
    }

    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.unsubscribe().await,
            InnerConsumer::Multi(c) => c.unsubscribe().await,
        }
    }

    pub async fn get_last_message_id(&mut self) -> Result<Vec<CommandGetLastMessageIdResponse>, Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => Ok(vec![c.get_last_message_id().await?]),
            InnerConsumer::Multi(c) => c.get_last_message_id().await,
        }
    }

    /// returns the list of topics this consumer is subscribed on
    pub fn topics(&self) -> Vec<String> {
        match &self.inner {
            InnerConsumer::Single(c) => vec![c.topic()],
            InnerConsumer::Multi(c) => c.topics(),
        }
    }

    /// returns a list of broker URLs this consumer is connnected to
    pub async fn connections(&mut self) -> Result<Vec<Url>, Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => Ok(vec![c.connection().await?.url().clone()]),
            InnerConsumer::Multi(c) => {
                let v = c
                    .consumers
                    .values_mut()
                    .map(|c| c.connection())
                    .collect::<Vec<_>>();

                let mut connections = try_join_all(v).await?;
                Ok(connections
                    .drain(..)
                    .map(|conn| conn.url().clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect())
            }
        }
    }

    /// returns the consumer's configuration options
    pub fn options(&self) -> &ConsumerOptions {
        match &self.inner {
            InnerConsumer::Single(c) => &c.config.options,
            InnerConsumer::Multi(c) => &c.config.options,
        }
    }

    /// returns the consumer's dead letter policy options
    pub fn dead_letter_policy(&self) -> Option<&DeadLetterPolicy> {
        match &self.inner {
            InnerConsumer::Single(c) => c.dead_letter_policy.as_ref(),
            InnerConsumer::Multi(c) => c.config.dead_letter_policy.as_ref(),
        }
    }

    /// returns the consumer's subscription name
    pub fn subscription(&self) -> &str {
        match &self.inner {
            InnerConsumer::Single(c) => &c.config.subscription,
            InnerConsumer::Multi(c) => &c.config.subscription,
        }
    }

    /// returns the consumer's subscription type
    pub fn sub_type(&self) -> SubType {
        match &self.inner {
            InnerConsumer::Single(c) => c.config.sub_type,
            InnerConsumer::Multi(c) => c.config.sub_type,
        }
    }

    /// returns the consumer's batch size
    pub fn batch_size(&self) -> Option<u32> {
        match &self.inner {
            InnerConsumer::Single(c) => c.config.batch_size,
            InnerConsumer::Multi(c) => c.config.batch_size,
        }
    }

    /// returns the consumer's name
    pub fn consumer_name(&self) -> Option<&str> {
        match &self.inner {
            InnerConsumer::Single(c) => &c.config.consumer_name,
            InnerConsumer::Multi(c) => &c.config.consumer_name,
        }
        .as_ref()
        .map(|s| s.as_str())
    }

    /// returns the consumer's list of ids
    pub fn consumer_id(&self) -> Vec<u64> {
        match &self.inner {
            InnerConsumer::Single(c) => vec![c.consumer_id],
            InnerConsumer::Multi(c) => c.consumers.values().map(|c| c.consumer_id).collect(),
        }
    }

    /// returns the consumer's redelivery delay
    ///
    /// if messages are not acknowledged before this delay, they will be sent
    /// again on the subscription
    pub fn unacked_message_redelivery_delay(&self) -> Option<Duration> {
        match &self.inner {
            InnerConsumer::Single(c) => c.config.unacked_message_redelivery_delay,
            InnerConsumer::Multi(c) => c.config.unacked_message_redelivery_delay,
        }
    }

    /// returns the date of the last message reception
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        match &self.inner {
            InnerConsumer::Single(c) => c.last_message_received(),
            InnerConsumer::Multi(c) => c.last_message_received(),
        }
    }

    /// returns the current number of messages received
    pub fn messages_received(&self) -> u64 {
        match &self.inner {
            InnerConsumer::Single(c) => c.messages_received(),
            InnerConsumer::Multi(c) => c.messages_received(),
        }
    }
}

//TODO: why does T need to be 'static?
impl<T: DeserializeMessage + 'static, Exe: Executor> Stream for Consumer<T, Exe> {
    type Item = Result<Message<T>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.inner {
            InnerConsumer::Single(c) => Pin::new(c).poll_next(cx),
            InnerConsumer::Multi(c) => Pin::new(c).poll_next(cx),
        }
    }
}

enum InnerConsumer<T: DeserializeMessage, Exe: Executor> {
    Single(TopicConsumer<T, Exe>),
    Multi(MultiTopicConsumer<T, Exe>),
}

type MessageIdDataReceiver = mpsc::Receiver<Result<(proto::MessageIdData, Payload), Error>>;

// this is entirely public for use in reader.rs
pub(crate) struct TopicConsumer<T: DeserializeMessage, Exe: Executor> {
    pub(crate) consumer_id: u64,
    pub(crate) config: ConsumerConfig,
    topic: String,
    messages: Pin<Box<MessageIdDataReceiver>>,
    engine_tx: mpsc::UnboundedSender<EngineMessage<Exe>>,
    #[allow(unused)]
    data_type: PhantomData<fn(Payload) -> T::Output>,
    pub(crate) dead_letter_policy: Option<DeadLetterPolicy>,
    last_message_received: Option<DateTime<Utc>>,
    messages_received: u64,
}

impl<T: DeserializeMessage, Exe: Executor> TopicConsumer<T, Exe> {
    async fn new(
        client: Pulsar<Exe>,
        topic: String,
        mut addr: BrokerAddress,
        config: ConsumerConfig,
    ) -> Result<TopicConsumer<T, Exe>, Error> {
        let ConsumerConfig {
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_redelivery_delay,
            options,
            dead_letter_policy,
        } = config.clone();
        let consumer_id = consumer_id.unwrap_or_else(rand::random);
        let (resolver, messages) = mpsc::unbounded();
        let batch_size = batch_size.unwrap_or(1000);

        let mut connection = client.manager.get_connection(&addr).await?;
        let mut current_retries = 0u32;
        let start = std::time::Instant::now();
        let operation_retry_options = client.operation_retry_options.clone();

        loop {
            match connection
                .sender()
                .subscribe(
                    resolver.clone(),
                    topic.clone(),
                    subscription.clone(),
                    sub_type,
                    consumer_id,
                    consumer_name.clone(),
                    options.clone(),
                )
                .await
            {
                Ok(_) => {
                    if current_retries > 0 {
                        let dur = (std::time::Instant::now() - start).as_secs();
                        log::info!(
                            "subscribe({}) success after {} retries over {} seconds",
                            topic,
                            current_retries + 1,
                            dur
                        );
                    }
                    break;
                }
                Err(ConnectionError::PulsarError(
                    Some(proto::ServerError::ServiceNotReady),
                    text,
                )) => {
                    if operation_retry_options.max_retries.is_none()
                        || operation_retry_options.max_retries.unwrap() > current_retries
                    {
                        error!("subscribe({}) answered ServiceNotReady, retrying request after {}ms (max_retries = {:?}): {}",
                        topic, operation_retry_options.retry_delay.as_millis(),
                        operation_retry_options.max_retries, text.unwrap_or_else(String::new));

                        current_retries += 1;
                        client
                            .executor
                            .delay(operation_retry_options.retry_delay)
                            .await;

                        // we need to look up again the topic's address
                        let prev = addr;
                        addr = client.lookup_topic(&topic).await?;
                        if prev != addr {
                            info!(
                                "topic {} moved: previous = {:?}, new = {:?}",
                                topic, prev, addr
                            );
                        }

                        connection = client.manager.get_connection(&addr).await?;
                        continue;
                    } else {
                        error!("subscribe({}) reached max retries", topic);

                        return Err(ConnectionError::PulsarError(
                            Some(proto::ServerError::ServiceNotReady),
                            text,
                        )
                        .into());
                    }
                }
                Err(e) => return Err(Error::Connection(e)),
            }
        }

        connection
            .sender()
            .send_flow(consumer_id, batch_size)
            .map_err(|e| {
                error!("TopicConsumer::new error[{}]: {:?}", line!(), e);
                e
            })
            .map_err(|e| Error::Consumer(ConsumerError::Connection(e)))?;

        let (engine_tx, engine_rx) = unbounded();
        // drop_signal will be dropped when Consumer is dropped, then
        // drop_receiver will return, and we can close the consumer
        let (_drop_signal, drop_receiver) = oneshot::channel::<()>();
        let conn = connection.clone();
        //let ack_sender = nack_handler.clone();
        let name = consumer_name.clone();
        let topic_name = topic.clone();
        let _ = client.executor.spawn(Box::pin(async move {
            let _res = drop_receiver.await;
            // if we receive a message, it indicates we want to stop this task
            if _res.is_err() {
                if let Err(e) = conn.sender().close_consumer(consumer_id).await {
                    error!(
                        "could not close consumer {:?}({}) for topic {}: {:?}",
                        consumer_name, consumer_id, topic_name, e
                    );
                }
            }
        }));

        if unacked_message_redelivery_delay.is_some() {
            let mut redelivery_tx = engine_tx.clone();
            let mut interval = client.executor.interval(Duration::from_millis(500));
            let res = client.executor.spawn(Box::pin(async move {
                while interval.next().await.is_some() {
                    if redelivery_tx
                        .send(EngineMessage::UnackedRedelivery)
                        .await
                        .is_err()
                    {
                        // Consumer shut down - stop ticker
                        break;
                    }
                }
            }));
            if res.is_err() {
                return Err(Error::Executor);
            }
        }
        let (tx, rx) = mpsc::channel(1000);
        let mut c = ConsumerEngine::new(
            client.clone(),
            connection.clone(),
            topic.clone(),
            subscription.clone(),
            sub_type,
            consumer_id,
            name,
            tx,
            messages,
            engine_rx,
            batch_size,
            unacked_message_redelivery_delay,
            dead_letter_policy.clone(),
            options.clone(),
            _drop_signal,
        );
        let f = async move {
            c.engine()
                .map(|res| {
                    debug!("consumer engine stopped: {:?}", res);
                })
                .await;
        };
        if client.executor.spawn(Box::pin(f)).is_err() {
            return Err(Error::Executor);
        }

        Ok(TopicConsumer {
            consumer_id,
            config,
            topic,
            messages: Box::pin(rx),
            engine_tx,
            data_type: PhantomData,
            dead_letter_policy,
            last_message_received: None,
            messages_received: 0,
        })
    }

    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    pub async fn connection(&mut self) -> Result<Arc<Connection<Exe>>, Error> {
        let (resolver, response) = oneshot::channel();
        self.engine_tx
            .send(EngineMessage::GetConnection(resolver))
            .await
            .map_err(|_| ConsumerError::Connection(ConnectionError::Disconnected))?;

        response.await.map_err(|oneshot::Canceled| {
            error!("the consumer engine dropped the request");
            ConnectionError::Disconnected.into()
        })
    }

    pub async fn check_connection(&mut self) -> Result<(), Error> {
        let conn = self.connection().await?;
        info!("check connection for id {}", conn.id());
        conn.sender().send_ping().await?;
        Ok(())
    }

    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Ack(msg.message_id.clone(), false))
            .await?;
        Ok(())
    }

    pub(crate) fn acker(&self) -> mpsc::UnboundedSender<EngineMessage<Exe>> {
        self.engine_tx.clone()
    }

    async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Ack(msg.message_id.clone(), true))
            .await?;
        Ok(())
    }

    async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Nack(msg.message_id.clone()))
            .await?;
        Ok(())
    }

    pub async fn seek(
        &mut self,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        self.connection()
            .await?
            .sender()
            .seek(consumer_id, message_id, timestamp)
            .await?;
        Ok(())
    }

    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        self.connection()
            .await?
            .sender()
            .unsubscribe(consumer_id)
            .await?;
        Ok(())
    }

    pub async fn get_last_message_id(&mut self) -> Result<CommandGetLastMessageIdResponse, Error> {
        let consumer_id = self.consumer_id;
        let conn = self.connection().await?;
        let get_last_message_id_response = conn.sender().get_last_message_id(consumer_id).await?;
        Ok(get_last_message_id_response)
    }

    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.last_message_received
    }

    pub fn messages_received(&self) -> u64 {
        self.messages_received
    }

    fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    fn create_message(&self, message_id: proto::MessageIdData, payload: Payload) -> Message<T> {
        Message {
            topic: self.topic.clone(),
            message_id: MessageData {
                id: message_id,
                batch_size: payload.metadata.num_messages_in_batch,
            },
            payload,
            _phantom: PhantomData,
        }
    }
}

impl<T: DeserializeMessage, Exe: Executor> Stream for TopicConsumer<T, Exe> {
    type Item = Result<Message<T>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.messages.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok((id, payload)))) => {
                self.last_message_received = Some(Utc::now());
                self.messages_received += 1;
                Poll::Ready(Some(Ok(self.create_message(id, payload))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
        }
    }
}

struct ConsumerEngine<Exe: Executor> {
    client: Pulsar<Exe>,
    connection: Arc<Connection<Exe>>,
    topic: String,
    subscription: String,
    sub_type: SubType,
    id: u64,
    name: Option<String>,
    tx: mpsc::Sender<Result<(proto::MessageIdData, Payload), Error>>,
    messages_rx: Option<mpsc::UnboundedReceiver<RawMessage>>,
    engine_rx: Option<mpsc::UnboundedReceiver<EngineMessage<Exe>>>,
    batch_size: u32,
    remaining_messages: u32,
    unacked_message_redelivery_delay: Option<Duration>,
    unacked_messages: HashMap<MessageIdData, Instant>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    options: ConsumerOptions,
    _drop_signal: oneshot::Sender<()>,
}

pub(crate) enum EngineMessage<Exe: Executor> {
    Ack(MessageData, bool),
    Nack(MessageData),
    UnackedRedelivery,
    GetConnection(oneshot::Sender<Arc<Connection<Exe>>>),
}

impl<Exe: Executor> ConsumerEngine<Exe> {
    fn new(
        client: Pulsar<Exe>,
        connection: Arc<Connection<Exe>>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        id: u64,
        name: Option<String>,
        tx: mpsc::Sender<Result<(proto::MessageIdData, Payload), Error>>,
        messages_rx: mpsc::UnboundedReceiver<RawMessage>,
        engine_rx: mpsc::UnboundedReceiver<EngineMessage<Exe>>,
        batch_size: u32,
        unacked_message_redelivery_delay: Option<Duration>,
        dead_letter_policy: Option<DeadLetterPolicy>,
        options: ConsumerOptions,
        _drop_signal: oneshot::Sender<()>,
    ) -> ConsumerEngine<Exe> {
        ConsumerEngine {
            client,
            connection,
            topic,
            subscription,
            sub_type,
            id,
            name,
            tx,
            messages_rx: Some(messages_rx),
            engine_rx: Some(engine_rx),
            batch_size,
            remaining_messages: batch_size,
            unacked_message_redelivery_delay,
            unacked_messages: HashMap::new(),
            dead_letter_policy,
            options,
            _drop_signal,
        }
    }

    async fn engine(&mut self) -> Result<(), Error> {
        debug!("starting the consumer engine for topic {}", self.topic);
        let mut messages_or_ack_f = None;
        loop {
            if !self.connection.is_valid() {
                if let Some(err) = self.connection.error() {
                    error!(
                        "Consumer: connection {} is not valid: {:?}",
                        self.connection.id(),
                        err
                    );
                    self.reconnect().await?;
                }
            }

            if self.remaining_messages < self.batch_size / 2 {
                match self
                    .connection
                    .sender()
                    .send_flow(self.id, self.batch_size - self.remaining_messages)
                {
                    Ok(()) => {}
                    Err(ConnectionError::Disconnected) => {
                        self.reconnect().await?;
                        self.connection
                            .sender()
                            .send_flow(self.id, self.batch_size - self.remaining_messages)?;
                    }
                    Err(e) => return Err(e.into()),
                }
                self.remaining_messages = self.batch_size;
            }

            let mut f = match messages_or_ack_f.take() {
                None => {
                    // we need these complicated steps to select on two streams of different types,
                    // while being able to store it in the ConsumerEngine object (biggest issue),
                    // and replacing messages_rx when we reconnect, and considering that engine_rx is
                    // not clonable.
                    // Please, someone find a better solution
                    let messages_f = self.messages_rx.take().unwrap().into_future();
                    let ack_f = self.engine_rx.take().unwrap().into_future();
                    select(messages_f, ack_f)
                }
                Some(f) => f,
            };

            // we want to wake up regularly to check if the connection is still valid:
            // if the heartbeat failed, the connection.is_valid() call at the beginning
            // of the loop should fail, but to get there we must stop waiting on
            // messages_f and ack_f
            let delay_f = self.client.executor.delay(Duration::from_secs(1));
            let f_pin = std::pin::Pin::new(&mut f);
            pin_mut!(delay_f);

            let f = match select(f_pin, delay_f).await {
                Either::Left((res, _)) => res,
                Either::Right((_, _f)) => {
                    messages_or_ack_f = Some(f);
                    continue;
                }
            };

            match f {
                Either::Left(((message_opt, messages_rx), engine_rx)) => {
                    self.messages_rx = Some(messages_rx);
                    self.engine_rx = engine_rx.into_inner();
                    match message_opt {
                        None => {
                            error!("Consumer: messages::next: returning Disconnected");
                            self.reconnect().await?;
                            continue;
                            //return Err(Error::Consumer(ConsumerError::Connection(ConnectionError::Disconnected)).into());
                        }
                        Some(message) => {
                            self.remaining_messages -= message
                                .payload
                                .as_ref()
                                .and_then(|payload| payload.metadata.num_messages_in_batch)
                                .unwrap_or(1i32)
                                as u32;

                            match self.process_message(message).await {
                                // Continue
                                Ok(true) => {}
                                // End of Topic
                                Ok(false) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    if let Err(e) = self.tx.send(Err(e)).await {
                                        error!("cannot send a message from the consumer engine to the consumer({}), stopping the engine", self.id);
                                        return Err(Error::Consumer(e.into()));
                                    }
                                }
                            }
                        }
                    }
                }
                Either::Right(((ack_opt, engine_rx), messages_rx)) => {
                    self.messages_rx = messages_rx.into_inner();
                    self.engine_rx = Some(engine_rx);

                    match ack_opt {
                        None => {
                            trace!("ack channel was closed");
                            return Ok(());
                        }
                        Some(EngineMessage::Ack(message_id, cumulative)) => {
                            self.ack(message_id, cumulative);
                        }
                        Some(EngineMessage::Nack(message_id)) => {
                            if let Err(e) = self
                                .connection
                                .sender()
                                .send_redeliver_unacknowleged_messages(
                                    self.id,
                                    vec![message_id.id.clone()],
                                )
                            {
                                error!(
                                    "could not ask for redelivery for message {:?}: {:?}",
                                    message_id, e
                                );
                            }
                        }
                        Some(EngineMessage::UnackedRedelivery) => {
                            let mut h = HashSet::new();
                            let now = Instant::now();
                            //info!("unacked messages length: {}", self.unacked_messages.len());
                            for (id, t) in self.unacked_messages.iter() {
                                if *t < now {
                                    h.insert(id.clone());
                                }
                            }

                            let ids: Vec<_> = h.iter().cloned().collect();
                            if !ids.is_empty() {
                                //info!("will unack ids: {:?}", ids);
                                if let Err(e) = self
                                    .connection
                                    .sender()
                                    .send_redeliver_unacknowleged_messages(self.id, ids)
                                {
                                    error!("could not ask for redelivery: {:?}", e);
                                } else {
                                    for i in h.iter() {
                                        self.unacked_messages.remove(i);
                                    }
                                }
                            }
                        }
                        Some(EngineMessage::GetConnection(sender)) => {
                            let _ = sender.send(self.connection.clone()).map_err(|_| {
                                error!("consumer requested the engine's connection but dropped the channel before receiving");
                            });
                        }
                    }
                }
            };
        }
    }

    fn ack(&mut self, message_id: MessageData, cumulative: bool) {
        //FIXME: this does not handle cumulative acks
        self.unacked_messages.remove(&message_id.id);
        let res = self
            .connection
            .sender()
            .send_ack(self.id, vec![message_id.id], cumulative);
        if res.is_err() {
            error!("ack error: {:?}", res);
        }
    }

    /// Process the message. Returns `true` if there are more messages to process
    async fn process_message(&mut self, message: RawMessage) -> Result<bool, Error> {
        match message {
            RawMessage {
                command:
                    BaseCommand {
                        reached_end_of_topic: Some(_),
                        ..
                    },
                ..
            } => {
                return Ok(false);
            }
            RawMessage {
                command:
                    BaseCommand {
                        active_consumer_change: Some(active_consumer_change),
                        ..
                    },
                ..
            } => {
                // TODO: Communicate this status to the Consumer and expose it
                debug!(
                    "Active consumer change for {} - Active: {:?}",
                    self.debug_format(),
                    active_consumer_change.is_active
                );
            }
            RawMessage {
                command:
                    BaseCommand {
                        message: Some(message),
                        ..
                    },
                payload: Some(payload),
            } => {
                self.process_payload(message, payload).await?;
            }
            RawMessage {
                command: BaseCommand {
                    message: Some(_), ..
                },
                payload: None,
            } => {
                error!(
                    "Consumer {} received message without payload",
                    self.debug_format()
                );
            }
            RawMessage {
                command:
                    BaseCommand {
                        close_consumer: Some(CommandCloseConsumer { consumer_id, .. }),
                        ..
                    },
                ..
            } => {
                error!(
                    "Broker notification of closed consumer {}: {}",
                    consumer_id,
                    self.debug_format()
                );

                self.reconnect().await?;
            }
            unexpected => {
                let r#type = proto::base_command::Type::try_from(unexpected.command.r#type)
                    .map(|t| format!("{:?}", t))
                    .unwrap_or_else(|_| unexpected.command.r#type.to_string());
                warn!(
                    "Unexpected message type sent to consumer: {}. This is probably a bug!",
                    r#type
                );
            }
        }
        Ok(true)
    }

    async fn process_payload(
        &mut self,
        message: CommandMessage,
        mut payload: Payload,
    ) -> Result<(), Error> {
        let compression = payload.metadata.compression;

        let payload = match compression {
            None | Some(0) => payload,
            // LZ4
            Some(1) => {
                #[cfg(not(feature = "lz4"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "got a LZ4 compressed message but 'lz4' cargo feature is deactivated",
                    )))
                    .into());
                }

                #[cfg(feature = "lz4")]
                {
                    let decompressed_payload = lz4::block::decompress(
                        &payload.data[..],
                        payload.metadata.uncompressed_size.map(|i| i as i32),
                    )
                    .map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            // zlib
            Some(2) => {
                #[cfg(not(feature = "flate2"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "got a zlib compressed message but 'flate2' cargo feature is deactivated",
                    )))
                    .into());
                }

                #[cfg(feature = "flate2")]
                {
                    use flate2::read::ZlibDecoder;
                    use std::io::Read;

                    let mut d = ZlibDecoder::new(&payload.data[..]);
                    let mut decompressed_payload = Vec::new();
                    d.read_to_end(&mut decompressed_payload)
                        .map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            // zstd
            Some(3) => {
                #[cfg(not(feature = "zstd"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "got a zstd compressed message but 'zstd' cargo feature is deactivated",
                    )))
                    .into());
                }

                #[cfg(feature = "zstd")]
                {
                    let decompressed_payload =
                        zstd::decode_all(&payload.data[..]).map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            //Snappy
            Some(4) => {
                #[cfg(not(feature = "snap"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "got a Snappy compressed message but 'snap' cargo feature is deactivated",
                    )))
                    .into());
                }

                #[cfg(feature = "snap")]
                {
                    use std::io::Read;

                    let mut decompressed_payload = Vec::new();
                    let mut decoder = snap::read::FrameDecoder::new(&payload.data[..]);
                    decoder
                        .read_to_end(&mut decompressed_payload)
                        .map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            Some(i) => {
                error!("unknown compression type: {}", i);
                return Err(Error::Consumer(ConsumerError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("unknown compression type: {}", i),
                ))));
            }
        };

        match payload.metadata.num_messages_in_batch {
            Some(_) => {
                let it = BatchedMessageIterator::new(message.message_id, payload)?;
                for (id, payload) in it {
                    // TODO: Dead letter policy for batched messages
                    self.send_to_consumer(id, payload).await?;
                }
            }
            None => match (message.redelivery_count, self.dead_letter_policy.as_ref()) {
                (Some(redelivery_count), Some(dead_letter_policy)) => {
                    // Send message to Dead Letter Topic and ack message in original topic
                    if redelivery_count as usize >= dead_letter_policy.max_redeliver_count {
                        self.client
                            .send(&dead_letter_policy.dead_letter_topic, payload.data)
                            .await?
                            .await
                            .map_err(|e| {
                                error!("One shot cancelled {:?}", e);
                                Error::Custom("DLQ send error".to_string())
                            })?;

                        self.ack(
                            MessageData {
                                id: message.message_id,
                                batch_size: None,
                            },
                            false,
                        );
                    } else {
                        self.send_to_consumer(message.message_id, payload).await?
                    }
                }
                _ => self.send_to_consumer(message.message_id, payload).await?,
            },
        }
        Ok(())
    }

    async fn send_to_consumer(
        &mut self,
        message_id: MessageIdData,
        payload: Payload,
    ) -> Result<(), Error> {
        let now = Instant::now();
        self.tx
            .send(Ok((message_id.clone(), payload)))
            .await
            .map_err(|e| {
                error!("tx returned {:?}", e);
                Error::Custom("tx closed".to_string())
            })?;
        if let Some(duration) = self.unacked_message_redelivery_delay {
            self.unacked_messages.insert(message_id, now + duration);
        }
        Ok(())
    }

    async fn reconnect(&mut self) -> Result<(), Error> {
        debug!("reconnecting consumer for topic: {}", self.topic);
        let broker_address = self.client.lookup_topic(&self.topic).await?;
        let conn = self.client.manager.get_connection(&broker_address).await?;

        self.connection = conn;

        let topic = self.topic.clone();
        let (resolver, messages) = mpsc::unbounded();

        self.connection
            .sender()
            .subscribe(
                resolver,
                topic.clone(),
                self.subscription.clone(),
                self.sub_type,
                self.id,
                self.name.clone(),
                self.options.clone(),
            )
            .await
            .map_err(Error::Connection)?;

        self.connection
            .sender()
            .send_flow(self.id, self.batch_size)
            .map_err(|e| Error::Consumer(ConsumerError::Connection(e)))?;

        self.messages_rx = Some(messages);

        // drop_signal will be dropped when Consumer is dropped, then
        // drop_receiver will return, and we can close the consumer
        let (_drop_signal, drop_receiver) = oneshot::channel::<()>();
        let conn = self.connection.clone();
        let name = self.name.clone();
        let id = self.id;
        let topic = self.topic.clone();
        let _ = self.client.executor.spawn(Box::pin(async move {
            let _res = drop_receiver.await;
            // if we receive a message, it indicates we want to stop this task
            if _res.is_err() {
                if let Err(e) = conn.sender().close_consumer(id).await {
                    error!("could not close consumer {:?}({}) for topic {}: {:?}", name, id, topic, e);
                }
            }
        }));
        let old_signal = std::mem::replace(&mut self._drop_signal, _drop_signal);
        if let Err(e) = old_signal.send(()) {
            error!(
                "could not send the drop signal to the old consumer(id={}): {:?}",
                id, e
            );
        }

        Ok(())
    }

    fn debug_format(&self) -> String {
        format!(
            "[{id} - {subscription}{name}: {topic}]",
            id = self.id,
            subscription = &self.subscription,
            name = self
                .name
                .as_ref()
                .map(|s| format!("({})", s))
                .unwrap_or_default(),
            topic = &self.topic
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MessageData {
    pub id: proto::MessageIdData,
    batch_size: Option<i32>,
}

struct BatchedMessageIterator {
    messages: std::vec::IntoIter<BatchedMessage>,
    message_id: proto::MessageIdData,
    metadata: Metadata,
    total_messages: u32,
    current_index: u32,
}

impl BatchedMessageIterator {
    fn new(message_id: proto::MessageIdData, payload: Payload) -> Result<Self, ConnectionError> {
        let total_messages = payload
            .metadata
            .num_messages_in_batch
            .expect("expected batched message") as u32;
        let messages = parse_batched_message(total_messages, &payload.data)?;

        Ok(Self {
            messages: messages.into_iter(),
            message_id,
            total_messages,
            metadata: payload.metadata,
            current_index: 0,
        })
    }
}

impl Iterator for BatchedMessageIterator {
    type Item = (proto::MessageIdData, Payload);

    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.total_messages - self.current_index;
        if remaining == 0 {
            return None;
        }
        let index = self.current_index;
        self.current_index += 1;
        if let Some(batched_message) = self.messages.next() {
            let id = proto::MessageIdData {
                batch_index: Some(index as i32),
                ..self.message_id.clone()
            };

            let metadata = Metadata {
                properties: batched_message.metadata.properties,
                partition_key: batched_message.metadata.partition_key,
                event_time: batched_message.metadata.event_time,
                ..self.metadata.clone()
            };

            let payload = Payload {
                metadata,
                data: batched_message.payload,
            };

            Some((id, payload))
        } else {
            None
        }
    }
}

/// Builder structure for consumers
///
/// This is the main way to create a [Consumer] or a [Reader]
#[derive(Clone)]
pub struct ConsumerBuilder<Exe: Executor> {
    pulsar: Pulsar<Exe>,
    topics: Option<Vec<String>>,
    topic_regex: Option<Regex>,
    subscription: Option<String>,
    subscription_type: Option<SubType>,
    consumer_id: Option<u64>,
    consumer_name: Option<String>,
    batch_size: Option<u32>,
    unacked_message_resend_delay: Option<Duration>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    consumer_options: Option<ConsumerOptions>,
    namespace: Option<String>,
    topic_refresh: Option<Duration>,
}

impl<Exe: Executor> ConsumerBuilder<Exe> {
    /// Creates a new [ConsumerBuilder] from an existing client instance
    pub fn new(pulsar: &Pulsar<Exe>) -> Self {
        ConsumerBuilder {
            pulsar: pulsar.clone(),
            topics: None,
            topic_regex: None,
            subscription: None,
            subscription_type: None,
            consumer_id: None,
            consumer_name: None,
            batch_size: None,
            //TODO what should this default to? None seems incorrect..
            unacked_message_resend_delay: None,
            dead_letter_policy: None,
            consumer_options: None,
            namespace: None,
            topic_refresh: None,
        }
    }

    /// sets the consumer's topic or add one to the list of topics
    pub fn with_topic<S: Into<String>>(mut self, topic: S) -> ConsumerBuilder<Exe> {
        match &mut self.topics {
            Some(topics) => topics.push(topic.into()),
            None => self.topics = Some(vec![topic.into()]),
        }
        self
    }

    /// adds a list of topics to the future consumer
    pub fn with_topics<S: AsRef<str>, I: IntoIterator<Item = S>>(
        mut self,
        topics: I,
    ) -> ConsumerBuilder<Exe> {
        let new_topics = topics.into_iter().map(|t| t.as_ref().into());
        match &mut self.topics {
            Some(topics) => {
                topics.extend(new_topics);
            }
            None => self.topics = Some(new_topics.collect()),
        }
        self
    }

    /// sets up a consumer that will listen on all topics matching the regular
    /// expression
    pub fn with_topic_regex(mut self, regex: Regex) -> ConsumerBuilder<Exe> {
        self.topic_regex = Some(regex);
        self
    }

    /// sets the subscription's name
    pub fn with_subscription<S: Into<String>>(mut self, subscription: S) -> Self {
        self.subscription = Some(subscription.into());
        self
    }

    /// sets the kind of subscription
    pub fn with_subscription_type(mut self, subscription_type: SubType) -> Self {
        self.subscription_type = Some(subscription_type);
        self
    }

    /// Tenant/Namespace to be used when matching against a regex. For other consumers,
    /// specify namespace using the `<persistent|non-persistent://<tenant>/<namespace>/<topic>`
    /// topic format.
    /// Defaults to `public/default` if not specifid
    pub fn with_lookup_namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Interval for refreshing the topics when using a topic regex. Unused otherwise.
    pub fn with_topic_refresh(mut self, refresh_interval: Duration) -> Self {
        self.topic_refresh = Some(refresh_interval);
        self
    }

    /// sets the consumer id for this consumer
    pub fn with_consumer_id(mut self, consumer_id: u64) -> Self {
        self.consumer_id = Some(consumer_id);
        self
    }

    /// sets the consumer's name
    pub fn with_consumer_name<S: Into<String>>(mut self, consumer_name: S) -> Self {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    /// sets the batch size
    ///
    /// batch messages containing more than the configured batch size will
    /// not be sent by Pulsar
    ///
    /// default value: 1000
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// sets consumer options
    pub fn with_options(mut self, options: ConsumerOptions) -> Self {
        self.consumer_options = Some(options);
        self
    }

    /// sets the dead letter policy
    pub fn with_dead_letter_policy(mut self, dead_letter_policy: DeadLetterPolicy) -> Self {
        self.dead_letter_policy = Some(dead_letter_policy);
        self
    }

    /// The time after which a message is dropped without being acknowledged or nacked
    /// that the message is resent. If `None`, messages will only be resent when a
    /// consumer disconnects with pending unacknowledged messages.
    pub fn with_unacked_message_resend_delay(mut self, delay: Option<Duration>) -> Self {
        self.unacked_message_resend_delay = delay;
        self
    }

    // Checks the builder for inconsistencies
    // returns a config and a list of topics with associated brokers
    async fn validate<T: DeserializeMessage>(
        self,
    ) -> Result<(ConsumerConfig, Vec<(String, BrokerAddress)>), Error> {
        let ConsumerBuilder {
            pulsar,
            topics,
            topic_regex,
            subscription,
            subscription_type,
            consumer_id,
            mut consumer_name,
            batch_size,
            unacked_message_resend_delay,
            consumer_options,
            dead_letter_policy,
            namespace: _,
            topic_refresh: _,
        } = self;

        if consumer_name.is_none() {
            let s: String = (0..8)
                .map(|_| rand::thread_rng().sample(Alphanumeric))
                .map(|c| c as char)
                .collect();
            consumer_name = Some(format!("consumer_{}", s));
        }

        if topics.is_none() && topic_regex.is_none() {
            return Err(Error::Custom(
                "Cannot create consumer with no topics and no topic regex".into(),
            ));
        }

        let topics: Vec<(String, BrokerAddress)> = try_join_all(
            topics
                .into_iter()
                .flatten()
                .map(|topic| pulsar.lookup_partitioned_topic(topic)),
        )
        .await?
        .into_iter()
        .flatten()
        .collect();

        if topics.is_empty() && topic_regex.is_none() {
            return Err(Error::Custom(
                "Unable to create consumer - topic not found".to_string(),
            ));
        }

        let consumer_id = match (consumer_id, topics.len()) {
            (Some(consumer_id), 1) => Some(consumer_id),
            (Some(_), _) => {
                warn!("Cannot specify consumer id for connecting to partitioned topics or multiple topics");
                None
            }
            _ => None,
        };
        let subscription = subscription.unwrap_or_else(|| {
            let s: String = (0..8)
                .map(|_| rand::thread_rng().sample(Alphanumeric))
                .map(|c| c as char)
                .collect();
            let subscription = format!("sub_{}", s);
            warn!(
                "Subscription not specified. Using new subscription `{}`.",
                subscription
            );
            subscription
        });
        let sub_type = subscription_type.unwrap_or_else(|| {
            warn!("Subscription Type not specified. Defaulting to `Shared`.");
            SubType::Shared
        });

        let config = ConsumerConfig {
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_redelivery_delay: unacked_message_resend_delay,
            options: consumer_options.unwrap_or_default(),
            dead_letter_policy,
        };
        Ok((config, topics))
    }

    /// creates a [Consumer] from this builder
    pub async fn build<T: DeserializeMessage>(self) -> Result<Consumer<T, Exe>, Error> {
        // would this clone() consume too much memory?
        let (config, joined_topics) = self.clone().validate::<T>().await?;

        let consumers = try_join_all(joined_topics.into_iter().map(|(topic, addr)| {
            TopicConsumer::new(self.pulsar.clone(), topic, addr, config.clone())
        }))
        .await?;

        let consumer = if consumers.len() == 1 {
            let consumer = consumers.into_iter().next().unwrap();
            InnerConsumer::Single(consumer)
        } else {
            let consumers: BTreeMap<_, _> = consumers
                .into_iter()
                .map(|c| (c.topic(), Box::pin(c)))
                .collect();
            let topics = consumers.keys().cloned().collect();
            let topic_refresh = self
                .topic_refresh
                .unwrap_or_else(|| Duration::from_secs(30));
            let refresh = Box::pin(self.pulsar.executor.interval(topic_refresh).map(drop));
            let mut consumer = MultiTopicConsumer {
                namespace: self
                    .namespace
                    .unwrap_or_else(|| "public/default".to_string()),
                topic_regex: self.topic_regex,
                pulsar: self.pulsar,
                consumers,
                topics,
                new_consumers: None,
                refresh,
                config,
                disc_last_message_received: None,
                disc_messages_received: 0,
            };
            if consumer.topic_regex.is_some() {
                consumer.update_topics();
                let initial_consumers = consumer.new_consumers.take().unwrap().await?;
                consumer.add_consumers(initial_consumers);
            }
            InnerConsumer::Multi(consumer)
        };
        Ok(Consumer { inner: consumer })
    }

    /// creates a [Reader] from this builder
    pub async fn into_reader<T: DeserializeMessage>(self) -> Result<Reader<T, Exe>, Error> {
        // would this clone() consume too much memory?
        let (mut config, mut joined_topics) = self.clone().validate::<T>().await?;

        // the validate() function defaults sub_type to SubType::Shared,
        // but a reader's subscription is exclusive
        warn!("Subscription Type for a reader is `Exclusive`. Resetting.");
        config.sub_type = SubType::Exclusive;

        if self.topics.unwrap().len() > 1 {
            return Err(Error::Custom(
                "Unable to create a reader - one topic max".to_string(),
            ));
        }

        let (topic, addr) = joined_topics.pop().unwrap();
        let consumer = TopicConsumer::new(self.pulsar.clone(), topic, addr, config.clone()).await?;

        Ok(Reader {
            consumer,
            state: Some(State::PollingConsumer),
        })
    }
}

/// the complete configuration of a consumer
#[derive(Debug, Clone, Default)]
pub(crate) struct ConsumerConfig {
    /// subscription name
    pub(crate) subscription: String,
    /// subscription type
    ///
    /// default: Shared
    pub(crate) sub_type: SubType,
    /// maximum size for batched messages
    ///
    /// default: 1000
    pub(crate) batch_size: Option<u32>,
    /// name of the consumer
    pub(crate) consumer_name: Option<String>,
    /// numerical id of the consumer
    consumer_id: Option<u64>,
    /// time after which unacked messages will be sent again
    unacked_message_redelivery_delay: Option<Duration>,
    /// consumer options
    pub(crate) options: ConsumerOptions,
    /// dead letter policy
    dead_letter_policy: Option<DeadLetterPolicy>,
}

/// A consumer that can subscribe on multiple topics, from a regex matching topic names
struct MultiTopicConsumer<T: DeserializeMessage, Exe: Executor> {
    namespace: String,
    topic_regex: Option<Regex>,
    pulsar: Pulsar<Exe>,
    consumers: BTreeMap<String, Pin<Box<TopicConsumer<T, Exe>>>>,
    topics: VecDeque<String>,
    #[allow(clippy::type_complexity)]
    new_consumers:
        Option<Pin<Box<dyn Future<Output = Result<Vec<TopicConsumer<T, Exe>>, Error>> + Send>>>,
    refresh: Pin<Box<dyn Stream<Item = ()> + Send>>,
    config: ConsumerConfig,
    // Stats on disconnected consumers to keep metrics correct
    disc_messages_received: u64,
    disc_last_message_received: Option<DateTime<Utc>>,
}

impl<T: DeserializeMessage, Exe: Executor> MultiTopicConsumer<T, Exe> {
    fn topics(&self) -> Vec<String> {
        self.topics.iter().map(|s| s.to_string()).collect()
    }

    fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.consumers
            .values()
            .filter_map(|c| c.last_message_received)
            .chain(self.disc_last_message_received)
            .max()
    }

    fn messages_received(&self) -> u64 {
        self.consumers
            .values()
            .map(|c| c.messages_received)
            .chain(iter::once(self.disc_messages_received))
            .sum()
    }

    async fn check_connections(&mut self) -> Result<(), Error> {
        self.pulsar
            .manager
            .get_base_connection()
            .await?
            .sender()
            .send_ping()
            .await?;

        for consumer in self.consumers.values_mut() {
            consumer.connection().await?.sender().send_ping().await?;
        }

        Ok(())
    }

    async fn get_last_message_id(&mut self) -> Result<Vec<CommandGetLastMessageIdResponse>, Error> {
        let responses = try_join_all(self.consumers.values_mut().map(|c| c.get_last_message_id())).await?;
        Ok(responses)
    }

    async fn unsubscribe(&mut self) -> Result<(), Error> {
        for consumer in self.consumers.values_mut() {
            consumer.unsubscribe().await?;
        }

        Ok(())
    }

    fn add_consumers<I: IntoIterator<Item = TopicConsumer<T, Exe>>>(&mut self, consumers: I) {
        for consumer in consumers {
            let topic = consumer.topic().to_owned();
            self.consumers.insert(topic.clone(), Box::pin(consumer));
            self.topics.push_back(topic);
        }
    }

    fn remove_consumers(&mut self, topics: &[String]) {
        self.topics.retain(|t| !topics.contains(t));
        for topic in topics {
            if let Some(consumer) = self.consumers.remove(topic) {
                self.disc_messages_received += consumer.messages_received;
                self.disc_last_message_received = self
                    .disc_last_message_received
                    .into_iter()
                    .chain(consumer.last_message_received)
                    .max();
            }
        }
    }

    fn update_topics(&mut self) {
        if let Some(regex) = self.topic_regex.clone() {
            let pulsar = self.pulsar.clone();
            let namespace = self.namespace.clone();
            let existing_topics: BTreeSet<String> = self.consumers.keys().cloned().collect();
            let consumer_config = self.config.clone();

            self.new_consumers = Some(Box::pin(async move {
                let topics = pulsar
                    .get_topics_of_namespace(
                        namespace.clone(),
                        proto::command_get_topics_of_namespace::Mode::All,
                    )
                    .await?;
                trace!("fetched topics {:?}", topics);

                let topics: Vec<_> = try_join_all(
                    topics
                        .into_iter()
                        .filter(|t| regex.is_match(t))
                        .map(|topic| pulsar.lookup_partitioned_topic(topic)),
                )
                .await?
                .into_iter()
                .flatten()
                .collect();

                trace!("matched topics {:?} (regex: {})", topics, &regex);

                let consumers = try_join_all(
                    topics
                        .into_iter()
                        .filter(|(t, _)| !existing_topics.contains(t))
                        .map(|(topic, addr)| {
                            TopicConsumer::new(pulsar.clone(), topic, addr, consumer_config.clone())
                        }),
                )
                .await?;
                trace!("created {} consumers", consumers.len());
                Ok(consumers)
            }));
        }
    }

    async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.ack(msg).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.cumulative_ack(msg).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.nack(msg).await?;
            Ok(())
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    /// Assume that this seek method will call seek for the topics given in the consumer_ids
    async fn seek(
        &mut self,
        consumer_ids: Option<Vec<String>>,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        // 0. null or empty vector
        match consumer_ids {
            Some(consumer_ids) => {
                // 1, select consumers
                let mut actions = Vec::default();
                for (consumer_id, consumer) in self.consumers.iter_mut() {
                    if consumer_ids.contains(consumer_id) {
                        actions.push(consumer.seek(message_id.clone(), timestamp));
                    }
                }
                // 2 join all the futures
                let mut v = futures::future::join_all(actions).await;

                for res in v.drain(..) {
                    if res.is_err() {
                        return res;
                    }
                }

                Ok(())
            }
            None => Err(ConnectionError::Unexpected(format!(
                "no consumer for consumer ids {:?}",
                consumer_ids
            ))
            .into()),
        }
    }

    fn config(&self) -> &ConsumerConfig {
        &self.config
    }
}

/// a message received by a consumer
///
/// it is generic over the type it can be deserialized to
pub struct Message<T> {
    /// origin topic of the message
    pub topic: String,
    /// contains the message's data and other metadata
    pub payload: Payload,
    /// contains the message's id and batch size data
    pub message_id: MessageData,
    _phantom: PhantomData<T>,
}

impl<T> Message<T> {
    /// Pulsar metadata for the message
    pub fn metadata(&self) -> &MessageMetadata {
        &self.payload.metadata
    }

    /// Get Pulsar message id for the message
    pub fn message_id(&self) -> &proto::MessageIdData {
        &self.message_id.id
    }

    /// Get message key (partition key)
    pub fn key(&self) -> Option<String> {
        self.payload.metadata.partition_key.clone()
    }
}
impl<T: DeserializeMessage> Message<T> {
    /// directly deserialize a message
    pub fn deserialize(&self) -> T::Output {
        T::deserialize_message(&self.payload)
    }
}

impl<T: DeserializeMessage, Exe: Executor> Debug for MultiTopicConsumer<T, Exe> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MultiTopicConsumer({:?}, {:?})",
            &self.namespace, &self.topic_regex
        )
    }
}

impl<T: 'static + DeserializeMessage, Exe: Executor> Stream for MultiTopicConsumer<T, Exe> {
    type Item = Result<Message<T>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(mut new_consumers) = self.new_consumers.take() {
            match new_consumers.as_mut().poll(cx) {
                Poll::Ready(Ok(new_consumers)) => {
                    self.add_consumers(new_consumers);
                }
                Poll::Pending => {
                    self.new_consumers = Some(new_consumers);
                }
                Poll::Ready(Err(e)) => {
                    error!("Error creating pulsar consumers: {}", e);
                    // don't return error here; could be intermittent connection failure and we want
                    // to retry
                }
            }
        }

        if let Poll::Ready(Some(_)) = self.refresh.as_mut().poll_next(cx) {
            self.update_topics();
            return self.poll_next(cx);
        }

        let mut topics_to_remove = Vec::new();
        let mut result = None;
        for _ in 0..self.topics.len() {
            if result.is_some() {
                break;
            }
            let topic = self.topics.pop_front().unwrap();
            if let Some(item) = self
                .consumers
                .get_mut(&topic)
                .map(|c| c.as_mut().poll_next(cx))
            {
                match item {
                    Poll::Pending => {}
                    Poll::Ready(Some(Ok(msg))) => result = Some(msg),
                    Poll::Ready(None) => {
                        error!("Unexpected end of stream for pulsar topic {}", &topic);
                        topics_to_remove.push(topic.clone());
                    }
                    Poll::Ready(Some(Err(e))) => {
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
            return Poll::Ready(Some(Ok(result)));
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use futures::{StreamExt, TryStreamExt};
    use log::LevelFilter;
    use regex::Regex;
    #[cfg(feature = "tokio-runtime")]
    use tokio::time::timeout;

    #[cfg(feature = "tokio-runtime")]
    use crate::executor::TokioExecutor;
    use crate::{producer, tests::TEST_LOGGER, Pulsar, SerializeMessage};

    use super::*;
    use futures::future::{select, Either};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
    pub struct TestData {
        topic: String,
        msg: u32,
    }

    impl<'a> SerializeMessage for &'a TestData {
        fn serialize_message(input: Self) -> Result<producer::Message, Error> {
            let payload = serde_json::to_vec(&input).map_err(|e| Error::Custom(e.to_string()))?;
            Ok(producer::Message {
                payload,
                ..Default::default()
            })
        }
    }

    impl DeserializeMessage for TestData {
        type Output = Result<TestData, serde_json::Error>;

        fn deserialize_message(payload: &Payload) -> Self::Output {
            serde_json::from_slice(&payload.data)
        }
    }

    pub static MULTI_LOGGER: crate::tests::SimpleLogger = crate::tests::SimpleLogger {
        tag: "multi_consumer",
    };
    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn multi_consumer() {
        let _ = log::set_logger(&MULTI_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";

        let topic_n: u16 = rand::random();
        let topic1 = format!("multi_consumer_a_{}", topic_n);
        let topic2 = format!("multi_consumer_b_{}", topic_n);

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
            msg: 3,
        };
        let data4 = TestData {
            topic: "b".to_owned(),
            msg: 4,
        };

        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        try_join_all(vec![
            client.send(&topic1, &data1),
            client.send(&topic1, &data2),
            client.send(&topic2, &data3),
            client.send(&topic2, &data4),
        ])
        .await
        .unwrap();

        let builder = client
            .consumer()
            .with_subscription_type(SubType::Shared)
            // get earliest messages
            .with_options(ConsumerOptions {
                initial_position: InitialPosition::Earliest,
                ..Default::default()
            });

        let consumer_1: Consumer<TestData, _> = builder
            .clone()
            .with_subscription("consumer_1")
            .with_topics(&[&topic1, &topic2])
            .build()
            .await
            .unwrap();

        let consumer_2: Consumer<TestData, _> = builder
            .with_subscription("consumer_2")
            .with_topic_regex(Regex::new(&format!("multi_consumer_[ab]_{}", topic_n)).unwrap())
            .build()
            .await
            .unwrap();

        let expected: HashSet<_> = vec![data1, data2, data3, data4].into_iter().collect();
        for consumer in [consumer_1, consumer_2].iter_mut() {
            let connected_topics = consumer.topics();
            debug!(
                "connected topics for {}: {:?}",
                consumer.subscription(),
                &connected_topics
            );
            assert_eq!(connected_topics.len(), 2);
            assert!(connected_topics.iter().any(|t| t.ends_with(&topic1)));
            assert!(connected_topics.iter().any(|t| t.ends_with(&topic2)));

            let mut received = HashSet::new();
            while let Some(message) = timeout(Duration::from_secs(1), consumer.next())
                .await
                .unwrap()
            {
                received.insert(message.unwrap().deserialize().unwrap());
                if received.len() == 4 {
                    break;
                }
            }
            assert_eq!(expected, received);
            assert_eq!(consumer.messages_received(), 4);
            assert!(consumer.last_message_received().is_some());
        }
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn consumer_dropped_with_lingering_acks() {
        use rand::{distributions::Alphanumeric, Rng};
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";

        let topic = format!(
            "consumer_dropped_with_lingering_acks_{}",
            rand::random::<u16>()
        );

        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        let message = TestData {
            topic: std::iter::repeat(())
                .map(|()| rand::thread_rng().sample(Alphanumeric) as char)
                .take(8)
                .map(|c| c as char)
                .collect(),
            msg: 1,
        };

        client.send(&topic, &message).await.unwrap().await.unwrap();
        println!("producer sends done");

        {
            println!("creating consumer");
            let mut consumer: Consumer<TestData, _> = client
                .consumer()
                .with_topic(&topic)
                .with_subscription("dropped_ack")
                .with_subscription_type(SubType::Shared)
                // get earliest messages
                .with_options(ConsumerOptions {
                    initial_position: InitialPosition::Earliest,
                    ..Default::default()
                })
                .build()
                .await
                .unwrap();

            println!("created consumer");

            //consumer.next().await
            let msg: Message<TestData> = timeout(Duration::from_secs(1), consumer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            println!("got message: {:?}", msg.payload);
            assert_eq!(
                message,
                msg.deserialize().unwrap(),
                "we probably receive a message from a previous run of the test"
            );
            consumer.ack(&msg).await.unwrap();
        }

        {
            println!("creating second consumer. The message should have been acked");
            let mut consumer: Consumer<TestData, _> = client
                .consumer()
                .with_topic(&topic)
                .with_subscription("dropped_ack")
                .with_subscription_type(SubType::Shared)
                .with_options(ConsumerOptions {
                    initial_position: InitialPosition::Earliest,
                    ..Default::default()
                })
                .build()
                .await
                .unwrap();

            println!("created second consumer");

            // the message has already been acked, so we should not receive anything
            let res: Result<_, tokio::time::error::Elapsed> =
                tokio::time::timeout(Duration::from_secs(1), consumer.next()).await;
            let is_err = res.is_err();
            if let Ok(val) = res {
                let msg = val.unwrap().unwrap();
                println!("got message: {:?}", msg.payload);
                // cleanup for the next test
                consumer.ack(&msg).await.unwrap();
                // we should not receive a different message anyway
                assert_eq!(message, msg.deserialize().unwrap());
            }

            assert!(is_err, "waiting for a message should have timed out, since we already acknowledged the only message in the queue");
        }
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn dead_letter_queue() {
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";

        let test_id: u16 = rand::random();
        let topic = format!("dead_letter_queue_test_{}", test_id);
        let test_msg: u32 = rand::random();

        let message = TestData {
            topic: topic.clone(),
            msg: test_msg,
        };

        let dead_letter_topic = format!("{}_dlq", topic);

        let dead_letter_policy = crate::consumer::DeadLetterPolicy {
            max_redeliver_count: 1,
            dead_letter_topic: dead_letter_topic.clone(),
        };

        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        println!("creating consumer");
        let mut consumer: Consumer<TestData, _> = client
            .consumer()
            .with_topic(topic.clone())
            .with_subscription("nack")
            .with_subscription_type(SubType::Shared)
            .with_dead_letter_policy(dead_letter_policy)
            .build()
            .await
            .unwrap();

        println!("created consumer");

        println!("creating second consumer that consumes from the DLQ");
        let mut dlq_consumer: Consumer<TestData, _> = client
            .clone()
            .consumer()
            .with_topic(dead_letter_topic)
            .with_subscription("dead_letter_topic")
            .with_subscription_type(SubType::Shared)
            .build()
            .await
            .unwrap();

        println!("created second consumer");

        client.send(&topic, &message).await.unwrap().await.unwrap();
        println!("producer sends done");

        let msg = consumer.next().await.unwrap().unwrap();
        println!("got message: {:?}", msg.payload);
        assert_eq!(
            message,
            msg.deserialize().unwrap(),
            "we probably received a message from a previous run of the test"
        );
        // Nacking message to send it to DLQ
        consumer.nack(&msg).await.unwrap();

        let dlq_msg = dlq_consumer.next().await.unwrap().unwrap();
        println!("got message: {:?}", msg.payload);
        assert_eq!(
            message,
            dlq_msg.deserialize().unwrap(),
            "we probably received a message from a previous run of the test"
        );
        dlq_consumer.ack(&dlq_msg).await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn failover() {
        let _ = log::set_logger(&MULTI_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("failover_{}", rand::random::<u16>());
        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        let msg_count = 100_u32;
        try_join_all((0..msg_count).map(|i| client.send(&topic, i.to_string())))
            .await
            .unwrap();

        let builder = client
            .consumer()
            .with_subscription("failover")
            .with_topic(&topic)
            .with_subscription_type(SubType::Failover)
            // get earliest messages
            .with_options(ConsumerOptions {
                initial_position: InitialPosition::Earliest,
                ..Default::default()
            });

        let mut consumer_1: Consumer<String, _> = builder.clone().build().await.unwrap();

        let mut consumer_2: Consumer<String, _> = builder.build().await.unwrap();

        let mut consumed_1 = 0_u32;
        let mut consumed_2 = 0_u32;
        let mut pending_1 = Some(consumer_1.next());
        let mut pending_2 = Some(consumer_2.next());
        while consumed_1 + consumed_2 < msg_count {
            let next = select(pending_1.take().unwrap(), pending_2.take().unwrap());
            match timeout(Duration::from_secs(2), next).await.unwrap() {
                Either::Left((msg, pending)) => {
                    consumed_1 += 1;
                    let _ = consumer_1.ack(&msg.unwrap().unwrap());
                    pending_1 = Some(consumer_1.next());
                    pending_2 = Some(pending);
                }
                Either::Right((msg, pending)) => {
                    consumed_2 += 1;
                    let _ = consumer_2.ack(&msg.unwrap().unwrap());
                    pending_1 = Some(pending);
                    pending_2 = Some(consumer_2.next());
                }
            }
        }
        match (consumed_1, consumed_2) {
            (consumed_1, 0) => assert_eq!(consumed_1, msg_count),
            (0, consumed_2) => assert_eq!(consumed_2, msg_count),
            _ => panic!("Expected one consumer to consume all messages. Message count: {}, consumer_1: {} consumer_2: {}", msg_count, consumed_1, consumed_2),
        }
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn seek_single_consumer() {
        let _ = log::set_logger(&MULTI_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        log::info!("starting seek test");
        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("seek_{}", rand::random::<u16>());
        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        // send 100 messages and record the starting time
        let msg_count = 100_u32;

        let start_time: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        std::thread::sleep(Duration::from_secs(2));

        println!("this is the starting time: {}", start_time);

        try_join_all((0..msg_count).map(|i| client.send(&topic, i.to_string())))
            .await
            .unwrap();
        log::info!("sent all messages");

        let mut consumer_1: Consumer<String, _> = client
            .consumer()
            .with_consumer_name("seek_single_test")
            .with_subscription("seek_single_test")
            .with_subscription_type(SubType::Shared)
            .with_topic(&topic)
            .build()
            .await
            .unwrap();

        log::info!("built the consumer");

        let mut consumed_1 = 0_u32;
        while let Some(msg) = consumer_1.try_next().await.unwrap() {
            consumer_1.ack(&msg).await.unwrap();
            let publish_time = msg.metadata().publish_time;
            let data = match msg.deserialize() {
                Ok(data) => data,
                Err(e) => {
                    log::error!("could not deserialize message: {:?}", e);
                    break;
                }
            };

            consumed_1 += 1;
            log::info!(
                "first loop, got {} messages, content: {}, publish time: {}",
                consumed_1,
                data,
                publish_time
            );

            //break after enough half of the messages were received
            if consumed_1 >= msg_count / 2 {
                log::info!("first loop, received {} messages, so break", consumed_1);
                break;
            }
        }

        // // call seek(timestamp), roll back the consumer to start_time
        log::info!("calling seek method");
        let _seek_result = consumer_1
            .seek(None, None, Some(start_time), client)
            .await
            .unwrap();

        // let mut consumer_2: Consumer<String, _> = client
        // .consumer()
        // .with_consumer_name("seek")
        // .with_subscription("seek")
        // .with_topic(&topic)
        // .build()
        // .await
        // .unwrap();

        // then read the messages again
        let mut consumed_2 = 0_u32;
        log::info!("reading messages again");
        while let Some(msg) = consumer_1.try_next().await.unwrap() {
            let publish_time = msg.metadata().publish_time;
            consumer_1.ack(&msg).await.unwrap();
            let data = match msg.deserialize() {
                Ok(data) => data,
                Err(e) => {
                    log::error!("could not deserialize message: {:?}", e);
                    break;
                }
            };
            consumed_2 += 1;
            log::info!(
                "second loop, got {} messages, content: {},  publish time: {}",
                consumed_2,
                data,
                publish_time,
            );

            if consumed_2 >= msg_count {
                log::info!("received {} messagses, so break", consumed_2);
                break;
            }
        }

        //then check if all messages were received
        assert_eq!(50, consumed_1);
        assert_eq!(100, consumed_2);
    }
}
