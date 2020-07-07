//! Topic subscriptions
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::channel::mpsc::unbounded;
use futures::future::{try_join_all, Either};
use futures::task::{Context, Poll};
use futures::{
    channel::{mpsc, oneshot},
    Future, FutureExt, SinkExt, Stream, StreamExt,
};
use regex::Regex;

use crate::connection::Connection;
use crate::error::{ConnectionError, ConsumerError, Error};
use crate::executor::Executor;
use crate::message::{
    parse_batched_message,
    proto::{self, command_subscribe::SubType, MessageIdData, MessageMetadata, Schema},
    BatchedMessage, Message as RawMessage, Metadata, Payload,
};
use crate::{BrokerAddress, DeserializeMessage, Pulsar};
use core::iter;
use rand::distributions::Alphanumeric;
use rand::Rng;
use url::Url;

/// Configuration options for consumers
#[derive(Clone, Default, Debug)]
pub struct ConsumerOptions {
    pub priority_level: Option<i32>,
    pub durable: Option<bool>,
    pub start_message_id: Option<MessageIdData>,
    pub metadata: BTreeMap<String, String>,
    pub read_compacted: Option<bool>,
    pub schema: Option<Schema>,
    pub initial_position: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct DeadLetterPolicy {
    //Maximum number of times that a message will be redelivered before being sent to the dead letter queue.
    pub max_redeliver_count: usize,
    //Name of the dead topic where the failing messages will be sent.
    pub dead_letter_topic: String,
}

/// the consumer is used to subscribe to a topic
///
/// ```rust,ignore
/// let mut consumer: Consumer<TestData> = pulsar
///     .consumer()
///     .with_topic("non-persistent://public/default/test")
///     .with_consumer_name("test_consumer")
///     .with_subscription_type(SubType::Exclusive)
///     .with_subscription("test_subscription")
///     .build()
///     .await?;
///
/// let mut counter = 0usize;
/// while let Some(msg) = consumer.try_next().await? {
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
/// ```
pub struct Consumer<T: DeserializeMessage, Exe: Executor + ?Sized> {
    inner: InnerConsumer<T, Exe>,
}
impl<T: DeserializeMessage, Exe: Executor + ?Sized> Consumer<T, Exe> {
    pub fn builder(pulsar: &Pulsar<Exe>) -> ConsumerBuilder<Exe> {
        ConsumerBuilder::new(pulsar)
    }

    pub fn topics(&self) -> Vec<String> {
        match &self.inner {
            InnerConsumer::Single(c) => vec![c.topic.clone()],
            InnerConsumer::Mulit(c) => c.topics(),
        }
    }

    pub fn connections(&self) -> Vec<&Url> {
        match &self.inner {
            InnerConsumer::Single(c) => vec![c.connection.url()],
            InnerConsumer::Mulit(c) => {
                c.consumers.values()
                    .map(|c| c.connection.url())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            },
        }
    }

    pub fn options(&self) -> &ConsumerOptions {
        match &self.inner {
            InnerConsumer::Single(c) => &c.options(),
            InnerConsumer::Mulit(c) => c.options(),
        }
    }

    pub fn dead_letter_policy(&self) -> Option<&DeadLetterPolicy> {
        match &self.inner {
            InnerConsumer::Single(c) => c.dead_letter_policy.as_ref(),
            InnerConsumer::Mulit(c) => c.consumer_config.dead_letter_policy.as_ref(),
        }
    }

    pub async fn check_connection(&self) -> Result<(), Error> {
        match &self.inner {
            InnerConsumer::Single(c) => c.check_connection().await,
            InnerConsumer::Mulit(c) => c.check_connections().await,
        }
    }

    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.ack(msg).await,
            InnerConsumer::Mulit(c) => c.ack(msg).await,
        }
    }

    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.cumulative_ack(msg).await,
            InnerConsumer::Mulit(c) => c.cumulative_ack(msg).await,
        }
    }

    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.nack(msg).await,
            InnerConsumer::Mulit(c) => c.nack(msg).await,
        }
    }

    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        match &self.inner {
            InnerConsumer::Single(c) => c.last_message_received(),
            InnerConsumer::Mulit(c) => c.last_message_received(),
        }
    }

    pub fn messages_received(&self) -> u64 {
        match &self.inner {
            InnerConsumer::Single(c) => c.messages_received(),
            InnerConsumer::Mulit(c) => c.messages_received(),
        }
    }
}

enum InnerConsumer<T: DeserializeMessage, Exe: Executor> {
    Single(TopicConsumer<T>),
    Mulit(MultiTopicConsumer<T, Exe>),
}

pub(crate) struct TopicConsumer<T: DeserializeMessage> {
    connection: Arc<Connection>,
    topic: String,
    messages: Pin<Box<mpsc::Receiver<Result<(proto::MessageIdData, Payload), Error>>>>,
    ack_tx: mpsc::UnboundedSender<AckMessage>,
    #[allow(unused)]
    data_type: PhantomData<fn(Payload) -> T::Output>,
    options: ConsumerOptions,
    dead_letter_policy: Option<DeadLetterPolicy>,
    last_message_received: Option<DateTime<Utc>>,
    messages_received: u64,
}

impl<T: DeserializeMessage> TopicConsumer<T> {
    async fn new<Exe: Executor + ?Sized>(
        client: Pulsar<Exe>,
        topic: String,
        addr: BrokerAddress,
        config: ConsumerConfig,
    ) -> Result<TopicConsumer<T>, Error> {
        let ConsumerConfig {
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_redelivery_delay,
            options,
            dead_letter_policy
        } = config;
        let connection = client.manager.get_connection(&addr).await?;
        let consumer_id = consumer_id.unwrap_or_else(rand::random);
        let (resolver, messages) = mpsc::unbounded();
        let batch_size = batch_size.unwrap_or(1000);

        connection
            .sender()
            .subscribe(
                resolver,
                topic.clone(),
                subscription.clone(),
                sub_type,
                consumer_id,
                consumer_name.clone(),
                options.clone(),
            )
            .await
            .map_err(Error::Connection)?;

        connection
            .sender()
            .send_flow(consumer_id, batch_size)
            .map_err(|e| Error::Consumer(ConsumerError::Connection(e)))?;

        let (ack_tx, ack_rx) = unbounded();
        // drop_signal will be dropped when Consumer is dropped, then
        // drop_receiver will return, and we can close the consumer
        let (_drop_signal, drop_receiver) = oneshot::channel::<()>();
        let conn = connection.clone();
        //let ack_sender = nack_handler.clone();
        let name = consumer_name.clone();
        let _ = Exe::spawn(Box::pin(async move {
            let _res = drop_receiver.await;
            // if we receive a message, it indicates we want to stop this task
            if _res.is_err() {
                if let Err(e) = conn.sender().close_consumer(consumer_id).await {
                    error!(
                        "could not close consumer {:?}({}): {:?}",
                        consumer_name, consumer_id, e
                    );
                }
            }
        }));

        if unacked_message_redelivery_delay.is_some() {
            let mut redelivery_tx = ack_tx.clone();
            let mut interval = Exe::interval(Duration::from_millis(500));
            if Exe::spawn(Box::pin(async move {
                while interval.next().await.is_some() {
                    if let Err(e) = redelivery_tx.send(AckMessage::UnackedRedelivery).await {
                        error!("could not send redelivery ticker: {:?}", e);
                    }
                }
            }))
            .is_err()
            {
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
            ack_tx.clone(),
            ack_rx,
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
        if Exe::spawn(Box::pin(f)).is_err() {
            return Err(Error::Executor);
        }

        Ok(TopicConsumer {
            connection,
            topic,
            messages: Box::pin(rx),
            ack_tx,
            data_type: PhantomData,
            options,
            dead_letter_policy,
            last_message_received: None,
            messages_received: 0,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn options(&self) -> &ConsumerOptions {
        &self.options
    }

    pub async fn check_connection(&self) -> Result<(), Error> {
        self.connection.sender().send_ping().await?;
        Ok(())
    }

    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.ack_tx
            .send(AckMessage::Ack(msg.message_id.clone(), false))
            .await?;
        Ok(())
    }

    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.ack_tx
            .send(AckMessage::Ack(msg.message_id.clone(), true))
            .await?;
        Ok(())
    }

    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.ack_tx
            .send(AckMessage::Nack(msg.message_id.clone()))
            .await?;
        Ok(())
    }

    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.last_message_received
    }

    pub fn messages_received(&self) -> u64 {
        self.messages_received
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

impl<T: DeserializeMessage> Stream for TopicConsumer<T> {
    type Item = Result<Message<T>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.messages.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                Poll::Ready(Some(Err(Error::Connection(ConnectionError::Disconnected))))
            }
            Poll::Ready(Some(Ok((id, payload)))) => {
                self.last_message_received = Some(Utc::now());
                self.messages_received += 1;
                Poll::Ready(Some(Ok(self.create_message(id, payload))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
        }
    }
}

struct ConsumerEngine<Exe: Executor + ?Sized> {
    client: Pulsar<Exe>,
    connection: Arc<Connection>,
    topic: String,
    subscription: String,
    sub_type: SubType,
    id: u64,
    name: Option<String>,
    tx: mpsc::Sender<Result<(proto::MessageIdData, Payload), Error>>,
    messages_rx: Option<mpsc::UnboundedReceiver<RawMessage>>,
    ack_tx: mpsc::UnboundedSender<AckMessage>,
    ack_rx: Option<mpsc::UnboundedReceiver<AckMessage>>,
    batch_size: u32,
    remaining_messages: u32,
    unacked_message_redelivery_delay: Option<Duration>,
    unacked_messages: HashMap<MessageIdData, Instant>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    options: ConsumerOptions,
    _drop_signal: oneshot::Sender<()>,
}

enum AckMessage {
    Ack(MessageData, bool),
    Nack(MessageData),
    UnackedRedelivery,
}

impl<Exe: Executor + ?Sized> ConsumerEngine<Exe> {
    fn new(
        client: Pulsar<Exe>,
        connection: Arc<Connection>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        id: u64,
        name: Option<String>,
        tx: mpsc::Sender<Result<(proto::MessageIdData, Payload), Error>>,
        messages_rx: mpsc::UnboundedReceiver<RawMessage>,
        ack_tx: mpsc::UnboundedSender<AckMessage>,
        ack_rx: mpsc::UnboundedReceiver<AckMessage>,
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
            ack_tx,
            ack_rx: Some(ack_rx),
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
        loop {
            if !self.connection.is_valid() {
                if let Some(err) = self.connection.error() {
                    error!("Consumer: connection is not valid: {:?}", err);
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

            // we need these complicated steps to select on two streams of different types,
            // while being able to store it in the ConsumerEngine object (biggest issue),
            // and replacing messages_rx when we reconnect, and cnsidering that ack_rx is
            // not clonable.
            // Please, someone find a better solution
            let messages_f = self.messages_rx.take().unwrap().into_future();
            let ack_f = self.ack_rx.take().unwrap().into_future();
            match futures::future::select(messages_f, ack_f).await {
                Either::Left(((message_opt, messages_rx), ack_rx)) => {
                    self.messages_rx = Some(messages_rx);
                    self.ack_rx = ack_rx.into_inner();
                    match message_opt {
                        None => {
                            error!("Consumer: messages::next: returning Disconnected");
                            self.reconnect().await?;
                            continue;
                            //return Err(Error::Consumer(ConsumerError::Connection(ConnectionError::Disconnected)).into());
                        }
                        Some(message) => {
                            self.remaining_messages -= 1;
                            if let Err(e) = self.process_message(message).await {
                                if let Err(e) = self.tx.send(Err(e)).await {
                                    error!("cannot send a message from the consumer engine to the consumer({}), stopping the engine", self.id);
                                    return Err(Error::Consumer(e.into()));
                                }
                            }
                        }
                    }
                }
                Either::Right(((ack_opt, ack_rx), messages_rx)) => {
                    self.messages_rx = messages_rx.into_inner();
                    self.ack_rx = Some(ack_rx);

                    match ack_opt {
                        None => {
                            trace!("ack channel was closed");
                            return Ok(());
                        }
                        Some(AckMessage::Ack(message_id, cumulative)) => {
                            //FIXME: this does not handle cumulative acks
                            self.unacked_messages.remove(&message_id.id);
                            let res = self.connection.sender().send_ack(
                                self.id,
                                vec![message_id.id.clone()],
                                cumulative,
                            );
                            if res.is_err() {
                                error!("ack error: {:?}", res);
                            }
                        }
                        Some(AckMessage::Nack(message_id)) => {
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
                        Some(AckMessage::UnackedRedelivery) => {
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
                                        self.unacked_messages.remove(&i);
                                    }
                                }
                            }
                        }
                    }
                }
            };
        }
    }

    async fn process_message(&mut self, message: RawMessage) -> Result<(), Error> {
        let RawMessage { command, payload } = message;
        let (message, mut payload) = match (command.message.clone(), payload) {
            (Some(message), Some(payload)) => (message, payload),
            (Some(message), None) => {
                return Err(Error::Consumer(ConsumerError::MissingPayload(format!(
                    "expecting payload with {:?}",
                    message
                ))));
            }
            (None, Some(_)) => {
                return Err(Error::Consumer(ConsumerError::MissingPayload(format!(
                    "expecting 'message' command in {:?}",
                    command
                ))));
            }
            (None, None) => {
                return Err(Error::Consumer(ConsumerError::MissingPayload(format!(
                    "expecting 'message' command and payload in {:?}",
                    command
                ))));
            }
        };

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
                    use std::io::Read;

                    let mut decompressed_payload = Vec::new();
                    let mut decoder =
                        lz4::Decoder::new(&payload.data[..]).map_err(ConsumerError::Io)?;
                    decoder
                        .read_to_end(&mut decompressed_payload)
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
                            .send(
                                dead_letter_policy.dead_letter_topic.clone(),
                                payload.data,
                                producer::ProducerOptions::default(),
                            )
                            .await?
                            .await
                            .map_err(|e| {
                                error!("One shot cancelled {:?}", e);
                                Error::Custom("DLQ send error".to_string())
                            })?;

                        self.ack_tx
                            .send(AckMessage::Ack(
                                MessageData {
                                    id: message.message_id,
                                    batch_size: None,
                                },
                                false,
                            ))
                            .await
                            .map_err(|e| {
                                error!("ack tx returned {:?}", e);
                                Error::Custom("tx closed".to_string())
                            })?;
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
        debug!("reconnecting producer for topic: {}", self.topic);
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
        let _ = Exe::spawn(Box::pin(async move {
            let _res = drop_receiver.await;
            // if we receive a message, it indicates we want to stop this task
            if _res.is_err() {
                if let Err(e) = conn.sender().close_consumer(id).await {
                    error!("could not close consumer {:?}({}): {:?}", name, id, e);
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MessageData {
    id: proto::MessageIdData,
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

pub struct SingleTopic;
pub struct MultiTopic;
pub struct Unset;

pub struct ConsumerBuilder<Exe: Executor + ?Sized> {
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

    // Currently only used for multi-topic
    namespace: Option<String>,
    topic_refresh: Option<Duration>,
}

impl<Exe: Executor> ConsumerBuilder<Exe> {
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

    pub fn with_topic<S: Into<String>>(mut self, topic: S) -> ConsumerBuilder<Exe> {
        match &mut self.topics {
            Some(topics) => topics.push(topic.into()),
            None => self.topics = Some(vec![topic.into()]),
        }
        self
    }

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

    pub fn with_topic_regex(mut self, regex: Regex) -> ConsumerBuilder<Exe> {
        self.topic_regex = Some(regex);
        self
    }

    pub fn with_subscription<S: Into<String>>(mut self, subscription: S) -> Self {
        self.subscription = Some(subscription.into());
        self
    }

    pub fn with_subscription_type(mut self, subscription_type: SubType) -> Self {
        self.subscription_type = Some(subscription_type);
        self
    }

    pub fn with_namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_topic_refresh(mut self, refresh_interval: Duration) -> Self {
        self.topic_refresh = Some(refresh_interval);
        self
    }

    pub fn with_consumer_id(mut self, consumer_id: u64) -> Self {
        self.consumer_id = Some(consumer_id);
        self
    }

    pub fn with_consumer_name<S: Into<String>>(mut self, consumer_name: S) -> Self {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn with_options(mut self, options: ConsumerOptions) -> Self {
        self.consumer_options = Some(options);
        self
    }

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

    pub async fn build<T: DeserializeMessage>(self) -> Result<Consumer<T, Exe>, Error> {
        let ConsumerBuilder {
            pulsar,
            topics,
            topic_regex,
            subscription,
            subscription_type,
            consumer_id,
            consumer_name,
            batch_size,
            unacked_message_resend_delay,
            namespace,
            topic_refresh,
            consumer_options,
            dead_letter_policy,
        } = self;

        if topics.is_none() && topic_regex.is_none() {
            return Err(Error::Custom("Cannot create consumer with no topics and no topic regex".into()));
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

        if topics.len() == 0 && topic_regex.is_none() {
            return Err(Error::Custom(format!(
                "Unable to create consumer - topic not found"
            )));
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
        };

        let consumers =
            try_join_all(topics.into_iter().map(|(topic, addr)| {
                TopicConsumer::new(pulsar.clone(), topic, addr, config.clone())
            }))
            .await?;

        let consumer = if consumers.len() == 1 {
            let consumer = consumers.into_iter().next().unwrap();
            InnerConsumer::Single(consumer)
        } else {
            let consumers: BTreeMap<_, _> = consumers
                .into_iter()
                .map(|c| (c.topic().to_owned(), Box::pin(c)))
                .collect();
            let topics = consumers.keys().map(|c| c.clone()).collect();
            let topic_refresh = topic_refresh.unwrap_or(Duration::from_secs(30));
            let consumer = MultiTopicConsumer {
                namespace: namespace.unwrap_or_else(|| "public/default".to_string()),
                topic_regex,
                pulsar,
                consumers,
                topics,
                new_consumers: None,
                refresh: Box::pin(Exe::interval(topic_refresh).map(drop)),
                consumer_config: config,
                disc_last_message_received: None,
                disc_messages_received: 0,
            };
            InnerConsumer::Mulit(consumer)
        };
        Ok(Consumer { inner: consumer })
    }
}

#[derive(Debug, Clone, Default)]
struct ConsumerConfig {
    subscription: String,
    sub_type: SubType,
    batch_size: Option<u32>,
    consumer_name: Option<String>,
    consumer_id: Option<u64>,
    unacked_message_redelivery_delay: Option<Duration>,
    options: ConsumerOptions,
    dead_letter_policy: Option<DeadLetterPolicy>,
}

/// A consumer that can subscribe on multiple topics, from a rege matching topic names
struct MultiTopicConsumer<T: DeserializeMessage, Exe: Executor> {
    namespace: String,
    topic_regex: Option<Regex>,
    pulsar: Pulsar<Exe>,
    consumers: BTreeMap<String, Pin<Box<TopicConsumer<T>>>>,
    topics: VecDeque<String>,
    new_consumers:
        Option<Pin<Box<dyn Future<Output = Result<Vec<TopicConsumer<T>>, Error>> + Send>>>,
    refresh: Pin<Box<dyn Stream<Item = ()> + Send>>,
    consumer_config: ConsumerConfig,
    // Stats on disconnected consumers to keep metrics correct
    disc_messages_received: u64,
    disc_last_message_received: Option<DateTime<Utc>>,
}

impl<T: DeserializeMessage, Exe: Executor> MultiTopicConsumer<T, Exe> {
    pub fn options(&self) -> &ConsumerOptions {
        &self.consumer_config.options
    }

    pub fn topics(&self) -> Vec<String> {
        self.topics.iter().map(|s| s.to_string()).collect()
    }

    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.consumers
            .values()
            .filter_map(|c| c.last_message_received)
            .chain(self.disc_last_message_received)
            .max()
    }

    pub fn messages_received(&self) -> u64 {
        self.consumers
            .values()
            .map(|c| c.messages_received)
            .chain(iter::once(self.disc_messages_received))
            .sum()
    }

    pub async fn check_connections(&self) -> Result<(), Error> {
        let base_conn = self.pulsar.manager.get_base_connection().await?;
        let connections: BTreeMap<_, _> = iter::once(base_conn)
            .chain(self.consumers.values().map(|c| c.connection.clone()))
            .map(|c| (c.id(), c))
            .collect();

        try_join_all(connections.values().map(|c| c.sender().send_ping())).await?;
        Ok(())
    }

    fn add_consumers<I: IntoIterator<Item = TopicConsumer<T>>>(&mut self, consumers: I) {
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
            let consumer_config = self.consumer_config.clone();

            self.new_consumers = Some(Box::pin(async move {
                let topics = try_join_all(
                    pulsar
                        .get_topics_of_namespace(namespace.clone(), proto::get_topics::Mode::All)
                        .await?
                        .into_iter()
                        .filter(|t| regex.is_match(t))
                        .map(|topic| pulsar.lookup_partitioned_topic(topic)),
                )
                .await?
                .into_iter()
                .flatten();

                let consumers = try_join_all(
                    topics
                        .into_iter()
                        .filter(|(t, _)| existing_topics.contains(t))
                        .map(|(topic, addr)| {
                            TopicConsumer::new(pulsar.clone(), topic, addr, consumer_config.clone())
                        }),
                )
                .await?;
                Ok(consumers)
            }));
        }
    }

    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.ack(&msg).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.cumulative_ack(&msg).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.nack(&msg).await?;
            Ok(())
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }
}

pub struct Message<T> {
    pub topic: String,
    pub payload: Payload,
    message_id: MessageData,
    _phantom: PhantomData<T>,
}

impl<T> Message<T> {
    pub fn metadata(&self) -> &MessageMetadata {
        &self.payload.metadata
    }
}
impl<T: DeserializeMessage> Message<T> {
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;
    use std::thread;

    use log::LevelFilter;
    use regex::Regex;
    #[cfg(feature = "tokio-runtime")]
    use tokio::runtime::Runtime;

    #[cfg(feature = "tokio-runtime")]
    use crate::executor::TokioExecutor;
    use crate::{producer, tests::TEST_LOGGER, Pulsar, SerializeMessage};

    use super::*;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub struct TestData {
        topic: String,
        msg: u32,
    }

    impl SerializeMessage for TestData {
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
    #[test]
    #[cfg(feature = "tokio-runtime")]
    fn multi_consumer() {
        let _ = log::set_logger(&MULTI_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";
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

        let error: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        let successes = Arc::new(AtomicUsize::new(0));
        let err = error.clone();

        let succ = successes.clone();

        let f = async move {
            let client: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await.unwrap();
            let mut producer = client.create_multi_topic_producer(None);

            let send_start = Utc::now();
            producer
                .send(topic1, data1.clone())
                .await
                .unwrap()
                .await
                .unwrap();
            producer
                .send(topic1, data2.clone())
                .await
                .unwrap()
                .await
                .unwrap();
            producer
                .send(topic2, data3.clone())
                .await
                .unwrap()
                .await
                .unwrap();
            producer
                .send(topic2, data4.clone())
                .await
                .unwrap()
                .await
                .unwrap();

            let data = vec![data1, data2, data3, data4];

            let mut consumer: MultiTopicConsumer<TestData, _> = client
                .consumer()
                // TODO extract out test functionality to test both multi-topic cases
                // .with_topics(&["mt_test_a", "mt_test_b"])
                .with_topic_regex(Regex::new("mt_test_[ab]").unwrap())
                .with_namespace(namespace)
                .with_subscription("test_sub")
                .with_subscription_type(SubType::Shared)
                .with_topic_refresh(Duration::from_secs(1))
                // get earliest messages
                .with_options(ConsumerOptions {
                    initial_position: Some(1),
                    ..Default::default()
                })
                .build();

            let mut counter = 0usize;
            while let Some(res) = consumer.next().await {
                match res {
                    Ok(message) => {
                        consumer.ack(&message).await.unwrap();
                        let msg = message.deserialize().unwrap();
                        if !data.contains(&msg) {
                            panic!("Unexpected message: {:?}", &msg);
                        } else {
                            succ.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        let err = err.clone();
                        let mut error = err.lock().unwrap();
                        *error = Some(e);
                    }
                }

                counter += 1;
                if counter == 4 {
                    break;
                }
            }

            let consumer_state: Vec<ConsumerState> =
                consumer_state.collect::<Vec<ConsumerState>>().await;
            let latest_state = consumer_state.last().unwrap();
            assert!(latest_state.messages_received >= 4);
            assert!(latest_state.connected_topics.len() >= 2);
            assert!(latest_state.last_message_received.unwrap() >= send_start);
        };

        rt.spawn(f);

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

    #[test]
    #[cfg(feature = "tokio-runtime")]
    fn consumer_dropped_with_lingering_acks() {
        use rand::{distributions::Alphanumeric, Rng};
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";
        let mut rt = Runtime::new().unwrap();

        let topic = "issue_51";

        let f = async move {
            let client: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await.unwrap();

            let message = TestData {
                topic: std::iter::repeat(())
                    .map(|()| rand::thread_rng().sample(Alphanumeric))
                    .take(8)
                    .collect(),
                msg: 1,
            };

            {
                let mut producer = client
                    .create_producer(topic, None, producer::ProducerOptions::default())
                    .await
                    .unwrap();

                producer.send(message.clone()).await.unwrap().await.unwrap();
                println!("producer sends done");
            }

            {
                println!("creating consumer");
                let mut consumer: Consumer<TestData, _> = client
                    .consumer()
                    .with_topic(topic)
                    .with_subscription("dropped_ack")
                    .with_subscription_type(SubType::Shared)
                    // get earliest messages
                    .with_options(ConsumerOptions {
                        initial_position: Some(1),
                        ..Default::default()
                    })
                    .build()
                    .await
                    .unwrap();

                println!("created consumer");

                //consumer.next().await
                let msg = consumer.next().await.unwrap().unwrap();
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
                    .with_topic(topic)
                    .with_subscription("dropped_ack")
                    .with_subscription_type(SubType::Shared)
                    .with_options(ConsumerOptions {
                        initial_position: Some(1),
                        ..Default::default()
                    })
                    .build()
                    .await
                    .unwrap();

                println!("created second consumer");

                // the message has already been acked, so we should not receive anything
                let res: Result<_, tokio::time::Elapsed> =
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
        };

        rt.block_on(f);
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

        let client: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await.unwrap();

        let mut producer = client
            .create_producer(topic.clone(), None, producer::ProducerOptions::default())
            .await
            .unwrap();

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

        producer.send(message.clone()).await.unwrap().await.unwrap();
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
}
