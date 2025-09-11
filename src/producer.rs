//! Message publication
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, VecDeque},
    io::Write,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::{
    channel::{mpsc, oneshot},
    future::{self, try_join_all, Either},
    task::{Context, Poll},
    Future, SinkExt, StreamExt,
};

use crate::{
    client::SerializeMessage,
    compression::Compression,
    connection::{Connection, SerialId},
    error::{ConnectionError, ProducerError},
    executor::Executor,
    message::{
        proto::{self, CommandSendReceipt, EncryptionKeys, Schema},
        BatchedMessage,
    },
    retry_op::retry_create_producer,
    BrokerAddress, Error, Pulsar,
};

type ProducerId = u64;
type ProducerName = String;

/// returned by [Producer::send]
///
/// it contains a channel on which we can await to get the message receipt.
/// Depending on the producer's configuration (batching, flow control, etc)and
/// the server's load, the send receipt could come much later after sending it
pub struct SendFuture(pub(crate) oneshot::Receiver<Result<CommandSendReceipt, Error>>);

impl Future for SendFuture {
    type Output = Result<CommandSendReceipt, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_)) => Poll::Ready(Err(ProducerError::Custom(
                "producer unexpectedly disconnected".into(),
            )
            .into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// message data that will be sent on a topic
///
/// generated from the [SerializeMessage] trait or [MessageBuilder]
///
/// this is actually a subset of the fields of a message, because batching,
/// compression and encryption should be handled by the producer
#[derive(Debug, Clone, Default)]
pub struct Message {
    /// serialized data
    pub payload: Vec<u8>,
    /// user defined properties
    pub properties: HashMap<String, String>,
    /// key to decide partition for the message
    pub partition_key: ::std::option::Option<String>,
    /// key to decide partition for the message
    pub ordering_key: ::std::option::Option<Vec<u8>>,
    /// Override namespace's replication
    pub replicate_to: ::std::vec::Vec<String>,
    /// the timestamp that this event occurs. it is typically set by applications.
    /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    pub event_time: ::std::option::Option<u64>,
    /// current version of the schema
    pub schema_version: ::std::option::Option<Vec<u8>>,
    /// UTC Unix timestamp in milliseconds, time at which the message should be
    /// delivered to consumers
    pub deliver_at_time: ::std::option::Option<i64>,
}

/// internal message type carrying options that must be defined
/// by the producer
#[derive(Debug, Clone, Default)]
pub(crate) struct ProducerMessage {
    pub payload: Vec<u8>,
    pub properties: HashMap<String, String>,
    ///key to decide partition for the msg
    pub partition_key: ::std::option::Option<String>,
    ///key to decide partition for the msg
    pub ordering_key: ::std::option::Option<Vec<u8>>,
    /// Override namespace's replication
    pub replicate_to: ::std::vec::Vec<String>,
    pub compression: ::std::option::Option<i32>,
    pub uncompressed_size: ::std::option::Option<u32>,
    /// Removed below checksum field from Metadata as
    /// it should be part of send-command which keeps checksum of header + payload
    ///optional sfixed64 checksum = 10;
    ///differentiate single and batch message metadata
    pub num_messages_in_batch: ::std::option::Option<i32>,
    pub event_time: ::std::option::Option<u64>,
    /// Contains encryption key name, encrypted key and metadata to describe the key
    pub encryption_keys: ::std::vec::Vec<EncryptionKeys>,
    /// Algorithm used to encrypt data key
    pub encryption_algo: ::std::option::Option<String>,
    /// Additional parameters required by encryption
    pub encryption_param: ::std::option::Option<Vec<u8>>,
    pub schema_version: ::std::option::Option<Vec<u8>>,
    /// UTC Unix timestamp in milliseconds, time at which the message should be
    /// delivered to consumers
    pub deliver_at_time: ::std::option::Option<i64>,
}

impl From<Message> for ProducerMessage {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(m: Message) -> Self {
        ProducerMessage {
            payload: m.payload,
            properties: m.properties,
            partition_key: m.partition_key,
            ordering_key: m.ordering_key,
            replicate_to: m.replicate_to,
            event_time: m.event_time,
            schema_version: m.schema_version,
            deliver_at_time: m.deliver_at_time,
            ..Default::default()
        }
    }
}

/// Configuration options for producers
#[derive(Clone, Default)]
pub struct ProducerOptions {
    /// end to end message encryption (not implemented yet)
    pub encrypted: Option<bool>,
    /// user defined properties added to all messages
    pub metadata: BTreeMap<String, String>,
    /// schema used to encode this producer's messages
    pub schema: Option<Schema>,
    /// batch message size
    pub batch_size: Option<u32>,
    /// batch size in bytes treshold (only relevant when batch_size active).
    /// batch is sent when batch size in bytes is reached
    pub batch_byte_size: Option<usize>,
    /// the batch will be sent if this timeout is reached after the 1st message is added into the
    /// batch even if it does not reach the size or byte size limit.
    pub batch_timeout: Option<Duration>,
    /// algorithm used to compress the messages
    pub compression: Option<Compression>,
    /// producer access mode: shared = 0, exclusive = 1, waitforexclusive =2,
    /// exclusivewithoutfencing =3
    pub access_mode: Option<i32>,
    /// Whether to block if the internal pending queue, whose size is configured by
    /// [`crate::client::PulsarBuilder::with_outbound_channel_size`] is full, when awaiting
    /// [`Producer::send_non_blocking`]. (default: false)
    pub block_queue_if_full: bool,
}

impl ProducerOptions {
    fn enabled_batching(&self) -> bool {
        match self.batch_size {
            Some(batch_size) => batch_size > 1,
            None => self.batch_byte_size.is_some() || self.batch_timeout.is_some(),
        }
    }
}

/// Wrapper structure that manges multiple producers at once, creating them as needed
/// ```rust,no_run
/// use pulsar::{Pulsar, TokioExecutor};
///
/// # async fn test() -> Result<(), pulsar::Error> {
/// # let addr = "pulsar://127.0.0.1:6650";
/// # let topic = "topic";
/// # let message = "data".to_owned();
/// let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
/// let mut producer = pulsar.producer().with_name("name").build_multi_topic();
/// let send_1 = producer.send_non_blocking(topic, &message).await?;
/// let send_2 = producer.send_non_blocking(topic, &message).await?;
/// send_1.await?;
/// send_2.await?;
/// # Ok(())
/// # }
/// ```
pub struct MultiTopicProducer<Exe: Executor> {
    client: Pulsar<Exe>,
    producers: BTreeMap<String, Producer<Exe>>,
    options: ProducerOptions,
    name: Option<String>,
}

impl<Exe: Executor> MultiTopicProducer<Exe> {
    /// producer options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn options(&self) -> &ProducerOptions {
        &self.options
    }

    /// list topics currently handled by this producer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topics(&self) -> Vec<String> {
        self.producers.keys().cloned().collect()
    }

    /// stops the producer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close_producer<S: Into<String>>(&mut self, topic: S) -> Result<(), Error> {
        let partitions = self.client.lookup_partitioned_topic(topic).await?;
        for (topic, _) in partitions {
            self.producers.remove(&topic);
        }
        Ok(())
    }

    /// sends one message on a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    #[deprecated = "instead use send_non_blocking"]
    pub async fn send<T: SerializeMessage + Sized, S: Into<String>>(
        &mut self,
        topic: S,
        message: T,
    ) -> Result<SendFuture, Error> {
        let fut = self.send_non_blocking(topic, message).await?;
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(fut.await);
        Ok(SendFuture(rx))
    }

    /// sends one message on a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_non_blocking<T: SerializeMessage + Sized, S: Into<String>>(
        &mut self,
        topic: S,
        message: T,
    ) -> Result<SendFuture, Error> {
        let message = T::serialize_message(message)?;
        let topic = topic.into();
        if !self.producers.contains_key(&topic) {
            let mut builder = self
                .client
                .producer()
                .with_topic(&topic)
                .with_options(self.options.clone());
            if let Some(name) = &self.name {
                builder = builder.with_name(name.clone());
            }
            let producer = builder.build().await?;
            self.producers.insert(topic.clone(), producer);
        }

        let producer = self.producers.get_mut(&topic).unwrap();
        producer.send_non_blocking(message).await
    }

    /// sends a list of messages on a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    #[deprecated = "instead use send_all_non_blocking"]
    pub async fn send_all<'a, 'b, T, S, I>(
        &mut self,
        topic: S,
        messages: I,
    ) -> Result<Vec<SendFuture>, Error>
    where
        'b: 'a,
        T: 'b + SerializeMessage + Sized,
        I: IntoIterator<Item = T>,
        S: Into<String>,
    {
        let topic: String = topic.into();
        let mut futs = vec![];
        for message in messages {
            #[allow(deprecated)]
            let fut = self.send(&topic, message).await?;
            futs.push(fut);
        }
        Ok(futs)
    }

    /// sends a list of messages on a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_all_non_blocking<'a, 'b, T, S, I>(
        &mut self,
        topic: S,
        messages: I,
    ) -> Result<Vec<SendFuture>, Error>
    where
        'b: 'a,
        T: 'b + SerializeMessage + Sized,
        I: IntoIterator<Item = T>,
        S: Into<String>,
    {
        let topic = topic.into();
        let mut sends = Vec::new();
        for msg in messages {
            sends.push(self.send_non_blocking(&topic, msg).await);
        }
        // TODO determine whether to keep this approach or go with the partial send, but more mem
        // friendly lazy approach. serialize all messages before sending to avoid a partial
        // send
        if sends.iter().all(|s| s.is_ok()) {
            Ok(sends.into_iter().map(|s| s.unwrap()).collect())
        } else {
            Err(ProducerError::PartialSend(sends).into())
        }
    }
}

/// a producer for a single topic
pub struct Producer<Exe: Executor> {
    inner: ProducerInner<Exe>,
}

impl<Exe: Executor> Producer<Exe> {
    /// creates a producer builder from a client instance
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn builder(pulsar: &Pulsar<Exe>) -> ProducerBuilder<Exe> {
        ProducerBuilder::new(pulsar)
    }

    /// this producer's topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topic(&self) -> &str {
        match &self.inner {
            ProducerInner::Single(p) => p.topic(),
            ProducerInner::Partitioned(p) => &p.topic,
        }
    }

    /// list of partitions for this producer's topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn partitions(&self) -> Option<Vec<String>> {
        match &self.inner {
            ProducerInner::Single(_) => None,
            ProducerInner::Partitioned(p) => {
                Some(p.producers.iter().map(|p| p.topic().to_owned()).collect())
            }
        }
    }

    /// configuration options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn options(&self) -> &ProducerOptions {
        match &self.inner {
            ProducerInner::Single(p) => p.options(),
            ProducerInner::Partitioned(p) => &p.options,
        }
    }

    /// creates a message builder
    ///
    /// the created message will ber sent by this producer in [MessageBuilder::send]
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn create_message<'a>(&'a mut self) -> MessageBuilder<'a, (), Exe> {
        MessageBuilder::new(self)
    }

    /// test that the broker connections are still valid
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connection(&self) -> Result<(), Error> {
        match &self.inner {
            ProducerInner::Single(p) => p.check_connection().await,
            ProducerInner::Partitioned(p) => {
                try_join_all(p.producers.iter().map(|p| p.check_connection()))
                    .await
                    .map(drop)
            }
        }
    }

    /// Sends a message
    ///
    /// this function returns a `SendFuture` because the receipt can come long after
    /// this function was called, for various reasons:
    /// - the message was sent successfully but Pulsar did not send the receipt yet
    /// - the producer is batching messages, so this function must return immediately, and the
    ///   receipt will come when the batched messages are actually sent
    ///
    /// If [`ProducerOptions::block_queue_if_full`] is false (by default) and the internal pending
    /// queue is full, which means the send rate is too fast,
    /// [`crate::error::ConnectionError::SlowDown`] will be returned. You should handle the error
    /// like:
    ///
    /// ```rust,no_run
    /// match producer.send_non_blocking("msg").await {
    ///     Ok(future) => { /* handle the send future */ }
    ///     Err(Error::Producer(ProducerError::Connection(ConnectionError::SlowDown))) => {
    ///         /* wait for a while and resent */
    ///     }
    ///     Err(e) => { /* handle other errors */ }
    /// }
    /// ```
    ///
    /// Usage:
    ///
    /// ```rust,no_run
    /// # async fn run(mut producer: pulsar::Producer<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let f1 = producer.send_non_blocking("hello").await?;
    /// let f2 = producer.send_non_blocking("world").await?;
    /// let receipt1 = f1.await?;
    /// let receipt2 = f2.await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_non_blocking<T: SerializeMessage + Sized>(
        &mut self,
        message: T,
    ) -> Result<SendFuture, Error> {
        match &mut self.inner {
            ProducerInner::Single(p) => p.send(message).await,
            ProducerInner::Partitioned(p) => p.next().send(message).await,
        }
    }

    /// Sends a message
    ///
    /// this function is similar to send_non_blocking then waits the returned `SendFuture`
    /// for the receipt.
    ///
    /// It returns the returned receipt in another `SendFuture` to be backward compatible.
    ///
    /// It is deprecated, and users should instread use send_non_blocking. Users should await the
    /// returned `SendFuture` if blocking is needed.
    ///
    /// Usage:
    ///
    /// ```rust,no_run
    /// # async fn run(mut producer: pulsar::Producer<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let f1 = producer.send_non_blocking("hello").await?;
    /// let f2 = producer.send_non_blocking("world").await?;
    /// let receipt1 = f1.await?;
    /// let receipt2 = f2.await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    #[deprecated = "instead use send_non_blocking"]
    pub async fn send<T: SerializeMessage + Sized>(
        &mut self,
        message: T,
    ) -> Result<SendFuture, Error> {
        let fut = self.send_non_blocking(message).await?;
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(fut.await);
        Ok(SendFuture(rx))
    }

    /// sends a list of messages
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_all<T, I>(&mut self, messages: I) -> Result<Vec<SendFuture>, Error>
    where
        T: SerializeMessage,
        I: IntoIterator<Item = T>,
    {
        let producer = match &mut self.inner {
            ProducerInner::Single(p) => p,
            ProducerInner::Partitioned(p) => p.next(),
        };
        let mut sends = Vec::new();
        for message in messages {
            sends.push(producer.send(message).await);
        }
        if sends.iter().all(|s| s.is_ok()) {
            Ok(sends.into_iter().map(|s| s.unwrap()).collect())
        } else {
            Err(ProducerError::PartialSend(sends).into())
        }
    }

    /// sends the current batch of messages
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_batch(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            ProducerInner::Single(p) => p.send_batch().await,
            ProducerInner::Partitioned(p) => {
                try_join_all(p.producers.iter_mut().map(|p| p.send_batch()))
                    .await
                    .map(drop)
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn send_raw(&mut self, message: ProducerMessage) -> Result<SendFuture, Error> {
        match &mut self.inner {
            ProducerInner::Single(p) => p.send_raw(message).await,
            ProducerInner::Partitioned(p) => p.next().send_raw(message).await,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            ProducerInner::Single(producer) => producer.close().await,
            ProducerInner::Partitioned(p) => try_join_all(p.producers.iter().map(|p| p.close()))
                .await
                .map(drop),
        }
    }
}

enum ProducerInner<Exe: Executor> {
    Single(TopicProducer<Exe>),
    Partitioned(PartitionedProducer<Exe>),
}

struct PartitionedProducer<Exe: Executor> {
    // Guaranteed to be non-empty
    producers: VecDeque<TopicProducer<Exe>>,
    topic: String,
    options: ProducerOptions,
}

impl<Exe: Executor> PartitionedProducer<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn next(&mut self) -> &mut TopicProducer<Exe> {
        self.producers.rotate_left(1);
        self.producers.front_mut().unwrap()
    }
}

/// a producer is used to publish messages on a topic
struct TopicProducer<Exe: Executor> {
    client: Pulsar<Exe>,
    connection: Arc<Connection<Exe>>,
    id: ProducerId,
    name: ProducerName,
    topic: String,
    batch: RefCell<Option<Batch>>,
    sequence_id: SerialId,
    compression: Option<Compression>,
    options: ProducerOptions,
}

impl<Exe: Executor> TopicProducer<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn new<S: Into<String>>(
        client: Pulsar<Exe>,
        addr: BrokerAddress,
        topic: S,
        name: Option<String>,
        options: ProducerOptions,
    ) -> Result<Self, Error> {
        static PRODUCER_ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

        let topic = topic.into();
        let producer_id = PRODUCER_ID_GENERATOR.fetch_add(1, Ordering::SeqCst);
        let sequence_id = SerialId::new();

        let topic = topic.clone();
        let compression = options.compression.clone();
        let mut connection = client.manager.get_connection(&addr).await?;

        let producer_name = retry_create_producer(
            &client,
            &mut connection,
            addr,
            &topic,
            producer_id,
            name,
            &options,
        )
        .await?;

        if !options.enabled_batching() {
            return Ok(TopicProducer {
                client,
                connection,
                id: producer_id,
                name: producer_name,
                topic,
                sequence_id,
                compression,
                options,
                batch: RefCell::new(None),
            });
        }
        let executor = client.executor.clone();
        let batch_storage = BatchStorage {
            max_size: options.batch_size,
            max_byte_size: options.batch_byte_size,
            timeout: options.batch_timeout,
            size: 0,
            storage: match options.batch_size {
                Some(batch_size) => VecDeque::with_capacity(batch_size as usize),
                None => VecDeque::new(),
            },
        };
        // the message should be received quickly, so a small buffer is okay
        let (msg_sender, msg_receiver) = mpsc::channel::<BatchItem>(10);
        let executor_clone = executor.clone();
        let (batch_sender, batch_receiver) = mpsc::channel::<Vec<BatchItem>>(1);
        let (close_sender, close_receiver) = oneshot::channel::<()>();

        let _ = executor.spawn(Box::pin(batch_process_loop(
            producer_id,
            batch_storage,
            msg_receiver,
            close_sender,
            batch_sender,
            executor_clone,
        )));
        let _ = executor.spawn(Box::pin(message_send_loop(
            batch_receiver,
            client.clone(),
            connection.clone(),
            topic.clone(),
            producer_id,
            producer_name.clone(),
            sequence_id.clone(),
            options.clone(),
        )));

        Ok(TopicProducer {
            client,
            connection,
            id: producer_id,
            name: producer_name,
            topic,
            batch: RefCell::new(Some(Batch {
                msg_sender,
                close_receiver,
            })),
            sequence_id,
            compression,
            options,
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn topic(&self) -> &str {
        &self.topic
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn options(&self) -> &ProducerOptions {
        &self.options
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn check_connection(&self) -> Result<(), Error> {
        self.connection.sender().send_ping().await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send<T: SerializeMessage + Sized>(&mut self, message: T) -> Result<SendFuture, Error> {
        match T::serialize_message(message) {
            Ok(message) => self.send_raw(message.into()).await,
            Err(e) => Err(e),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_batch(&mut self) -> Result<(), Error> {
        let mut msg_sender = {
            let mut mut_ref = self.batch.borrow_mut();
            mut_ref.as_mut().map(|batch| batch.msg_sender.clone())
        };
        match &mut msg_sender {
            Some(msg_sender) => {
                let (tx, rx) = oneshot::channel::<()>();
                let item = BatchItem::Flush(tx);
                let _ = msg_sender.send(item).await;
                let _ = rx.await; // ignore any error
                Ok(())
            }
            None if self.options.enabled_batching() => Err(ProducerError::Closed.into()),
            _ => Err(ProducerError::Custom("not a batching producer".to_string()).into()),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn send_raw(&mut self, message: ProducerMessage) -> Result<SendFuture, Error> {
        let mut msg_sender = {
            let mut mut_ref = self.batch.borrow_mut();
            mut_ref.as_mut().map(|batch| batch.msg_sender.clone())
        };
        let (tx, rx) = oneshot::channel();
        match &mut msg_sender {
            Some(msg_sender) => {
                let properties = message
                    .properties
                    .into_iter()
                    .map(|(key, value)| proto::KeyValue { key, value })
                    .collect();
                let batched = BatchedMessage {
                    metadata: proto::SingleMessageMetadata {
                        properties,
                        partition_key: message.partition_key,
                        ordering_key: message.ordering_key,
                        payload_size: message.payload.len() as i32,
                        event_time: message.event_time,
                        ..Default::default()
                    },
                    payload: message.payload,
                };
                let item = BatchItem::SingleMessage(tx, batched);
                msg_sender.send(item).await.map_err(|e| {
                    Error::Producer(ProducerError::Custom(format!(
                        "failed to send message to batch_process_loop: {e}"
                    )))
                })?;
            }
            None if self.options.enabled_batching() => {
                return Err(ProducerError::Closed.into());
            }
            _ => {
                let compressed_message = compress_message(message, &self.compression)?;
                let fut = send_message(
                    &self.client,
                    &self.topic,
                    &mut self.connection,
                    compressed_message,
                    self.id,
                    &self.name,
                    &self.sequence_id,
                    &self.options,
                )
                .await?;
                self.client
                    .executor
                    .spawn(Box::pin(async move {
                        let _ = tx.send(fut.await);
                    }))
                    .map_err(|_| Error::Executor)?;
            }
        };
        Ok(SendFuture(rx))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close(&self) -> Result<(), Error> {
        let mut batch = {
            let mut mut_ref = self.batch.borrow_mut();
            mut_ref.take()
        };
        match &mut batch {
            None => {
                self.connection.sender().close_producer(self.id).await?;
            }
            Some(batch) if self.options.enabled_batching() => {
                batch.msg_sender.close_channel();
                let close_receiver = &mut batch.close_receiver;
                let _ = close_receiver.await;
            }
            _ => {
                warn!(
                    "close called multiple times on producer {} for topic {}",
                    self.id, self.topic
                );
            }
        };
        Ok(())
    }
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
fn compress_message(
    mut message: ProducerMessage,
    compression: &Option<Compression>,
) -> Result<ProducerMessage, Error> {
    let compressed_message = match compression {
        None | Some(Compression::None) => message,
        #[cfg(feature = "lz4")]
        Some(Compression::Lz4(compression)) => {
            let compressed_payload: Vec<u8> =
                lz4::block::compress(&message.payload[..], Some(compression.mode), false)
                    .map_err(ProducerError::Io)?;

            message.uncompressed_size = Some(message.payload.len() as u32);
            message.payload = compressed_payload;
            message.compression = Some(proto::CompressionType::Lz4.into());
            message
        }
        #[cfg(feature = "flate2")]
        Some(Compression::Zlib(compression)) => {
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), compression.level);
            e.write_all(&message.payload[..])
                .map_err(ProducerError::Io)?;
            let compressed_payload = e.finish().map_err(ProducerError::Io)?;

            message.uncompressed_size = Some(message.payload.len() as u32);
            message.payload = compressed_payload;
            message.compression = Some(proto::CompressionType::Zlib.into());
            message
        }
        #[cfg(feature = "zstd")]
        Some(Compression::Zstd(compression)) => {
            let compressed_payload = zstd::encode_all(&message.payload[..], compression.level)
                .map_err(ProducerError::Io)?;
            message.uncompressed_size = Some(message.payload.len() as u32);
            message.payload = compressed_payload;
            message.compression = Some(proto::CompressionType::Zstd.into());
            message
        }
        #[cfg(feature = "snap")]
        Some(Compression::Snappy(..)) => {
            let mut compressed_payload = Vec::new();
            {
                let mut encoder = snap::write::FrameEncoder::new(&mut compressed_payload);
                encoder
                    .write_all(&message.payload[..])
                    .map_err(ProducerError::Io)?;
                encoder.flush().map_err(ProducerError::Io)?;
            }

            message.uncompressed_size = Some(message.payload.len() as u32);
            message.payload = compressed_payload;
            message.compression = Some(proto::CompressionType::Snappy.into());
            message
        }
    };
    Ok(compressed_message)
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
async fn send_message<Exe>(
    client: &Pulsar<Exe>,
    topic: &String,
    connection: &mut Arc<Connection<Exe>>,
    message: ProducerMessage,
    producer_id: ProducerId,
    producer_name: &ProducerName,
    sequence_id: &SerialId,
    options: &ProducerOptions,
) -> Result<impl Future<Output = Result<CommandSendReceipt, Error>>, Error>
where
    Exe: Executor,
{
    loop {
        let message = message.clone();
        match connection
            .sender()
            .send(
                producer_id,
                producer_name.clone(),
                sequence_id.get(),
                message,
                options.block_queue_if_full,
            )
            .await
        {
            Ok(fut) => {
                let fut = async move {
                    let res = fut.await;
                    res.map_err(|e| {
                        error!("wait send receipt got error: {:?}", e);
                        Error::Producer(ProducerError::Connection(e))
                    })
                };
                return Ok(fut);
            }
            Err(ConnectionError::Disconnected) => {}
            Err(ConnectionError::Io(e)) => {
                if e.kind() != std::io::ErrorKind::TimedOut {
                    error!("send_message got io error: {:?}", e);
                    return Err(ProducerError::Connection(ConnectionError::Io(e)).into());
                }
            }
            Err(e) => {
                error!("send_message got error: {:?}", e);
                return Err(ProducerError::Connection(e).into());
            }
        };

        error!(
            "send_message: connection {} disconnected, reconnecting producer for topic: {}",
            connection.id(),
            &topic
        );

        if let Err(e) = connection.sender().close_producer(producer_id).await {
            error!(
                "could not close producer {:?}({}) for topic {}: {:?}",
                producer_name, producer_id, &topic, e
            );
        }

        let broker_address = client.lookup_topic(topic).await?;

        let _producer_name = retry_create_producer(
            client,
            connection,
            broker_address,
            topic,
            producer_id,
            Some(producer_name.clone()),
            options,
        )
        .await?;
    }
}

impl<Exe: Executor> std::ops::Drop for TopicProducer<Exe> {
    fn drop(&mut self) {
        let conn = self.connection.clone();
        let id = self.id;
        let name = self.name.clone();
        let topic = self.topic.clone();
        let mut batch = {
            let mut mut_ref = self.batch.borrow_mut();
            mut_ref.take()
        };
        if let Some(batch) = &mut batch {
            batch.msg_sender.close_channel();
        }
        let _ = self.client.executor.spawn(Box::pin(async move {
            if let Err(e) = conn.sender().close_producer(id).await {
                error!(
                    "could not close producer {:?}({}) for topic {}: {:?}",
                    name, id, topic, e
                );
            }
        }));
    }
}

/// Helper structure to prepare a producer
///
/// generated from [Pulsar::producer]
#[derive(Clone)]
pub struct ProducerBuilder<Exe: Executor> {
    pulsar: Pulsar<Exe>,
    topic: Option<String>,
    name: Option<String>,
    producer_options: Option<ProducerOptions>,
}

impl<Exe: Executor> ProducerBuilder<Exe> {
    /// creates a new ProducerBuilder from a client
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(pulsar: &Pulsar<Exe>) -> Self {
        ProducerBuilder {
            pulsar: pulsar.clone(),
            topic: None,
            name: None,
            producer_options: None,
        }
    }

    /// sets the producer's topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topic<S: Into<String>>(mut self, topic: S) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// sets the producer's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_name<S: Into<String>>(mut self, name: S) -> Self {
        self.name = Some(name.into());
        self
    }

    /// configuration options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_options(mut self, options: ProducerOptions) -> Self {
        self.producer_options = Some(options);
        self
    }

    /// creates a new producer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn build(self) -> Result<Producer<Exe>, Error> {
        let ProducerBuilder {
            pulsar,
            topic,
            name,
            producer_options,
        } = self;
        let topic = topic.ok_or_else(|| Error::Custom("topic not set".to_string()))?;
        let options = producer_options.unwrap_or_default();

        let producers: Vec<TopicProducer<Exe>> = try_join_all(
            pulsar
                .lookup_partitioned_topic(&topic)
                .await?
                .into_iter()
                .map(|(topic, addr)| {
                    let name = name.clone();
                    let options = options.clone();
                    let pulsar = pulsar.clone();
                    async move {
                        let producer =
                            TopicProducer::new(pulsar, addr, topic, name, options).await?;
                        Ok::<TopicProducer<Exe>, Error>(producer)
                    }
                }),
        )
        .await?;

        let producer = match producers.len() {
            0 => {
                return Err(Error::Custom(format!(
                    "Unexpected error: Partition lookup returned no topics for {topic}"
                )));
            }
            1 => ProducerInner::Single(producers.into_iter().next().unwrap()),
            _ => {
                let mut producers = VecDeque::from(producers);
                // write to topic-1 first
                producers.rotate_right(1);
                ProducerInner::Partitioned(PartitionedProducer {
                    producers,
                    topic,
                    options,
                })
            }
        };

        Ok(Producer { inner: producer })
    }

    /// creates a new [MultiTopicProducer]
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn build_multi_topic(self) -> MultiTopicProducer<Exe> {
        MultiTopicProducer {
            client: self.pulsar,
            producers: Default::default(),
            options: self.producer_options.unwrap_or_default(),
            name: self.name,
        }
    }
}

struct BatchStorage {
    max_size: Option<u32>,
    max_byte_size: Option<usize>,
    timeout: Option<Duration>,
    size: usize,
    storage: VecDeque<BatchItem>,
}

impl BatchStorage {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn push_back(&mut self, item: BatchItem) {
        if let BatchItem::SingleMessage(_, batched_msg) = &item {
            self.size += batched_msg.metadata.payload_size as usize;
        }
        self.storage.push_back(item);
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn get_messages(&mut self) -> Vec<BatchItem> {
        self.size = 0;
        self.storage.drain(..).collect()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn ready_to_flush(&self) -> bool {
        if let Some(max_size) = self.max_size {
            if self.storage.len() >= max_size as usize {
                return true;
            }
        }
        if let Some(max_byte_size) = self.max_byte_size {
            if self.size >= max_byte_size {
                return true;
            }
        }
        matches!(self.storage.back(), Some(BatchItem::Flush(_)))
    }
}

enum BatchItem {
    SingleMessage(
        oneshot::Sender<Result<CommandSendReceipt, Error>>,
        BatchedMessage,
    ),
    Flush(oneshot::Sender<()>),
}

struct Batch {
    // sends a message or trigger a flush
    msg_sender: mpsc::Sender<BatchItem>,
    // receives the notification when `bath_process_loop` is closed
    close_receiver: oneshot::Receiver<()>,
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
async fn batch_process_loop(
    producer_id: ProducerId,
    mut batch_storage: BatchStorage,
    mut msg_receiver: mpsc::Receiver<BatchItem>,
    close_sender: oneshot::Sender<()>,
    mut batch_sender: mpsc::Sender<Vec<BatchItem>>,
    executor: impl Executor,
) {
    let mut recv_future = msg_receiver.next();
    let mut timer_future: Pin<Box<dyn Future<Output = ()> + Send + 'static>> =
        Box::pin(future::pending());

    let flush = async |batch_sender: &mut mpsc::Sender<Vec<BatchItem>>,
                       messages: Vec<BatchItem>| {
        if !messages.is_empty() {
            let _ = batch_sender.send(messages).await;
        }
    };

    loop {
        match future::select(recv_future, timer_future).await {
            Either::Left((Some(batch_item), previous_timer_future)) => {
                batch_storage.push_back(batch_item);
                if batch_storage.ready_to_flush() {
                    flush(&mut batch_sender, batch_storage.get_messages()).await;
                    timer_future = Box::pin(future::pending());
                } else {
                    timer_future = match batch_storage.timeout {
                        Some(timeout) if batch_storage.storage.len() == 1 => {
                            Box::pin(executor.delay(timeout))
                        }
                        _ => previous_timer_future,
                    };
                }
                recv_future = msg_receiver.next();
            }
            Either::Left((None, _)) => {
                let count = batch_storage.storage.len();
                if count > 0 {
                    warn!("producer {}'s batch_process_loop exits when there are {} messages not flushed",
                        producer_id, count);
                    for item in batch_storage.get_messages() {
                        if let BatchItem::SingleMessage(tx, _) = item {
                            let _ = tx.send(Err(Error::Producer(ProducerError::Closed)));
                        }
                    }
                } else {
                    debug!("producer {producer_id}'s batch_process_loop: channel closed, exiting");
                }
                let _ = close_sender.send(()).inspect_err(|e| {
                    warn!(
                        "{producer_id} could not notify the batch_process_loop is closed: {:?}, the producer might be dropped without closing",
                        e
                    );
                });
                break;
            }
            Either::Right((_, previous_recv_future)) => {
                if batch_storage.timeout.is_some() {
                    flush(&mut batch_sender, batch_storage.get_messages()).await;
                }
                timer_future = Box::pin(future::pending());
                recv_future = previous_recv_future;
            }
        }
    }
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
async fn message_send_loop<Exe>(
    mut msg_receiver: mpsc::Receiver<Vec<BatchItem>>,
    client: Pulsar<Exe>,
    mut connection: Arc<Connection<Exe>>,
    topic: String,
    producer_id: ProducerId,
    producer_name: ProducerName,
    sequence_id: SerialId,
    options: ProducerOptions,
) where
    Exe: Executor,
{
    loop {
        match msg_receiver.next().await {
            Some(mut batch_items) => {
                if batch_items.is_empty() {
                    error!(
                        "producer {}'s message_send_loop received an empty batch unexpectedly",
                        producer_id
                    );
                    continue;
                }
                let mut payload: Vec<u8> = Vec::new();
                let mut receipts = Vec::new();

                let flush_tx = {
                    if let Some(BatchItem::Flush(_)) = batch_items.last() {
                        if let BatchItem::Flush(tx) = batch_items.pop().unwrap() {
                            Some(tx)
                        } else {
                            unreachable!()
                        }
                    } else {
                        None
                    }
                };
                let counter = batch_items.len();
                for item in batch_items {
                    if let BatchItem::SingleMessage(tx, batched_msg) = item {
                        receipts.push(tx);
                        batched_msg.serialize(&mut payload);
                    } else {
                        error!(
                            "producer {}'s message_send_loop received a Flush item unexpectedly",
                            producer_id
                        );
                    }
                }

                let message = ProducerMessage {
                    payload,
                    num_messages_in_batch: Some(counter as i32),
                    ..Default::default()
                };

                trace!("sending a batched message of size {}", counter);

                let send = async || {
                    let compressed_message = compress_message(message, &options.compression)?;
                    send_message(
                        &client,
                        &topic,
                        &mut connection,
                        compressed_message,
                        producer_id,
                        &producer_name,
                        &sequence_id,
                        &options,
                    )
                    .await?
                    .await
                };
                match send().await {
                    Ok(receipt) => {
                        for (batch_index, tx) in receipts.into_iter().enumerate() {
                            let mut receipt = receipt.clone();
                            if let Some(msg_id) = &mut receipt.message_id {
                                msg_id.batch_index = Some(batch_index as i32);
                                msg_id.batch_size = Some(counter as i32);
                            }
                            let _ = tx.send(Ok(receipt));
                        }
                        if let Some(flush_tx) = flush_tx {
                            let _ = flush_tx.send(());
                        }
                    }
                    Err(e) => {
                        let error = Arc::new(e);
                        for tx in receipts {
                            let _ =
                                tx.send(Err(Error::Producer(ProducerError::Batch(error.clone()))));
                        }
                    }
                };
            }
            None => {
                debug!("producer {producer_id} message_send_loop: channel closed, exiting");
                break;
            }
        }
    }
}

/// Helper structure to prepare a message
///
/// generated with [Producer::create_message]
pub struct MessageBuilder<'a, T, Exe: Executor> {
    producer: &'a mut Producer<Exe>,
    properties: HashMap<String, String>,
    partition_key: Option<String>,
    ordering_key: Option<Vec<u8>>,
    deliver_at_time: Option<i64>,
    event_time: Option<u64>,
    content: T,
}

impl<'a, Exe: Executor> MessageBuilder<'a, (), Exe> {
    /// creates a message builder from an existing producer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(producer: &'a mut Producer<Exe>) -> Self {
        MessageBuilder {
            producer,
            properties: HashMap::new(),
            partition_key: None,
            ordering_key: None,
            deliver_at_time: None,
            event_time: None,
            content: (),
        }
    }
}

impl<'a, T, Exe: Executor> MessageBuilder<'a, T, Exe> {
    /// sets the message's content
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_content<C>(self, content: C) -> MessageBuilder<'a, C, Exe> {
        MessageBuilder {
            producer: self.producer,
            properties: self.properties,
            partition_key: self.partition_key,
            ordering_key: self.ordering_key,
            deliver_at_time: self.deliver_at_time,
            event_time: self.event_time,
            content,
        }
    }

    /// sets the message's partition key
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_partition_key<S: Into<String>>(mut self, partition_key: S) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    /// sets the message's ordering key for key_shared subscription
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_ordering_key<S: Into<Vec<u8>>>(mut self, ordering_key: S) -> Self {
        self.ordering_key = Some(ordering_key.into());
        self
    }

    /// sets the message's partition key
    ///
    /// this is the same as `with_partition_key`, this method is added for
    /// more consistency with other clients
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_key<S: Into<String>>(mut self, partition_key: S) -> Self {
        self.partition_key = Some(partition_key.into());
        self
    }

    /// sets a user defined property
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_property<S1: Into<String>, S2: Into<String>>(mut self, key: S1, value: S2) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// delivers the message at this date
    /// Note: The delayed and scheduled message attributes are only applied to shared subscription.
    /// With other subscription types, the messages will still be delivered immediately.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn deliver_at(mut self, date: SystemTime) -> Result<Self, std::time::SystemTimeError> {
        self.deliver_at_time = Some(date.duration_since(UNIX_EPOCH)?.as_millis() as i64);
        Ok(self)
    }

    /// delays message deliver with this duration
    /// Note: The delayed and scheduled message attributes are only applied to shared subscription.
    /// With other subscription types, the messages will still be delivered immediately.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn delay(mut self, delay: Duration) -> Result<Self, std::time::SystemTimeError> {
        let date = SystemTime::now() + delay;
        self.deliver_at_time = Some(date.duration_since(UNIX_EPOCH)?.as_millis() as i64);
        Ok(self)
    }

    // set the event time for a given message
    // By default, messages don't have an event time associated, while the publish
    // time will be be always present.
    // Set the event time to explicitly declare the time
    // that the event "happened", as opposed to when the message is being published.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn event_time(mut self, event_time: u64) -> Self {
        self.event_time = Some(event_time);
        self
    }
}

impl<T: SerializeMessage + Sized, Exe: Executor> MessageBuilder<'_, T, Exe> {
    /// sends the message through the producer that created it
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    #[deprecated = "instead use send_non_blocking"]
    pub async fn send(self) -> Result<SendFuture, Error> {
        let fut = self.send_non_blocking().await?;
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(fut.await);
        Ok(SendFuture(rx))
    }

    /// sends the message through the producer that created it
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send_non_blocking(self) -> Result<SendFuture, Error> {
        let MessageBuilder {
            producer,
            properties,
            partition_key,
            ordering_key,
            content,
            deliver_at_time,
            event_time,
        } = self;

        let mut message = T::serialize_message(content)?;
        message.properties = properties;
        message.partition_key = partition_key;
        message.ordering_key = ordering_key;
        message.event_time = event_time;

        let mut producer_message: ProducerMessage = message.into();
        producer_message.deliver_at_time = deliver_at_time;
        producer.send_raw(producer_message).await
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use log::LevelFilter;

    use super::*;
    use crate::{tests::TEST_LOGGER, TokioExecutor};

    #[test]
    fn send_future_errors_when_sender_dropped() {
        let (tx, rx) = futures::channel::oneshot::channel::<Result<CommandSendReceipt, Error>>();
        // Drop the sender immediately to simulate an unexpected disconnect:
        drop(tx);

        let fut = SendFuture(rx);
        let err = block_on(fut).expect_err("expected an error when sender is dropped");

        // It should be mapped to a ProducerError::Custom inside Error::Producer
        match err {
            Error::Producer(ProducerError::Custom(msg)) => {
                assert!(
                    msg.contains("unexpectedly disconnected"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("unexpected error variant: {:?}", other),
        }
    }

    #[test]
    fn message_converts_into_producer_message() {
        let mut props = HashMap::new();
        props.insert("a".to_string(), "1".to_string());
        props.insert("b".to_string(), "2".to_string());

        let m = Message {
            payload: b"hello".to_vec(),
            properties: props.clone(),
            partition_key: Some("key".into()),
            ordering_key: Some(vec![1, 2, 3]),
            replicate_to: vec!["r1".into(), "r2".into()],
            event_time: Some(42),
            schema_version: Some(vec![9, 9]),
            deliver_at_time: Some(123456789),
        };

        let pm: ProducerMessage = m.clone().into();

        assert_eq!(pm.payload, m.payload);
        assert_eq!(pm.properties, m.properties);
        assert_eq!(pm.partition_key, m.partition_key);
        assert_eq!(pm.ordering_key, m.ordering_key);
        assert_eq!(pm.replicate_to, m.replicate_to);
        assert_eq!(pm.event_time, m.event_time);
        assert_eq!(pm.schema_version, m.schema_version);
        assert_eq!(pm.deliver_at_time, m.deliver_at_time);

        // And defaults that the producer fills later:
        assert!(pm.num_messages_in_batch.is_none());
        assert!(pm.compression.is_none());
        assert!(pm.uncompressed_size.is_none());
    }

    #[tokio::test]
    async fn block_if_queue_full() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
            .with_outbound_channel_size(3)
            .build()
            .await
            .unwrap();
        let mut producer = pulsar
            .producer()
            .with_topic(format!("block_queue_if_full_{}", rand::random::<u16>()))
            .build()
            .await
            .unwrap();
        let mut send_results = Vec::with_capacity(10);
        for i in 0..10 {
            send_results.push(producer.send_non_blocking(format!("msg-{i}")).await);
        }
        let mut failed_indexes = vec![];
        for (i, result) in send_results.into_iter().enumerate() {
            match result {
                Ok(_) => {}
                Err(Error::Producer(ProducerError::Connection(ConnectionError::SlowDown))) => {
                    failed_indexes.push(i);
                }
                Err(e) => {
                    error!("failed to send {}: {}", i, e);
                    panic!();
                }
            }
        }
        info!("Messages failed due to SlowDown: {:?}", &failed_indexes);
        assert!(!failed_indexes.is_empty());

        let mut producer = pulsar
            .producer()
            .with_topic(format!("block_queue_if_full_{}", rand::random::<u16>()))
            .with_options(ProducerOptions {
                block_queue_if_full: true,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let mut send_results = Vec::with_capacity(10);
        for i in 0..10 {
            send_results.push(producer.send_non_blocking(format!("msg-{i}")).await);
        }
        for (i, result) in send_results.into_iter().enumerate() {
            match result {
                Ok(_) => {}
                Err(e) => {
                    error!("failed to send {}: {}", i, e);
                    panic!();
                }
            }
        }
    }
}
