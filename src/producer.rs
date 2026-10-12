//! Message publication
use std::{
    collections::{btree_map::Entry, BTreeMap, HashMap, VecDeque},
    io::Write,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::Engine;
use futures::{
    channel::{mpsc, oneshot},
    future::{self, try_join_all, Either},
    lock::{Mutex, MutexGuard},
    pin_mut,
    task::{Context, Poll},
    Future, SinkExt, StreamExt,
};
use rand::Rng;

use crate::{
    client::SerializeMessage,
    compression::Compression,
    connection::{Connection, SerialId},
    error::{ConnectionError, ProducerError, ServiceDiscoveryError},
    executor::Executor,
    message::{
        proto::{self, CommandSendReceipt, EncryptionKeys, Schema},
        BatchedMessage,
    },
    proto::CommandSuccess,
    retry_op::retry_create_producer,
    routing_policy::RoutingPolicy,
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
    /// whether [`partition_key`](Self::partition_key) is a Base64 encoding of raw key bytes
    pub partition_key_b64_encoded: ::std::option::Option<bool>,
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
    /// whether [`partition_key`](Self::partition_key) is Base64-encoded raw key bytes
    pub partition_key_b64_encoded: ::std::option::Option<bool>,
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
            partition_key_b64_encoded: m.partition_key_b64_encoded,
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
    pub routing_policy: Option<RoutingPolicy>,
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
        let producer = match self.producers.entry(topic) {
            Entry::Vacant(entry) => {
                let mut builder = self
                    .client
                    .producer()
                    .with_topic(entry.key())
                    .with_options(self.options.clone());
                if let Some(name) = &self.name {
                    builder = builder.with_name(name);
                }
                let producer = builder.build().await?;
                entry.insert(producer)
            }
            Entry::Occupied(entry) => entry.into_mut(),
        };

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
    /// use pulsar::error::{ConnectionError, Error, ProducerError};
    ///
    /// # async fn run(mut producer: pulsar::Producer<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// match producer.send_non_blocking("msg").await {
    ///     Ok(future) => { /* handle the send future */ }
    ///     Err(Error::Producer(ProducerError::Connection(ConnectionError::SlowDown))) => {
    ///         /* wait for a while and resent */
    ///     }
    ///     Err(e) => { /* handle other errors */ }
    /// }
    /// # Ok(())
    /// # }
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
        let serialized_message = T::serialize_message(message)?;
        match &mut self.inner {
            ProducerInner::Single(p) => p.send(serialized_message).await,
            ProducerInner::Partitioned(p) => {
                p.choose_partition(&serialized_message)
                    .send(serialized_message)
                    .await
            }
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
        let mut sends = Vec::new();
        for message in messages {
            let serialized_message = T::serialize_message(message)?;
            let producer = match &mut self.inner {
                ProducerInner::Single(p) => p,
                ProducerInner::Partitioned(p) => p.choose_partition(&serialized_message),
            };

            sends.push(producer.send(serialized_message).await);
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
    pub async fn close(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            ProducerInner::Single(producer) => producer.close().await,
            ProducerInner::Partitioned(p) => {
                try_join_all(p.producers.iter_mut().map(|p| p.close()))
                    .await
                    .map(drop)
            }
        }
    }
}

enum ProducerInner<Exe: Executor> {
    Single(TopicProducer<Exe>),
    Partitioned(PartitionedProducer<Exe>),
}

struct PartitionedProducer<Exe: Executor> {
    // Guaranteed to be non-empty
    producers: Vec<TopicProducer<Exe>>,
    last_used_producer_index: usize,
    topic: String,
    options: ProducerOptions,
}

impl<Exe: Executor> PartitionedProducer<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn get_next_round_robin_producer(&mut self) -> &mut TopicProducer<Exe> {
        let amount_of_producers = self.producers.len();
        self.last_used_producer_index += 1;
        if self.last_used_producer_index >= amount_of_producers {
            self.last_used_producer_index = 0;
        }
        self.producers
            .get_mut(self.last_used_producer_index)
            .unwrap()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn choose_partition(&mut self, message: &Message) -> &mut TopicProducer<Exe> {
        match &self.options.routing_policy {
            Some(RoutingPolicy::RoundRobin) => {
                // If the message has a partition key, use it
                if let Some(partition_key) = &message.partition_key {
                    let index = RoutingPolicy::compute_partition_index_for_key(
                        partition_key,
                        self.producers.len(),
                    );
                    return self.producers.get_mut(index).unwrap();
                }
                // If not, use round robin
                self.get_next_round_robin_producer()
            }
            Some(RoutingPolicy::Single) => self
                .producers
                .get_mut(self.last_used_producer_index)
                .unwrap(),
            Some(RoutingPolicy::Custom(policy)) => {
                let amount_of_producers = self.producers.len();
                self.producers
                    .get_mut(policy.route(message, amount_of_producers))
                    .unwrap()
            }
            None => self.get_next_round_robin_producer(),
        }
    }
}

/// a producer is used to publish messages on a topic
struct TopicProducer<Exe: Executor> {
    client: Pulsar<Exe>,
    connection: Arc<ProducerConnection<Exe>>,
    id: ProducerId,
    name: ProducerName,
    topic: String,
    batch: Option<Batch>,
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

        let topic = topic.clone();
        let compression = options.compression.clone();
        let mut connection = client.manager.get_connection(&addr).await?;

        let (producer_name, schema_version) = retry_create_producer(
            &client,
            &mut connection,
            addr,
            &topic,
            producer_id,
            name,
            &options,
        )
        .await?;
        let connection = Arc::new(ProducerConnection {
            client: client.clone(),
            topic: topic.clone(),
            id: producer_id,
            name: producer_name.clone(),
            options: options.clone(),
            sequence_id: SerialId::new(),
            pending: Default::default(),
            state: Mutex::new(ProducerState {
                connection,
                ready: true,
                schema_version,
            }),
        });

        if !options.enabled_batching() {
            return Ok(TopicProducer {
                client,
                connection,
                id: producer_id,
                name: producer_name,
                topic,
                compression,
                options,
                batch: None,
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
        let (close_sender, close_receiver) =
            oneshot::channel::<Result<CommandSuccess, ConnectionError>>();

        let _ = executor.spawn(Box::pin(batch_process_loop(
            producer_id,
            batch_storage,
            msg_receiver,
            batch_sender,
            executor_clone,
        )));
        let _ = executor.spawn(Box::pin(message_send_loop(
            batch_receiver,
            close_sender,
            connection.clone(),
        )));

        Ok(TopicProducer {
            client,
            connection,
            id: producer_id,
            name: producer_name,
            topic,
            batch: Some(Batch {
                msg_sender,
                close_receiver,
            }),
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
        let deadline = Instant::now() + self.client.operation_retry_options.operation_timeout;
        let connection = self
            .connection
            .lock_state(deadline)
            .await?
            .connection
            .clone();
        connection.sender().send_ping().await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send(&mut self, message: Message) -> Result<SendFuture, Error> {
        self.send_raw(message.into()).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_batch(&mut self) -> Result<(), Error> {
        match &mut self.batch.as_mut().map(|batch| &mut batch.msg_sender) {
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
        match &mut self.batch.as_mut().map(|batch| &mut batch.msg_sender) {
            Some(msg_sender) => {
                // Per-message schema_version is not supported by the Pulsar
                // protocol for batched messages. The schema_version is set
                // on the batch envelope when it is sent instead. Any
                // user-provided schema_version on individual messages is
                // structurally dropped here.
                let properties = message
                    .properties
                    .into_iter()
                    .map(|(key, value)| proto::KeyValue { key, value })
                    .collect();
                let batched = BatchedMessage {
                    metadata: proto::SingleMessageMetadata {
                        properties,
                        partition_key: message.partition_key,
                        partition_key_b64_encoded: message.partition_key_b64_encoded,
                        ordering_key: message.ordering_key,
                        payload_size: message.payload.len() as i32,
                        event_time: message.event_time,
                        ..Default::default()
                    },
                    payload: message.payload,
                };
                let (tx, rx) = oneshot::channel();
                let item = BatchItem::SingleMessage(tx, batched);
                msg_sender.send(item).await.map_err(|e| {
                    Error::Producer(ProducerError::Custom(format!(
                        "failed to send message to batch_process_loop: {e}"
                    )))
                })?;
                Ok(SendFuture(rx))
            }
            None if self.options.enabled_batching() => Err(ProducerError::Closed.into()),
            _ => {
                let compressed_message = compress_message(message, &self.compression)?;
                self.connection.send(compressed_message).await
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn close(&mut self) -> Result<(), Error> {
        match self.batch.take() {
            None => {
                self.connection.close().await?;
            }
            Some(mut batch) if self.options.enabled_batching() => {
                batch.msg_sender.close_channel();
                let close_receiver = &mut batch.close_receiver;
                return match close_receiver.await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => Err(Error::Producer(ProducerError::Connection(e))),
                    Err(_) => Err(Error::Producer(ProducerError::Closed)),
                };
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

/// The producer's connection and its messages waiting for a receipt, shared with the tasks
/// waiting for those receipts: when the broker closes the producer (e.g. on topic unload) or the
/// connection drops, they re-create it and resend the messages, even if the application stopped
/// sending. Like Java's `sendTimeout`, a message fails once `operation_timeout` has passed since
/// it was first sent, whatever the resends and re-creations.
struct ProducerConnection<Exe: Executor> {
    client: Pulsar<Exe>,
    topic: String,
    id: ProducerId,
    name: ProducerName,
    options: ProducerOptions,
    sequence_id: SerialId,
    /// a std mutex, never held across an await, so that messages can expire while a
    /// re-creation holds `state`
    pending: std::sync::Mutex<Pending>,
    state: Mutex<ProducerState<Exe>>,
}

struct ProducerState<Exe: Executor> {
    connection: Arc<Connection<Exe>>,
    /// whether the producer exists on `connection`: false from the start of a re-creation
    /// until it succeeds
    ready: bool,
    /// Schema version returned by the broker in `CommandProducerSuccess`, used for the messages
    /// that do not set one
    schema_version: Option<Vec<u8>>,
}

#[derive(Default)]
struct Pending {
    /// incremented each time the producer is re-created
    epoch: u64,
    /// set on close, so that the producer is not re-created anymore
    closed: bool,
    /// messages sent but not acknowledged yet, by sequence id, resent in order on re-creation
    messages: BTreeMap<u64, PendingMessage>,
}

struct PendingMessage {
    message: ProducerMessage,
    tx: oneshot::Sender<Result<CommandSendReceipt, Error>>,
    /// `operation_timeout` after the first send: resends do not extend it
    deadline: Instant,
}

impl Pending {
    fn complete(&mut self, sequence_id: u64, result: Result<CommandSendReceipt, Error>) {
        if let Some(message) = self.messages.remove(&sequence_id) {
            let _ = message.tx.send(result);
        }
    }
}

impl<Exe: Executor> ProducerConnection<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send(self: &Arc<Self>, mut message: ProducerMessage) -> Result<SendFuture, Error> {
        let deadline = Instant::now() + self.client.operation_retry_options.operation_timeout;
        let sequence_id = self.sequence_id.get();
        loop {
            let state = self
                .lock_state(deadline)
                .await
                .map_err(ProducerError::Connection)?;
            if self.is_ready(&state) {
                if message.schema_version.is_none() {
                    message.schema_version = state.schema_version.clone();
                }
                match state
                    .connection
                    .sender()
                    .send(
                        self.id,
                        self.name.clone(),
                        sequence_id,
                        message.clone(),
                        self.options.block_queue_if_full,
                    )
                    .await
                {
                    Ok(receipt) => {
                        let (tx, rx) = oneshot::channel();
                        let epoch = {
                            let mut pending = self.pending.lock().unwrap();
                            let message = PendingMessage {
                                message,
                                tx,
                                deadline,
                            };
                            pending.messages.insert(sequence_id, message);
                            pending.epoch
                        };
                        let connection = state.connection.clone();
                        if let Err(e) = self.wait_for_receipt(
                            sequence_id,
                            epoch,
                            deadline,
                            false,
                            connection,
                            receipt,
                        ) {
                            self.pending.lock().unwrap().messages.remove(&sequence_id);
                            return Err(e);
                        }
                        return Ok(SendFuture(rx));
                    }
                    Err(ConnectionError::Disconnected) => {}
                    Err(ConnectionError::Io(e)) if e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(e) => {
                        error!("send_message got error: {:?}", e);
                        return Err(ProducerError::Connection(e).into());
                    }
                }
            }
            // Re-create in the background: waiting for it can time out, while cancelling it
            // midway could leave the connection manager waiting forever on a half-created
            // connection.
            let epoch = self.pending.lock().unwrap().epoch;
            drop(state);
            let reconnected = self.spawn_reconnect(epoch)?;
            match self.wait_until(reconnected, deadline).await {
                Some(Ok(Err(e))) if is_permanent(&e) => return Err(e),
                Some(Ok(_)) => {} // re-created, or failed for now: try again
                Some(Err(oneshot::Canceled)) => return Err(Error::Executor),
                None => {
                    let error = timed_out("could not re-create the producer in time");
                    return Err(ProducerError::Connection(error).into());
                }
            }
        }
    }

    fn is_ready(&self, state: &ProducerState<Exe>) -> bool {
        state.ready && state.connection.is_valid() && !state.connection.is_producer_closed(self.id)
    }

    /// Waits for `fut`, but not past `deadline`.
    async fn wait_until<T>(&self, fut: impl Future<Output = T>, deadline: Instant) -> Option<T> {
        let expiry = self
            .client
            .executor
            .delay(deadline.saturating_duration_since(Instant::now()));
        pin_mut!(fut, expiry);
        match future::select(fut, expiry).await {
            Either::Left((output, _)) => Some(output),
            Either::Right(_) => None,
        }
    }

    /// Locks the state, waiting for a running re-creation at most until `deadline`.
    async fn lock_state(
        &self,
        deadline: Instant,
    ) -> Result<MutexGuard<'_, ProducerState<Exe>>, ConnectionError> {
        self.wait_until(self.state.lock(), deadline)
            .await
            .ok_or_else(|| timed_out("timed out waiting for the producer to be re-created"))
    }

    /// Re-creates the producer in the background, unless another task did since `epoch`.
    fn spawn_reconnect(
        self: &Arc<Self>,
        epoch: u64,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        let this = self.clone();
        self.client
            .executor
            .spawn(Box::pin(async move {
                let mut state = this.state.lock().await;
                let stale = this.pending.lock().unwrap().epoch != epoch;
                let result = if stale {
                    Ok(())
                } else {
                    this.reconnect(&mut state, false).await
                };
                let _ = tx.send(result);
            }))
            .map_err(|_| Error::Executor)?;
        Ok(rx)
    }

    /// Re-creates the producer, then resends the pending messages in order, before any new one.
    /// With `retry`, a failed attempt is retried while messages are pending: they expire on
    /// their own. After an error that retrying cannot fix, the pending messages fail right away.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn reconnect(
        self: &Arc<Self>,
        state: &mut ProducerState<Exe>,
        retry: bool,
    ) -> Result<(), Error> {
        loop {
            match self.recreate(state).await {
                Ok(()) => break,
                Err(e) if is_permanent(&e) => {
                    let messages = std::mem::take(&mut self.pending.lock().unwrap().messages);
                    for (_, message) in messages {
                        let error = match &e {
                            Error::Producer(ProducerError::Closed) => ProducerError::Closed,
                            e => ProducerError::Custom(format!(
                                "could not re-create the producer: {e}"
                            )),
                        };
                        let _ = message.tx.send(Err(error.into()));
                    }
                    return Err(e);
                }
                Err(e) => {
                    if !retry || self.pending.lock().unwrap().messages.is_empty() {
                        return Err(e);
                    }
                    warn!(
                        "could not re-create producer {:?}({}) for topic {}, retrying: {:?}",
                        self.name, self.id, self.topic, e
                    );
                }
            }
        }
        let (epoch, to_resend) = {
            let mut pending = self.pending.lock().unwrap();
            pending.epoch += 1;
            let now = Instant::now();
            let expired_ids: Vec<u64> = pending
                .messages
                .iter()
                .filter(|(_, message)| message.deadline <= now)
                .map(|(sequence_id, _)| *sequence_id)
                .collect();
            for sequence_id in expired_ids {
                pending.complete(sequence_id, Err(expired()));
            }
            let to_resend: Vec<(u64, Instant)> = pending
                .messages
                .iter()
                .map(|(sequence_id, message)| (*sequence_id, message.deadline))
                .collect();
            (pending.epoch, to_resend)
        };
        for (sequence_id, deadline) in to_resend {
            let message = self
                .pending
                .lock()
                .unwrap()
                .messages
                .get(&sequence_id)
                .map(|pending| pending.message.clone());
            let Some(message) = message else {
                continue; // acknowledged meanwhile
            };
            let connection = state.connection.clone();
            let receipt = connection
                .sender()
                .send(self.id, self.name.clone(), sequence_id, message, true)
                .await;
            // a failed resend is handled by its receipt task, like a failed receipt
            let receipt = async move { receipt?.await };
            self.wait_for_receipt(sequence_id, epoch, deadline, true, connection, receipt)?;
        }
        Ok(())
    }

    /// One attempt at re-creating the producer.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn recreate(self: &Arc<Self>, state: &mut ProducerState<Exe>) -> Result<(), Error> {
        state.ready = false;
        // Like the Java client's first backoff step, give the broker a moment (e.g. to finish
        // unloading the topic), and do not spin if it keeps closing the producer (e.g. bookies
        // down) or cannot be reached.
        // shortcut: fixed delay, use an exponential backoff if that is not enough
        self.client.executor.delay(Duration::from_millis(100)).await;
        let closed = self.pending.lock().unwrap().closed;
        if closed {
            return Err(ProducerError::Closed.into());
        }
        warn!(
            "reconnecting producer {:?}({}) for topic {}, connection {}",
            self.name,
            self.id,
            self.topic,
            state.connection.id()
        );
        if let Err(e) = state.connection.sender().close_producer(self.id).await {
            error!(
                "could not close producer {:?}({}) for topic {}: {:?}",
                self.name, self.id, self.topic, e
            );
        }
        let broker_address = self.client.lookup_topic(&self.topic).await?;
        let (_producer_name, schema_version) = retry_create_producer(
            &self.client,
            &mut state.connection,
            broker_address,
            &self.topic,
            self.id,
            Some(self.name.clone()),
            &self.options,
        )
        .await?;
        // for future messages, the pending ones keep the schema version they were sent with
        state.schema_version = schema_version;
        let closed = self.pending.lock().unwrap().closed;
        if closed {
            // closed meanwhile: do not leave the producer open on the broker
            let _ = state.connection.sender().close_producer(self.id).await;
            return Err(ProducerError::Closed.into());
        }
        state.ready = true;
        Ok(())
    }

    /// Waits for a receipt in the background. If the broker closed the producer or the
    /// connection dropped meanwhile, the producer is re-created and the pending messages are
    /// resent, instead of failing this message.
    fn wait_for_receipt(
        self: &Arc<Self>,
        sequence_id: u64,
        epoch: u64,
        deadline: Instant,
        resent: bool,
        connection: Arc<Connection<Exe>>,
        receipt: impl Future<Output = Result<CommandSendReceipt, ConnectionError>> + Send + 'static,
    ) -> Result<(), Error> {
        let this = self.clone();
        self.client
            .executor
            .spawn(Box::pin(async move {
                // The first send keeps the connection's receipt timeout, which marks the
                // connection broken when it expires; a resend gets what is left before the
                // deadline.
                let result = if resent {
                    match this.wait_until(receipt, deadline).await {
                        Some(result) => result,
                        None => return this.expire(sequence_id),
                    }
                } else {
                    receipt.await
                };
                let error = match result {
                    Ok(receipt) => return this.complete(sequence_id, Ok(receipt)),
                    Err(e) => e,
                };
                let stale = this.pending.lock().unwrap().epoch != epoch;
                if stale {
                    return; // resent since then, a newer task waits for its receipt
                }
                let lost = connection.is_producer_closed(this.id)
                    || matches!(error, ConnectionError::Disconnected);
                if !lost {
                    error!("wait send receipt got error: {:?}", error);
                    return this
                        .complete(sequence_id, Err(ProducerError::Connection(error).into()));
                }
                this.recover(sequence_id, epoch, deadline).await;
            }))
            .map_err(|_| Error::Executor)
    }

    /// Re-creates the producer after a receipt was lost with the producer or its connection.
    /// This message fails if its deadline passes first.
    async fn recover(self: &Arc<Self>, sequence_id: u64, epoch: u64, deadline: Instant) {
        let Ok(mut state) = self.lock_state(deadline).await else {
            return self.expire(sequence_id);
        };
        let done = {
            let pending = self.pending.lock().unwrap();
            pending.epoch != epoch || !pending.messages.contains_key(&sequence_id)
        };
        if done {
            return; // re-created by another task meanwhile, or completed
        }
        let reconnect = self.reconnect(&mut state, true);
        pin_mut!(reconnect);
        if self
            .wait_until(reconnect.as_mut(), deadline)
            .await
            .is_none()
        {
            self.expire(sequence_id);
            // an attempt cannot be cancelled safely, and other messages may still be resent
            let _ = reconnect.await;
        }
    }

    fn complete(&self, sequence_id: u64, result: Result<CommandSendReceipt, Error>) {
        self.pending.lock().unwrap().complete(sequence_id, result);
    }

    fn expire(&self, sequence_id: u64) {
        self.complete(sequence_id, Err(expired()));
    }

    /// Closes the producer, which is then no longer re-created.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn close(&self) -> Result<CommandSuccess, ConnectionError> {
        self.pending.lock().unwrap().closed = true;
        // a re-creation running longer than that closes the producer itself when it ends
        let deadline = Instant::now() + self.client.operation_retry_options.operation_timeout;
        let connection = self.lock_state(deadline).await?.connection.clone();
        connection.sender().close_producer(self.id).await
    }
}

fn timed_out(message: &'static str) -> ConnectionError {
    ConnectionError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, message))
}

/// The error of a message not acknowledged within `operation_timeout` of its first send.
fn expired() -> Error {
    ProducerError::Connection(timed_out(
        "the message was not acknowledged within the operation timeout",
    ))
    .into()
}

/// Errors after which re-creating the producer cannot succeed.
fn is_permanent(error: &Error) -> bool {
    use proto::ServerError as E;
    let kind = match error {
        Error::Producer(ProducerError::Closed) => return true,
        Error::Connection(ConnectionError::PulsarError(kind, _))
        | Error::Producer(ProducerError::Connection(ConnectionError::PulsarError(kind, _)))
        | Error::ServiceDiscovery(ServiceDiscoveryError::Query(kind, _)) => kind,
        _ => return false,
    };
    matches!(
        kind,
        Some(
            E::TopicNotFound
                | E::TopicTerminatedError
                | E::AuthenticationError
                | E::AuthorizationError
                | E::ProducerFenced
                | E::IncompatibleSchema
                | E::NotAllowedError
                | E::InvalidTopicName
        )
    )
}

impl<Exe: Executor> std::ops::Drop for TopicProducer<Exe> {
    fn drop(&mut self) {
        let conn = self.connection.clone();
        let id = self.id;
        let name = self.name.clone();
        let topic = self.topic.clone();
        if let Some(mut batch) = self.batch.take() {
            batch.msg_sender.close_channel();
        }
        let _ = self.client.executor.spawn(Box::pin(async move {
            if let Err(e) = conn.close().await {
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

        let mut producers: Vec<TopicProducer<Exe>> = try_join_all(
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

        // sort by partition id
        producers.sort_by_key(|prod| prod.id);

        let producer = match producers.len() {
            0 => {
                return Err(Error::Custom(format!(
                    "Unexpected error: Partition lookup returned no topics for {topic}"
                )));
            }
            1 => ProducerInner::Single(producers.into_iter().next().unwrap()),
            len => ProducerInner::Partitioned(PartitionedProducer {
                producers,
                last_used_producer_index: rand::thread_rng().gen_range(0..len),
                topic,
                options,
            }),
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
    close_receiver: oneshot::Receiver<Result<CommandSuccess, ConnectionError>>,
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
async fn batch_process_loop(
    producer_id: ProducerId,
    mut batch_storage: BatchStorage,
    mut msg_receiver: mpsc::Receiver<BatchItem>,
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
                    info!("producer {producer_id}'s batch_process_loop: channel closed, exiting");
                }
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
    close_sender: oneshot::Sender<Result<CommandSuccess, ConnectionError>>,
    connection: Arc<ProducerConnection<Exe>>,
) where
    Exe: Executor,
{
    let producer_id = connection.id;
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
                if counter == 0 {
                    if let Some(flush_tx) = flush_tx {
                        let _ = flush_tx.send(());
                    }
                    continue;
                }

                let message = ProducerMessage {
                    payload,
                    num_messages_in_batch: Some(counter as i32),
                    ..Default::default()
                };

                trace!("sending a batched message of size {}", counter);

                let send = async || {
                    let compressed_message =
                        compress_message(message, &connection.options.compression)?;
                    connection.send(compressed_message).await?.await
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
                let close_result = connection.close().await;
                let _ = close_sender.send(close_result).inspect_err(|e| {
                    warn!(
                        "{producer_id} could not notify the message_send_loop is closed: {:?}, the producer might be dropped without closing",
                        e
                    );
                });
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
    partition_key_b64_encoded: Option<bool>,
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
            partition_key_b64_encoded: None,
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
            partition_key_b64_encoded: self.partition_key_b64_encoded,
            ordering_key: self.ordering_key,
            deliver_at_time: self.deliver_at_time,
            event_time: self.event_time,
            content,
        }
    }

    /// sets the message's partition key as a UTF-8 string
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_partition_key<S: Into<String>>(mut self, partition_key: S) -> Self {
        let (key, b64) = utf8_partition_key(partition_key);
        self.partition_key = Some(key);
        self.partition_key_b64_encoded = Some(b64);
        self
    }

    /// sets the message's ordering key for key_shared subscription
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_ordering_key<S: Into<Vec<u8>>>(mut self, ordering_key: S) -> Self {
        self.ordering_key = Some(ordering_key.into());
        self
    }

    /// sets the message's partition key as a UTF-8 string
    ///
    /// this is the same as `with_partition_key`, this method is added for
    /// more consistency with other clients
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_key<S: Into<String>>(mut self, partition_key: S) -> Self {
        let (key, b64) = utf8_partition_key(partition_key);
        self.partition_key = Some(key);
        self.partition_key_b64_encoded = Some(b64);
        self
    }

    /// sets the message's partition key from raw bytes
    ///
    /// The bytes are Base64-encoded into `partition_key` and
    /// `partition_key_b64_encoded` is set to `true`.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_key_bytes<S: AsRef<[u8]>>(mut self, key: S) -> Self {
        let (encoded, b64) = partition_key_from_bytes(key.as_ref());
        self.partition_key = Some(encoded);
        self.partition_key_b64_encoded = Some(b64);
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
            partition_key_b64_encoded,
            ordering_key,
            content,
            deliver_at_time,
            event_time,
        } = self;

        let mut message = T::serialize_message(content)?;
        message.properties = properties;
        message.partition_key = partition_key;
        message.partition_key_b64_encoded = partition_key_b64_encoded;
        message.ordering_key = ordering_key;
        message.event_time = event_time;
        message.deliver_at_time = deliver_at_time;
        producer.send_non_blocking(message).await
    }
}

/// Encode raw key bytes for the partition key wire field.
///
/// Returns the Base64-encoded key string and `partition_key_b64_encoded = true`.
pub(crate) fn partition_key_from_bytes(key: &[u8]) -> (String, bool) {
    (base64::engine::general_purpose::STANDARD.encode(key), true)
}

/// Mark a UTF-8 partition key.
///
/// Returns the key string and `partition_key_b64_encoded = false`.
pub(crate) fn utf8_partition_key(key: impl Into<String>) -> (String, bool) {
    (key.into(), false)
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use log::LevelFilter;

    use super::*;
    use crate::{
        routing_policy::CustomRoutingPolicy, test_utils, tests::TEST_LOGGER, TokioExecutor,
    };

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
            partition_key_b64_encoded: Some(true),
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
        assert_eq!(pm.partition_key_b64_encoded, Some(true));
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

    #[test]
    fn partition_key_from_bytes_encodes_and_sets_b64_flag() {
        let raw_key = b"\x00\x01binary-key";
        let (encoded, b64) = partition_key_from_bytes(raw_key);
        assert_eq!(
            encoded,
            base64::engine::general_purpose::STANDARD.encode(raw_key)
        );
        assert!(b64);

        let message = Message {
            payload: b"payload".to_vec(),
            partition_key: Some(encoded.clone()),
            partition_key_b64_encoded: Some(b64),
            ..Default::default()
        };
        let pm: ProducerMessage = message.into();
        assert_eq!(pm.partition_key.as_deref(), Some(encoded.as_str()));
        assert_eq!(pm.partition_key_b64_encoded, Some(true));
    }

    #[test]
    fn utf8_partition_key_sets_b64_flag_false() {
        let (key, b64) = utf8_partition_key("plain-key");
        assert_eq!(key, "plain-key");
        assert!(!b64);

        let message = Message {
            payload: b"payload".to_vec(),
            partition_key: Some(key),
            partition_key_b64_encoded: Some(b64),
            ..Default::default()
        };
        let pm: ProducerMessage = message.into();
        assert_eq!(pm.partition_key.as_deref(), Some("plain-key"));
        assert_eq!(pm.partition_key_b64_encoded, Some(false));
    }

    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn key_bytes_round_trip() {
        use crate::{
            consumer::Consumer, message::proto::command_subscribe::SubType, DeserializeMessage,
            Payload,
        };
        use futures::StreamExt;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        struct KeyBytesTestData {
            data: String,
        }

        impl SerializeMessage for &KeyBytesTestData {
            fn serialize_message(input: Self) -> Result<Message, Error> {
                let payload =
                    serde_json::to_vec(input).map_err(|e| Error::Custom(e.to_string()))?;
                Ok(Message {
                    payload,
                    ..Default::default()
                })
            }
        }

        impl DeserializeMessage for KeyBytesTestData {
            type Output = Result<KeyBytesTestData, serde_json::Error>;

            fn deserialize_message(payload: &Payload) -> Self::Output {
                serde_json::from_slice(&payload.data)
            }
        }

        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let pulsar: Pulsar<_> = Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!("key_bytes_round_trip_{}", rand::random::<u16>());
        let raw_key = b"\x00\x01\x02avro-like-key";

        let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();

        let mut consumer: Consumer<KeyBytesTestData, _> = pulsar
            .consumer()
            .with_topic(&topic)
            .with_subscription(format!("sub_{}", rand::random::<u16>()))
            .with_subscription_type(SubType::Exclusive)
            .build()
            .await
            .unwrap();

        let data = KeyBytesTestData {
            data: "hello-key-bytes".to_string(),
        };
        producer
            .create_message()
            .with_content(&data)
            .with_key_bytes(raw_key)
            .send_non_blocking()
            .await
            .unwrap()
            .await
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(5), consumer.next())
            .await
            .expect("timed out waiting for key_bytes round-trip message")
            .unwrap()
            .unwrap();
        assert!(msg.has_base64_encoded_key());
        assert_eq!(
            msg.key_bytes().unwrap().as_deref(),
            Some(raw_key.as_slice())
        );
        assert_eq!(
            msg.key().as_deref(),
            Some(
                base64::engine::general_purpose::STANDARD
                    .encode(raw_key)
                    .as_str()
            )
        );
        assert_eq!(msg.metadata().partition_key_b64_encoded, Some(true));
        assert_eq!(msg.deserialize().unwrap(), data);
        consumer.ack(&msg).await.unwrap();
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
                Err(e) => panic!("failed to send {}: {}", i, e),
            }
        }
        info!("Messages failed due to SlowDown: {:?}", failed_indexes);
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
                Err(e) => panic!("failed to send {}: {}", i, e),
            }
        }
    }

    #[tokio::test]
    async fn producer_resends_pending_messages_after_topic_unload() {
        use crate::{consumer::Consumer, OperationRetryOptions};

        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
            // the topic may still be unloading when the producer is re-created
            .with_operation_retry_options(OperationRetryOptions {
                retry_delay: Duration::from_millis(100),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let batched = ProducerOptions {
            batch_size: Some(10),
            batch_timeout: Some(Duration::from_millis(5)),
            ..Default::default()
        };
        for options in [ProducerOptions::default(), batched] {
            let topic = format!("producer_unload_pending_{}", rand::random::<u32>());
            let mut consumer: Consumer<String, _> = pulsar
                .consumer()
                .with_topic(&topic)
                .with_subscription("sub")
                .build()
                .await
                .unwrap();
            let mut producer = pulsar
                .producer()
                .with_topic(&topic)
                .with_options(options)
                .build()
                .await
                .unwrap();

            // unloading the topic makes the broker send CommandCloseProducer
            let unload_topic = topic.clone();
            let unload = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                test_utils::unload_topic("public", "default", &unload_topic).await;
            });
            // keep sending until the unload is done, then a bit more, so it lands mid-stream
            let mut receipts = Vec::new();
            let mut after_unload = 0;
            while after_unload < 20 {
                if unload.is_finished() {
                    after_unload += 1;
                }
                let message = format!("msg-{}", receipts.len());
                receipts.push(producer.send_non_blocking(message).await.unwrap());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            unload.await.unwrap();
            let count = receipts.len();
            // without handling the close, sends wait for the 30s operation timeout and fail
            tokio::time::timeout(Duration::from_secs(10), try_join_all(receipts))
                .await
                .expect("receipts timed out")
                .unwrap();

            // every message arrives, first in send order: resends may add duplicates when the
            // broker does not deduplicate
            let mut next = 0;
            while next < count {
                let msg = tokio::time::timeout(Duration::from_secs(10), consumer.next())
                    .await
                    .expect("message lost")
                    .unwrap()
                    .unwrap();
                consumer.ack(&msg).await.unwrap();
                let i: usize = msg.deserialize().unwrap()["msg-".len()..].parse().unwrap();
                assert!(i <= next, "msg-{i} arrived before msg-{next}");
                if i == next {
                    next += 1;
                }
            }
        }
    }

    #[tokio::test]
    async fn producer_send_times_out_when_it_cannot_be_recreated() {
        use crate::OperationRetryOptions;

        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let operation_timeout = Duration::from_secs(2);
        let pulsar: Pulsar<_> = Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
            .with_operation_retry_options(OperationRetryOptions {
                operation_timeout,
                retry_delay: Duration::from_millis(100),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let topic = format!("producer_recreate_timeout_{}", rand::random::<u32>());
        let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();
        producer
            .send_non_blocking("before")
            .await
            .unwrap()
            .await
            .unwrap();

        // after the unload, an exclusive producer takes the topic: ours cannot be re-created
        test_utils::unload_topic("public", "default", &topic).await;
        let exclusive = ProducerOptions {
            access_mode: Some(proto::ProducerAccessMode::Exclusive as i32),
            ..Default::default()
        };
        let mut exclusive = pulsar
            .producer()
            .with_topic(&topic)
            .with_options(exclusive)
            .build()
            .await
            .unwrap();

        let start = Instant::now();
        let blocked = async { producer.send_non_blocking("blocked").await?.await };
        let result = tokio::time::timeout(Duration::from_secs(10), blocked)
            .await
            .expect("the send hangs while the producer cannot be re-created");
        assert!(result.is_err());
        assert!(
            start.elapsed() < 2 * operation_timeout,
            "{:?}",
            start.elapsed()
        );

        // once the topic is free again, the producer is re-created
        exclusive.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            producer
                .send_non_blocking("after")
                .await
                .unwrap()
                .await
                .unwrap()
        })
        .await
        .expect("the send after the exclusive producer closed timed out");
    }

    #[tokio::test]
    async fn move_producer_to_spawned_task() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
            .with_outbound_channel_size(3)
            .build()
            .await
            .unwrap();
        let mut producer = pulsar
            .producer()
            .with_topic(format!("topic_{}", rand::random::<u16>()))
            .build()
            .await
            .unwrap();
        let (sender, receiver) = oneshot::channel();
        let _ = pulsar.executor.spawn(Box::pin(async move {
            sender.send(producer.close().await).unwrap();
        }));
        assert!(receiver.await.is_ok());
    }

    #[tokio::test]
    async fn test_round_robin_routing_policy() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!("topic_{}", rand::random::<u16>());
        let options = ProducerOptions {
            routing_policy: Some(RoutingPolicy::RoundRobin),
            ..Default::default()
        };
        let partition_count = 3;
        test_utils::create_partitioned_topic("public", "default", &topic, partition_count).await;

        let mut producer = pulsar
            .producer()
            .with_topic(topic)
            .with_options(options)
            .build()
            .await
            .unwrap();

        // test round robin without key
        let message = "test".to_string();
        let mut producer_id = 0;
        for _ in 1..100 {
            let send_receipt = producer
                .send_non_blocking(&message)
                .await
                .unwrap()
                .await
                .unwrap();

            assert!(send_receipt.producer_id != producer_id);
            producer_id = send_receipt.producer_id;
        }

        // test round robin with key
        let key = "test";
        let message = Message {
            payload: "test".to_string().into(),
            partition_key: Some(key.to_string()),
            ..Default::default()
        };
        let CommandSendReceipt { producer_id, .. } = producer
            .send_non_blocking(message)
            .await
            .unwrap()
            .await
            .unwrap();
        for _ in 1..100 {
            let message = Message {
                payload: "test".to_string().into(),
                partition_key: Some(key.to_string()),
                ..Default::default()
            };

            let send_receipt = producer
                .send_non_blocking(message)
                .await
                .unwrap()
                .await
                .unwrap();

            assert!(send_receipt.producer_id == producer_id);
        }
    }

    #[tokio::test]
    async fn test_single_routing_policy() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!("topic_{}", rand::random::<u16>());
        let options = ProducerOptions {
            routing_policy: Some(RoutingPolicy::Single),
            ..Default::default()
        };
        let partition_count = 3;
        test_utils::create_partitioned_topic("public", "default", &topic, partition_count).await;

        let mut producer = pulsar
            .producer()
            .with_topic(topic)
            .with_options(options)
            .build()
            .await
            .unwrap();

        let key = "test";
        let message = Message {
            payload: "test".to_string().into(),
            partition_key: Some(key.to_string()),
            ..Default::default()
        };

        let CommandSendReceipt { producer_id, .. } = producer
            .send_non_blocking(message)
            .await
            .unwrap()
            .await
            .unwrap();
        for _ in 1..100 {
            let message = Message {
                payload: "test".to_string().into(),
                partition_key: Some(key.to_string()),
                ..Default::default()
            };

            let send_receipt = producer
                .send_non_blocking(message)
                .await
                .unwrap()
                .await
                .unwrap();

            assert!(send_receipt.producer_id == producer_id);
        }
    }

    struct TestCustomRoutingPolicy {}

    impl CustomRoutingPolicy for TestCustomRoutingPolicy {
        fn route(&self, _message: &Message, _num_producers: usize) -> usize {
            1
        }
    }

    #[tokio::test]
    async fn test_custom_routing_policy() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let pulsar: Pulsar<_> = Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
            .build()
            .await
            .unwrap();
        let topic = format!("topic_{}", rand::random::<u16>());
        let options = ProducerOptions {
            routing_policy: Some(RoutingPolicy::Custom(Arc::new(TestCustomRoutingPolicy {}))),
            ..Default::default()
        };
        let partition_count = 3;
        test_utils::create_partitioned_topic("public", "default", &topic, partition_count).await;

        let mut producer = pulsar
            .producer()
            .with_topic(topic)
            .with_options(options)
            .build()
            .await
            .unwrap();

        let key = "test";
        let message = Message {
            payload: "test".to_string().into(),
            partition_key: Some(key.to_string()),
            ..Default::default()
        };

        let CommandSendReceipt { producer_id, .. } = producer
            .send_non_blocking(message)
            .await
            .unwrap()
            .await
            .unwrap();
        for _ in 1..100 {
            let message = Message {
                payload: "test".to_string().into(),
                partition_key: Some(key.to_string()),
                ..Default::default()
            };

            let send_receipt = producer
                .send_non_blocking(message)
                .await
                .unwrap()
                .await
                .unwrap();

            assert!(send_receipt.producer_id == producer_id);
        }
    }
}
