//! Message publication
use std::{
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
    channel::oneshot,
    future::try_join_all,
    lock::Mutex,
    task::{Context, Poll},
    Future,
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
    /// algorithm used to compress the messages
    pub compression: Option<Compression>,
    /// producer access mode: shared = 0, exclusive = 1, waitforexclusive =2,
    /// exclusivewithoutfencing =3
    pub access_mode: Option<i32>,
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
    pub fn create_message(&mut self) -> MessageBuilder<(), Exe> {
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
    message_id: SerialId,
    // putting it in a mutex because we must send multiple messages at once
    // while we might be pushing more messages from elsewhere
    batch: Option<Mutex<Batch>>,
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
        let sequence_ids = SerialId::new();

        let topic = topic.clone();
        let batch_size = options.batch_size;
        let batch_byte_size = options.batch_byte_size;
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

        let batch = batch_size
            .map(|batch_size| Batch::new(batch_size, batch_byte_size))
            .map(Mutex::new);

        Ok(TopicProducer {
            client,
            connection,
            id: producer_id,
            name: producer_name,
            topic,
            message_id: sequence_ids,
            batch,
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
        match self.batch.as_ref() {
            None => Err(ProducerError::Custom("not a batching producer".to_string()).into()),
            Some(batch) => {
                let messages = {
                    let guard = batch.lock().await;
                    guard.get_messages().await
                };
                let message_count = messages.len();

                if message_count == 0 {
                    return Ok(());
                }

                let mut payload: Vec<u8> = Vec::new();
                let mut receipts = Vec::with_capacity(message_count);

                for (tx, message) in messages {
                    receipts.push(tx);
                    message.serialize(&mut payload);
                }

                let message = ProducerMessage {
                    payload,
                    num_messages_in_batch: Some(message_count as i32),
                    ..Default::default()
                };

                trace!("sending a batched message of size {}", message_count);

                let compressed_message = self.compress_message(message)?;

                let send_receipt = self
                    .send_inner(compressed_message)
                    .await?
                    .await
                    .map_err(Arc::new);

                for resolver in receipts {
                    let _ = resolver.send(
                        send_receipt
                            .clone()
                            .map_err(|e| ProducerError::Batch(e).into()),
                    );
                }

                Ok(())
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn send_raw(&mut self, message: ProducerMessage) -> Result<SendFuture, Error> {
        let (tx, rx) = oneshot::channel();
        match self.batch.as_ref() {
            None => {
                let compressed_message = self.compress_message(message)?;

                let fut = self.send_inner(compressed_message).await?;

                self.client
                    .executor
                    .spawn(Box::pin(async move {
                        let _ = tx.send(fut.await);
                    }))
                    .map_err(|_| Error::Executor)?;
                Ok(SendFuture(rx))
            }
            Some(batch) => {
                // Push into batch while holding the lock, and conditionally drain.
                let (maybe_drained, counter) = {
                    let guard = batch.lock().await;
                    guard.push_back((tx, message)).await;

                    if guard.is_full().await {
                        let drained = guard.get_messages().await; // await inner storage lock
                        let count = drained.len() as i32;
                        (Some(drained), count)
                    } else {
                        (None, 0)
                    }
                };

                if counter == 0 {
                    return Ok(SendFuture(rx));
                }

                let mut payload: Vec<u8> = Vec::new();
                let mut receipts = Vec::new();

                if let Some(messages) = maybe_drained {
                    for (tx, message) in messages {
                        receipts.push(tx);
                        message.serialize(&mut payload);
                    }
                }

                let message = ProducerMessage {
                    payload,
                    num_messages_in_batch: Some(counter),
                    ..Default::default()
                };

                trace!("sending a batched message of size {}", counter);

                let compressed_message = self.compress_message(message)?;

                let receipt_fut = self.send_inner(compressed_message).await?;

                self.client
                    .executor
                    .spawn(Box::pin(async move {
                        let res = receipt_fut.await.map_err(Arc::new);
                        for tx in receipts.drain(..) {
                            let _ =
                                tx.send(res.clone().map_err(|e| ProducerError::Batch(e).into()));
                        }
                    }))
                    .map_err(|_| Error::Executor)?;

                Ok(SendFuture(rx))
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn compress_message(&mut self, mut message: ProducerMessage) -> Result<ProducerMessage, Error> {
        let compressed_message = match self.compression.clone() {
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
    async fn send_inner(
        &mut self,
        message: ProducerMessage,
    ) -> Result<impl Future<Output = Result<CommandSendReceipt, Error>>, Error> {
        loop {
            let msg = message.clone();
            match self.connection.sender().send(
                self.id,
                self.name.clone(),
                self.message_id.get(),
                msg,
            ) {
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
                        error!("send_inner got io error: {:?}", e);
                        return Err(ProducerError::Connection(ConnectionError::Io(e)).into());
                    }
                }
                Err(e) => {
                    error!("send_inner got error: {:?}", e);
                    return Err(ProducerError::Connection(e).into());
                }
            };

            error!(
                "send_inner: connection {} disconnected",
                self.connection.id()
            );

            self.reconnect().await?;
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn reconnect(&mut self) -> Result<(), Error> {
        debug!("reconnecting producer for topic: {}", self.topic);

        if let Err(e) = self.connection.sender().close_producer(self.id).await {
            error!(
                "could not close producer {:?}({}) for topic {}: {:?}",
                self.name, self.id, self.topic, e
            );
        }
        let broker_address = self.client.lookup_topic(&self.topic).await?;

        // should we ignore, test or use producer_name?
        let _producer_name = retry_create_producer(
            &self.client,
            &mut self.connection,
            broker_address,
            &self.topic,
            self.id,
            Some(self.name.clone()),
            &self.options,
        )
        .await?;

        self.batch = self
            .options
            .batch_size
            .map(|batch_size| Batch::new(batch_size, self.options.batch_byte_size))
            .map(Mutex::new);

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close(&self) -> Result<(), Error> {
        self.connection.sender().close_producer(self.id).await?;
        Ok(())
    }
}

impl<Exe: Executor> std::ops::Drop for TopicProducer<Exe> {
    fn drop(&mut self) {
        let conn = self.connection.clone();
        let id = self.id;
        let name = self.name.clone();
        let topic = self.topic.clone();
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
                )))
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
    size: usize,
    storage: VecDeque<(
        oneshot::Sender<Result<CommandSendReceipt, Error>>,
        BatchedMessage,
    )>,
}

impl BatchStorage {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(length: u32) -> BatchStorage {
        BatchStorage {
            size: 0,
            storage: VecDeque::with_capacity(length as usize),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn push_back(
        &mut self,
        tx: oneshot::Sender<Result<CommandSendReceipt, Error>>,
        batched: BatchedMessage,
    ) {
        self.size += batched.metadata.payload_size as usize;
        self.storage.push_back((tx, batched))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn get_messages(
        &mut self,
    ) -> Vec<(
        oneshot::Sender<Result<CommandSendReceipt, Error>>,
        BatchedMessage,
    )> {
        self.size = 0;
        self.storage.drain(..).collect()
    }
}

struct Batch {
    // max number of message
    pub length: u32,
    // message bytes threshold
    pub size: Option<usize>,
    // put it in a mutex because the design of Producer requires an immutable TopicProducer,
    // so we cannot have a mutable Batch in a send_raw(&mut self, ...)
    pub storage: Mutex<BatchStorage>,
}

impl Batch {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(length: u32, size: Option<usize>) -> Batch {
        Batch {
            length,
            size,
            storage: Mutex::new(BatchStorage::new(length)),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn is_full(&self) -> bool {
        let s = self.storage.lock().await;
        match self.size {
            None => s.storage.len() >= self.length as usize,
            Some(size) => s.storage.len() >= self.length as usize || s.size >= size,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn push_back(
        &self,
        msg: (
            oneshot::Sender<Result<CommandSendReceipt, Error>>,
            ProducerMessage,
        ),
    ) {
        let (tx, message) = msg;

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
        self.storage.lock().await.push_back(tx, batched)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_messages(
        &self,
    ) -> Vec<(
        oneshot::Sender<Result<CommandSendReceipt, Error>>,
        BatchedMessage,
    )> {
        self.storage.lock().await.get_messages()
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
