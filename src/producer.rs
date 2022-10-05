//! Message publication
use futures::{channel::oneshot, future::try_join_all, lock::Mutex};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::client::SerializeMessage;
use crate::connection::{Connection, SerialId};
use crate::error::{ConnectionError, ProducerError};
use crate::executor::Executor;
use crate::message::proto::{self, CommandSendReceipt, CompressionType, EncryptionKeys, Schema};
use crate::message::BatchedMessage;
use crate::{Error, Pulsar};
use futures::task::{Context, Poll};
use futures::Future;

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
    /// algorithm used to compress the messages
    pub compression: Option<proto::CompressionType>,
    /// producer access mode: shared = 0, exclusive = 1, waitforexclusive =2, exclusivewithoutfencing =3
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
/// let mut producer = pulsar.producer()
///     .with_name("name")
///     .build_multi_topic();
/// let send_1 = producer.send(topic, &message).await?;
/// let send_2 = producer.send(topic, &message).await?;
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
    pub async fn send<T: SerializeMessage + Sized, S: Into<String>>(
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
        producer.send(message).await
    }

    /// sends a list of messages on a topic
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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
        let topic = topic.into();
        let mut sends = Vec::new();
        for msg in messages {
            sends.push(self.send(&topic, msg).await);
        }
        // TODO determine whether to keep this approach or go with the partial send, but more mem friendly lazy approach.
        // serialize all messages before sending to avoid a partial send
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
    /// - the producer is batching messages, so this function must return immediately,
    /// and the receipt will come when the batched messages are actually sent
    ///
    /// Usage:
    ///
    /// ```rust,no_run
    /// # async fn run(mut producer: pulsar::Producer<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
    /// let f1 = producer.send("hello").await?;
    /// let f2 = producer.send("world").await?;
    /// let receipt1 = f1.await?;
    /// let receipt2 = f2.await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send<T: SerializeMessage + Sized>(
        &mut self,
        message: T,
    ) -> Result<SendFuture, Error> {
        match &mut self.inner {
            ProducerInner::Single(p) => p.send(message).await,
            ProducerInner::Partitioned(p) => p.next().send(message).await,
        }
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
    //putting it in a mutex because we must send multiple messages at once
    // while we might be pushing more messages from elsewhere
    batch: Option<Mutex<Batch>>,
    compression: Option<proto::CompressionType>,
    drop_signal: oneshot::Sender<()>,
    options: ProducerOptions,
}

impl<Exe: Executor> TopicProducer<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn from_connection<S: Into<String>>(
        client: Pulsar<Exe>,
        mut connection: Arc<Connection<Exe>>,
        topic: S,
        name: Option<String>,
        options: ProducerOptions,
    ) -> Result<Self, Error> {
        let topic = topic.into();
        let producer_id = rand::random();
        let sequence_ids = SerialId::new();

        let topic = topic.clone();
        let batch_size = options.batch_size;
        let compression = options.compression;

        match compression {
            None | Some(CompressionType::None) => {}
            Some(CompressionType::Lz4) => {
                #[cfg(not(feature = "lz4"))]
                return Err(Error::Custom("cannot create a producer with LZ4 compression because the 'lz4' cargo feature is not active".to_string()));
            }
            Some(CompressionType::Zlib) => {
                #[cfg(not(feature = "flate2"))]
                return Err(Error::Custom("cannot create a producer with zlib compression because the 'flate2' cargo feature is not active".to_string()));
            }
            Some(CompressionType::Zstd) => {
                #[cfg(not(feature = "zstd"))]
                return Err(Error::Custom("cannot create a producer with zstd compression because the 'zstd' cargo feature is not active".to_string()));
            }
            Some(CompressionType::Snappy) => {
                #[cfg(not(feature = "snap"))]
                return Err(Error::Custom("cannot create a producer with Snappy compression because the 'snap' cargo feature is not active".to_string()));
            } //Some() => unimplemented!(),
        };

        let producer_name: ProducerName;
        let mut current_retries = 0u32;
        let start = std::time::Instant::now();
        let operation_retry_options = client.operation_retry_options.clone();

        loop {
            let connection_sender = connection.sender();
            match connection_sender
                .create_producer(topic.clone(), producer_id, name.clone(), options.clone())
                .await
                .map_err(|e| {
                    error!("TopicProducer::from_connection error[{}]: {:?}", line!(), e);
                    e
                }) {
                Ok(partial_success) => {
                    // If producer is not "ready", the client will avoid to timeout the request
                    // for creating the producer. Instead it will wait indefinitely until it gets
                    // a subsequent  `CommandProducerSuccess` with `producer_ready==true`.
                    if let Some(producer_ready) = partial_success.producer_ready {
                        if !producer_ready {
                            // wait until next commandproducersuccess message has been received
                            trace!("producer is still waiting for exclusive access");
                            let result = connection_sender
                                .wait_for_exclusive_access(partial_success.request_id)
                                .await;
                            trace!("result is received: {:?}", result);
                        }
                    }
                    producer_name = partial_success.producer_name;

                    if current_retries > 0 {
                        let dur = (std::time::Instant::now() - start).as_secs();
                        log::info!(
                            "producer({}) success after {} retries over {} seconds",
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
                        error!("create_producer({}) answered ServiceNotReady, retrying request after {}ms (max_retries = {:?}): {}",
                        topic, operation_retry_options.retry_delay.as_millis(),
                        operation_retry_options.max_retries, text.unwrap_or_default());

                        current_retries += 1;
                        client
                            .executor
                            .delay(operation_retry_options.retry_delay)
                            .await;

                        let addr = client.lookup_topic(&topic).await?;
                        connection = client.manager.get_connection(&addr).await?;

                        continue;
                    } else {
                        error!("create_producer({}) reached max retries", topic);

                        return Err(ConnectionError::PulsarError(
                            Some(proto::ServerError::ServiceNotReady),
                            text,
                        )
                        .into());
                    }
                }
                Err(ConnectionError::PulsarError(Some(proto::ServerError::ProducerBusy), text)) => {
                    if operation_retry_options.max_retries.is_none()
                        || operation_retry_options.max_retries.unwrap() > current_retries
                    {
                        error!("create_producer({}) answered ProducerBusy, retrying request after {}ms (max_retries = {:?}): {}",
                        topic, operation_retry_options.retry_delay.as_millis(),
                        operation_retry_options.max_retries, text.unwrap_or_default());

                        current_retries += 1;
                        client
                            .executor
                            .delay(operation_retry_options.retry_delay)
                            .await;

                        let addr = client.lookup_topic(&topic).await?;
                        connection = client.manager.get_connection(&addr).await?;

                        continue;
                    } else {
                        error!("create_producer({}) reached max retries", topic);

                        return Err(ConnectionError::PulsarError(
                            Some(proto::ServerError::ProducerBusy),
                            text,
                        )
                        .into());
                    }
                }
                Err(ConnectionError::Io(e)) => {
                    if e.kind() != std::io::ErrorKind::TimedOut {
                        warn!("send_inner got io error: {:?}", e);
                        return Err(ProducerError::Connection(ConnectionError::Io(e)).into());
                    } else if operation_retry_options.max_retries.is_none()
                        || operation_retry_options.max_retries.unwrap() > current_retries
                    {
                        error!(
                                "create_producer({}) TimedOut, retrying request after {}ms (max_retries = {:?})",
                                topic, operation_retry_options.retry_delay.as_millis(),
                                operation_retry_options.max_retries
                            );

                        current_retries += 1;
                        client
                            .executor
                            .delay(operation_retry_options.retry_delay)
                            .await;

                        let addr = client.lookup_topic(&topic).await?;
                        connection = client.manager.get_connection(&addr).await?;

                        continue;
                    } else {
                        error!("create_producer({}) reached max retries", topic);

                        return Err(ProducerError::Connection(ConnectionError::Io(e)).into());
                    }
                }
                //this also captures producer fenced error
                Err(e) => return Err(Error::Connection(e)),
            }
        }

        // drop_signal will be dropped when the TopicProducer is dropped, then
        // drop_receiver will return, and we can close the producer
        let (_drop_signal, drop_receiver) = oneshot::channel::<()>();
        let conn = connection.clone();
        let _ = client.executor.spawn(Box::pin(async move {
            let _res = drop_receiver.await;
            let _ = conn.sender().close_producer(producer_id).await;
        }));

        Ok(TopicProducer {
            client,
            connection,
            id: producer_id,
            name: producer_name,
            topic,
            message_id: sequence_ids,
            batch: batch_size.map(Batch::new).map(Mutex::new),
            compression,
            drop_signal: _drop_signal,
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
                let mut payload: Vec<u8> = Vec::new();
                let mut receipts = Vec::new();
                let message_count;

                {
                    let batch = batch.lock().await;
                    let messages = batch.get_messages().await;
                    message_count = messages.len();
                    for (tx, message) in messages {
                        receipts.push(tx);
                        message.serialize(&mut payload);
                    }
                }

                if message_count == 0 {
                    return Ok(());
                }

                let message = ProducerMessage {
                    payload,
                    num_messages_in_batch: Some(message_count as i32),
                    ..Default::default()
                };

                trace!("sending a batched message of size {}", message_count);
                let send_receipt = self.send_compress(message).await.map_err(Arc::new);
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
                let receipt = self.send_compress(message).await?;
                let _ = tx.send(Ok(receipt));
                Ok(SendFuture(rx))
            }
            Some(batch) => {
                let mut payload: Vec<u8> = Vec::new();
                let mut receipts = Vec::new();
                let mut counter = 0i32;

                {
                    let batch = batch.lock().await;
                    batch.push_back((tx, message)).await;

                    if batch.is_full().await {
                        for (tx, message) in batch.get_messages().await {
                            receipts.push(tx);
                            message.serialize(&mut payload);
                            counter += 1;
                        }
                    }
                }

                if counter > 0 {
                    let message = ProducerMessage {
                        payload,
                        num_messages_in_batch: Some(counter),
                        ..Default::default()
                    };

                    let send_receipt = self.send_compress(message).await.map_err(Arc::new);

                    trace!("sending a batched message of size {}", counter);
                    for tx in receipts.drain(..) {
                        let _ = tx.send(
                            send_receipt
                                .clone()
                                .map_err(|e| ProducerError::Batch(e).into()),
                        );
                    }
                }

                Ok(SendFuture(rx))
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_compress(
        &mut self,
        mut message: ProducerMessage,
    ) -> Result<proto::CommandSendReceipt, Error> {
        let compressed_message = match self.compression {
            None | Some(CompressionType::None) => message,
            Some(CompressionType::Lz4) => {
                #[cfg(not(feature = "lz4"))]
                return unimplemented!();

                #[cfg(feature = "lz4")]
                {
                    let compressed_payload: Vec<u8> =
                        lz4::block::compress(&message.payload[..], None, false)
                            .map_err(ProducerError::Io)?;

                    message.uncompressed_size = Some(message.payload.len() as u32);
                    message.payload = compressed_payload;
                    message.compression = Some(1);
                    message
                }
            }
            Some(CompressionType::Zlib) => {
                #[cfg(not(feature = "flate2"))]
                return unimplemented!();

                #[cfg(feature = "flate2")]
                {
                    let mut e =
                        flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                    e.write_all(&message.payload[..])
                        .map_err(ProducerError::Io)?;
                    let compressed_payload = e.finish().map_err(ProducerError::Io)?;

                    message.uncompressed_size = Some(message.payload.len() as u32);
                    message.payload = compressed_payload;
                    message.compression = Some(2);
                    message
                }
            }
            Some(CompressionType::Zstd) => {
                #[cfg(not(feature = "zstd"))]
                return unimplemented!();

                #[cfg(feature = "zstd")]
                {
                    let compressed_payload =
                        zstd::encode_all(&message.payload[..], 0).map_err(ProducerError::Io)?;
                    message.uncompressed_size = Some(message.payload.len() as u32);
                    message.payload = compressed_payload;
                    message.compression = Some(3);
                    message
                }
            }
            Some(CompressionType::Snappy) => {
                #[cfg(not(feature = "snap"))]
                return unimplemented!();

                #[cfg(feature = "snap")]
                {
                    let compressed_payload: Vec<u8> = Vec::new();
                    let mut encoder = snap::write::FrameEncoder::new(compressed_payload);
                    encoder
                        .write(&message.payload[..])
                        .map_err(ProducerError::Io)?;
                    let compressed_payload = encoder
                        .into_inner()
                        //FIXME
                        .map_err(|e| {
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Snappy compression error: {:?}", e),
                            )
                        })
                        .map_err(ProducerError::Io)?;

                    message.uncompressed_size = Some(message.payload.len() as u32);
                    message.payload = compressed_payload;
                    message.compression = Some(4);
                    message
                }
            }
        };

        self.send_inner(compressed_message).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_inner(
        &mut self,
        message: ProducerMessage,
    ) -> Result<proto::CommandSendReceipt, Error> {
        loop {
            let msg = message.clone();
            match self
                .connection
                .sender()
                .send(self.id, self.name.clone(), self.message_id.get(), msg)
                .await
            {
                Ok(receipt) => return Ok(receipt),
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
        // Sender::send() method consumes the sender
        // as the sender is hold by the TopicProducer, there is no way to call send method
        // The lines below take the pointed sender and replace it by a new one bound to nothing
        // but as the TopicProducer sender is recreate below, there is no worry
        let (drop_signal, _) = oneshot::channel::<()>();
        let old_signal = std::mem::replace(&mut self.drop_signal, drop_signal);
        // This line ask for kill the previous errored producer
        let _ = old_signal.send(());

        let broker_address = self.client.lookup_topic(&self.topic).await?;
        let conn = self.client.manager.get_connection(&broker_address).await?;
        self.connection = conn;

        warn!(
            "Retry #0 -> reconnecting producer {:#} using connection {:#} to broker {:#} to topic {:#}",
            self.id,
            self.connection.id(),
            broker_address.url,
            self.topic
        );

        let topic = self.topic.clone();
        let batch_size = self.options.batch_size;

        let mut current_retries = 0u32;
        let start = std::time::Instant::now();
        let operation_retry_options = self.client.operation_retry_options.clone();

        loop {
            match self
                .connection
                .sender()
                .create_producer(
                    topic.clone(),
                    self.id,
                    Some(self.name.clone()),
                    self.options.clone(),
                )
                .await
                .map_err(|e| {
                    error!("TopicProducer::create_producer error[{}]: {:?}", line!(), e);
                    e
                }) {
                Ok(_success) => {
                    if current_retries > 0 {
                        let dur = (std::time::Instant::now() - start).as_secs();
                        log::info!(
                            "producer({}) success after {} retries over {} seconds",
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
                        warn!("create_producer({}) answered ServiceNotReady, retrying request after {}ms (max_retries = {:?}): {}",
                        topic, operation_retry_options.retry_delay.as_millis(),
                        operation_retry_options.max_retries, text.unwrap_or_default());

                        current_retries += 1;
                        self.client
                            .executor
                            .delay(operation_retry_options.retry_delay)
                            .await;

                        let addr = self.client.lookup_topic(&topic).await?;
                        self.connection = self.client.manager.get_connection(&addr).await?;

                        warn!(
                            "Retry #{} -> reconnecting producer {:#} using connection {:#} to broker {:#} to topic {:#}",
                            current_retries,
                            self.id,
                            self.connection.id(),
                            broker_address.url,
                            self.topic
                        );

                        continue;
                    } else {
                        error!("create_producer({}) reached max retries", topic);

                        return Err(ConnectionError::PulsarError(
                            Some(proto::ServerError::ServiceNotReady),
                            text,
                        )
                        .into());
                    }
                }
                Err(ConnectionError::PulsarError(Some(proto::ServerError::ProducerBusy), text)) => {
                    if operation_retry_options.max_retries.is_none()
                        || operation_retry_options.max_retries.unwrap() > current_retries
                    {
                        warn!("create_producer({}) answered ProducerBusy, retrying request after {}ms (max_retries = {:?}): {}",
                        topic, operation_retry_options.retry_delay.as_millis(),
                        operation_retry_options.max_retries, text.unwrap_or_default());

                        current_retries += 1;
                        self.client
                            .executor
                            .delay(operation_retry_options.retry_delay)
                            .await;

                        let addr = self.client.lookup_topic(&topic).await?;
                        self.connection = self.client.manager.get_connection(&addr).await?;

                        warn!(
                            "Retry #{} -> reconnecting producer {:#} using connection {:#} to broker {:#} to topic {:#}",
                            current_retries,
                            self.id,
                            self.connection.id(),
                            broker_address.url,
                            self.topic
                        );

                        continue;
                    } else {
                        error!("create_producer({}) reached max retries", topic);

                        return Err(ConnectionError::PulsarError(
                            Some(proto::ServerError::ProducerBusy),
                            text,
                        )
                        .into());
                    }
                }
                Err(ConnectionError::Io(e)) => {
                    if e.kind() != std::io::ErrorKind::TimedOut {
                        error!("send_inner got io error: {:?}", e);
                        return Err(ProducerError::Connection(ConnectionError::Io(e)).into());
                    } else if operation_retry_options.max_retries.is_none()
                        || operation_retry_options.max_retries.unwrap() > current_retries
                    {
                        warn!("create_producer({}) TimedOut, retrying request after {}ms (max_retries = {:?})",
                            topic, operation_retry_options.retry_delay.as_millis(), operation_retry_options.max_retries);

                        current_retries += 1;
                        self.client
                            .executor
                            .delay(operation_retry_options.retry_delay)
                            .await;

                        let addr = self.client.lookup_topic(&topic).await?;
                        self.connection = self.client.manager.get_connection(&addr).await?;

                        warn!(
                            "Retry #{} -> reconnecting producer {:#} using connection {:#} to broker {:#} to topic {:#}",
                            current_retries,
                            self.id,
                            self.connection.id(),
                            broker_address.url,
                            self.topic
                        );

                        continue;
                    } else {
                        error!("create_producer({}) reached max retries", topic);
                        return Err(Error::Connection(ConnectionError::Io(e)));
                    }
                }
                Err(e) => {
                    error!("reconnect error[{:?}]: {:?}", line!(), e);
                    return Err(Error::Connection(e));
                }
            }
        }

        // drop_signal will be dropped when the TopicProducer is dropped, then
        // drop_receiver will return, and we can close the producer
        let (_drop_signal, drop_receiver) = oneshot::channel::<()>();
        let batch = batch_size.map(Batch::new).map(Mutex::new);
        let conn = self.connection.clone();
        let producer_id = self.id;
        let _ = self.client.executor.spawn(Box::pin(async move {
            let _res = drop_receiver.await;
            let _ = conn.sender().close_producer(producer_id).await;
        }));

        self.batch = batch;
        self.drop_signal = _drop_signal;

        Ok(())
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
                        let conn = pulsar.manager.get_connection(&addr).await?;
                        let producer =
                            TopicProducer::from_connection(pulsar, conn, topic, name, options)
                                .await?;
                        Ok::<_, Error>(producer)
                    }
                }),
        )
        .await?;

        let producer = match producers.len() {
            0 => {
                return Err(Error::Custom(format!(
                    "Unexpected error: Partition lookup returned no topics for {}",
                    topic
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

struct Batch {
    pub length: u32,
    // put it in a mutex because the design of Producer requires an immutable TopicProducer,
    // so we cannot have a mutable Batch in a send_raw(&mut self, ...)
    #[allow(clippy::type_complexity)]
    pub storage: Mutex<
        VecDeque<(
            oneshot::Sender<Result<proto::CommandSendReceipt, Error>>,
            BatchedMessage,
        )>,
    >,
}

impl Batch {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(length: u32) -> Batch {
        Batch {
            length,
            storage: Mutex::new(VecDeque::with_capacity(length as usize)),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn is_full(&self) -> bool {
        self.storage.lock().await.len() >= self.length as usize
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn push_back(
        &self,
        msg: (
            oneshot::Sender<Result<proto::CommandSendReceipt, Error>>,
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
        self.storage.lock().await.push_back((tx, batched))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_messages(
        &self,
    ) -> Vec<(
        oneshot::Sender<Result<proto::CommandSendReceipt, Error>>,
        BatchedMessage,
    )> {
        self.storage.lock().await.drain(..).collect()
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
    pub fn with_ordering_key<S: Into<String>>(mut self, partition_key: S) -> Self {
        self.partition_key = Some(partition_key.into());
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
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn deliver_at(mut self, date: SystemTime) -> Result<Self, std::time::SystemTimeError> {
        self.deliver_at_time = Some(date.duration_since(UNIX_EPOCH)?.as_millis() as i64);
        Ok(self)
    }

    /// delays message deliver with this duration
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn delay(mut self, delay: Duration) -> Result<Self, std::time::SystemTimeError> {
        let date = SystemTime::now() + delay;
        println!(
            "current date: {}, deliver_at: {}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            date.duration_since(UNIX_EPOCH)?.as_millis()
        );
        self.deliver_at_time = Some(date.duration_since(UNIX_EPOCH)?.as_millis() as i64);
        Ok(self)
    }

    /// delivers the message at this date
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn event_time(mut self, event_time: u64) -> Self {
        self.event_time = Some(event_time);
        self
    }
}

impl<'a, T: SerializeMessage + Sized, Exe: Executor> MessageBuilder<'a, T, Exe> {
    /// sends the message through the producer that created it
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn send(self) -> Result<SendFuture, Error> {
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
