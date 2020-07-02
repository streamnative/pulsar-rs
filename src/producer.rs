//! Message publication
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::io::Write;
use futures::channel::oneshot;
use rand;

use crate::client::SerializeMessage;
use crate::connection::{Connection, SerialId};
use crate::error::{ProducerError, ConnectionError};
use crate::executor::Executor;
use crate::message::proto::{self, EncryptionKeys, Schema, CompressionType};
use crate::message::BatchedMessage;
use crate::{Error, Pulsar};

type ProducerId = u64;
type ProducerName = String;

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
    pub properties: HashMap<String, String>,
    /// key to decide partition for the msg
    pub partition_key: ::std::option::Option<String>,
    /// Override namespace's replication
    pub replicate_to: ::std::vec::Vec<String>,
    /// the timestamp that this event occurs. it is typically set by applications.
    /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    pub event_time: ::std::option::Option<u64>,
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
}

impl From<Message> for ProducerMessage {
    fn from(m: Message) -> Self {
        ProducerMessage {
            payload: m.payload,
            properties: m.properties,
            partition_key: m.partition_key,
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
    pub encrypted: Option<bool>,
    pub metadata: BTreeMap<String, String>,
    pub schema: Option<Schema>,
    pub batch_size: Option<u32>,
    pub compression: Option<proto::CompressionType>,
}

/// Wrapper structure that manges multiple producers at once, creating them as needed
pub struct MultiTopicProducer<Exe: Executor + ?Sized> {
    client: Pulsar<Exe>,
    producers: BTreeMap<String, Producer<Exe>>,
    options: ProducerOptions,
}

impl<Exe: Executor + ?Sized> MultiTopicProducer<Exe> {
    pub fn new(client: Pulsar<Exe>, options: ProducerOptions) -> MultiTopicProducer<Exe> {
        MultiTopicProducer {
            client,
            producers: BTreeMap::new(),
            options,
        }
    }

    pub fn options(&self) -> &ProducerOptions {
        &self.options
    }

    pub fn topics(&self) -> Vec<String> {
        self.producers.keys().cloned().collect()
    }

    pub async fn send<T: SerializeMessage + Sized, S: Into<String>>(
        &mut self,
        topic: S,
        message: T,
    ) -> Result<proto::CommandSendReceipt, Error> {
        let topic = topic.into();
        match T::serialize_message(&message) {
            Ok(message) => self.send_message(topic, message.into()).await,
            Err(e) => Err(e),
        }
    }

    async fn send_message<S: Into<String>>(
        &mut self,
        topic: S,
        message: ProducerMessage,
    ) ->  Result<proto::CommandSendReceipt, Error> {
        let topic = topic.into();
        if !self.producers.contains_key(&topic) {
            let producer = self.client.create_producer(&topic, None, self.options.clone()).await?;
            self.producers.insert(topic.clone(), producer);
        }

        let producer = self.producers.get_mut(&topic).unwrap();
        producer.send_raw(message).await
    }

    pub async fn send_all<'a, 'b, T, S, I>(
        &mut self,
        topic: S,
        messages: I,
    ) -> Result<Vec<proto::CommandSendReceipt>, Error>
    where
        'b: 'a,
        T: 'b + SerializeMessage + ?Sized,
        I: IntoIterator<Item = &'a T>,
        S: Into<String>,
    {
        let topic = topic.into();
        // TODO determine whether to keep this approach or go with the partial send, but more mem friendly lazy approach.
        // serialize all messages before sending to avoid a partial send
        match messages
            .into_iter()
            .map(|m| T::serialize_message(m))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(messages) => {
                let mut v = vec![];
                for m in messages.into_iter() {
                    v.push(self.send_message(topic.clone(), m.into()).await?);
                }

                Ok(v)
            }
            Err(e) => Err(e),
        }
    }
}

/// a producer is used to publish messages on a topic
pub struct Producer<Exe: Executor + ?Sized> {
    client: Pulsar<Exe>,
    connection: Arc<Connection>,
    id: ProducerId,
    name: ProducerName,
    topic: String,
    message_id: SerialId,
    //putting it in a mutex because we must send multiple messages at once
    // while we might be pushing more messages from elsewhere
    batch: Option<Mutex<Batch>>,
    compression: Option<proto::CompressionType>,
    _drop_signal: oneshot::Sender<()>,
    options: ProducerOptions,
}

impl<Exe: Executor + ?Sized> Producer<Exe> {
    pub async fn from_connection<S: Into<String>>(
        client: Pulsar<Exe>,
        connection: Arc<Connection>,
        topic: S,
        name: Option<String>,
        options: ProducerOptions,
    ) -> Result<Producer<Exe>, Error> {
        let topic = topic.into();
        let producer_id = rand::random();
        let sequence_ids = SerialId::new();

        let _ = connection
            .sender()
            .lookup_topic(topic.clone(), false).await?;

        let topic = topic.clone();
        let batch_size = options.batch_size.clone();
        let compression = options.compression.clone();

        match compression {
            None | Some(CompressionType::None) => {},
            Some(CompressionType::Lz4) => {
                #[cfg(not(feature = "lz4"))]
                return Err(Error::Custom("cannot create a producer with LZ4 compression because the 'lz4' cargo feature is not active".to_string()));
            },
            Some(CompressionType::Zlib) => {
                #[cfg(not(feature = "flate2"))]
                return Err(Error::Custom("cannot create a producer with zlib compression because the 'flate2' cargo feature is not active".to_string()));
            },
            Some(CompressionType::Zstd) => {
                #[cfg(not(feature = "zstd"))]
                return Err(Error::Custom("cannot create a producer with zstd compression because the 'zstd' cargo feature is not active".to_string()));
            },
            Some(CompressionType::Snappy) => {
                #[cfg(not(feature = "snap"))]
                return Err(Error::Custom("cannot create a producer with Snappy compression because the 'snap' cargo feature is not active".to_string()));
            },
            //Some() => unimplemented!(),
        };

        let success = connection.sender()
            .create_producer(topic.clone(), producer_id, name, options.clone()).await?;

        // drop_signal will be dropped when the TopicProducer is dropped, then
        // drop_receiver will return, and we can close the producer
        let (_drop_signal, drop_receiver) = oneshot::channel::<()>();
        let conn = connection.clone();
        let _ = Exe::spawn(Box::pin(async move {
            let _res = drop_receiver.await;
             let _ = conn.sender().close_producer(producer_id).await;
        }));

        Ok(Producer {
            client,
            connection,
            id: producer_id,
            name: success.producer_name,
            topic,
            message_id: sequence_ids,
            batch: batch_size.map(Batch::new).map(Mutex::new),
            compression,
            _drop_signal,
            options,
        })
    }

    pub fn is_valid(&self) -> bool {
        self.connection.is_valid()
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn options(&self) -> &ProducerOptions {
        &self.options
    }

    pub fn create_message(&mut self) -> MessageBuilder<Unset, Exe> {
        MessageBuilder::new(self)
    }

    pub async fn check_connection(&self) -> Result<(), Error> {
        self.connection
            .sender()
            .send_ping().await?;
        Ok(())
    }

    pub async fn send<T: SerializeMessage + Sized>(
        &mut self,
        message: T,
    ) -> Result<proto::CommandSendReceipt, Error> {
        match T::serialize_message(&message) {
            Ok(message) => self.send_raw(message.into()).await,
            Err(e) => Err(e),
        }
    }

    pub async fn send_all<'a, 'b, T, I>(
        &mut self,
        messages: I,
    ) -> Result<Vec<proto::CommandSendReceipt>, Error>
    where
        'b: 'a,
        T: 'b + SerializeMessage + ?Sized,
        I: IntoIterator<Item = &'a T>,
    {
        // TODO determine whether to keep this approach or go with the partial send, but more mem friendly lazy approach.
        // serialize all messages before sending to avoid a partial send
        match messages
            .into_iter()
            .map(|m| T::serialize_message(m))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(messages) => {
                let mut v = vec![];
                for m in messages.into_iter() {
                    v.push(self.send_raw(m.into()).await?);
                }

                Ok(v)
            }
            Err(e) => Err(e),
        }
    }

    pub fn error(&self) -> Option<Error> {
        self.connection
            .error()
            .map(|e| ProducerError::Connection(e).into())
    }

    pub(crate) async fn send_raw(
        &mut self,
        message: ProducerMessage,
    ) -> Result<proto::CommandSendReceipt, Error> {
        match self.batch.as_ref() {
            None => {
                self.send_compress(message).await
            },
            Some(batch) => {
                let (tx, rx) = oneshot::channel();
                let mut payload: Vec<u8> = Vec::new();
                let mut receipts = Vec::new();
                let mut counter = 0i32;

                {
                    let batch = batch.lock().unwrap();
                    batch.push_back((tx, message));

                    if batch.is_full() {
                        for (tx, message) in batch.get_messages() {
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

                    let send_receipt = self.send_compress(message).await?;

                    trace!("sending a batched message of size {}", counter);
                    for tx in receipts.drain(..) {
                        let _ = tx.send(send_receipt.clone());
                    }
                }
                rx.await.map_err(|_| ProducerError::Custom("could not send message".to_string()).into())
            }


        }
    }

    async fn send_compress(
        &mut self,
        mut message: ProducerMessage,
    ) -> Result<proto::CommandSendReceipt, Error> {
        let compressed_message = match self.compression {
            None | Some(CompressionType::None) => {
                message
            },
            Some(CompressionType::Lz4) => {
                #[cfg(not(feature = "lz4"))]
                return unimplemented!();

                #[cfg(feature = "lz4")]
                {
                    let v: Vec<u8> = Vec::new();
                    let mut encoder = lz4::EncoderBuilder::new().build(v).map_err(ProducerError::Io)?;
                    encoder.write(&message.payload[..]).map_err(ProducerError::Io)?;
                    let (compressed_payload, result) = encoder.finish();

                    result.map_err(ProducerError::Io)?;
                    message.payload = compressed_payload;
                    message.compression = Some(1);
                    message
                }
            },
            Some(CompressionType::Zlib) => {
                #[cfg(not(feature = "flate2"))]
                return unimplemented!();

                #[cfg(feature = "flate2")]
                {
                    let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                    e.write_all(&message.payload[..]).map_err(ProducerError::Io)?;
                    let compressed_payload = e.finish().map_err(ProducerError::Io)?;

                    message.payload = compressed_payload;
                    message.compression = Some(2);
                    message
                }
            },
            Some(CompressionType::Zstd) => {
                #[cfg(not(feature = "zstd"))]
                return unimplemented!();

                #[cfg(feature = "zstd")]
                {
                    let compressed_payload = zstd::encode_all(&message.payload[..], 0).map_err(ProducerError::Io)?;
                    message.compression = Some(3);
                    message.payload = compressed_payload;
                    message
                }
            },
            Some(CompressionType::Snappy) => {
                #[cfg(not(feature = "snap"))]
                return unimplemented!();

                #[cfg(feature = "snap")]
                {
                    let compressed_payload: Vec<u8> = Vec::new();
                    let mut encoder = snap::write::FrameEncoder::new(compressed_payload);
                    encoder.write(&message.payload[..]).map_err(ProducerError::Io)?;
                    let compressed_payload = encoder.into_inner()
                        //FIXME
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other,
                                                         format!("Snappy compression error: {:?}", e)))
                        .map_err(ProducerError::Io)?;

                    message.payload = compressed_payload;
                    message.compression = Some(4);
                    message
                }
            },
        };

        self.send_inner(compressed_message).await
    }

    async fn send_inner(
        &mut self,
        message: ProducerMessage,
    ) -> Result<proto::CommandSendReceipt, Error> {
        let msg = message.clone();
        match self.connection
            .sender()
            .send(
                self.id,
                self.name.clone(),
                self.message_id.get(),
                message,
                ).await {
                Ok(receipt) => return Ok(receipt),
                Err(ConnectionError::Disconnected) => {},
                Err(e) => {
                    error!("send_inner got error: {:?}", e);
                    return Err(ProducerError::Connection(e).into());
                }
            };

        error!("send_inner disconnected");
        self.reconnect().await?;

        match self.connection
            .sender()
            .send(
                self.id,
                self.name.clone(),
                self.message_id.get(),
                msg,
                ).await {
                Ok(receipt) => return Ok(receipt),
                Err(e) => {
                    error!("send_inner got error: {:?}", e);
                    return Err(ProducerError::Connection(e).into());
                }
            }

    }

    async fn reconnect(&mut self) -> Result<(), Error> {
        debug!("reconnecting producer for topic: {}", self.topic);
        let broker_address = self.client.lookup_topic(&self.topic).await?;
        let conn = self.client.manager.get_connection(&broker_address).await?;

        self.connection = conn;

        let topic = self.topic.clone();
        let batch_size = self.options.batch_size.clone();

        let _ = self.connection.sender()
            .create_producer(topic.clone(), self.id.clone(), Some(self.name.clone()), self.options.clone()).await?;

        // drop_signal will be dropped when the TopicProducer is dropped, then
        // drop_receiver will return, and we can close the producer
        let (_drop_signal, drop_receiver) = oneshot::channel::<()>();
        let batch =  batch_size.map(Batch::new).map(Mutex::new);
        let conn = self.connection.clone();
        let producer_id = self.id.clone();
        let _ = Exe::spawn(Box::pin(async move {
            let _res = drop_receiver.await;
             let _ = conn.sender().close_producer(producer_id).await;
        }));

        self.batch = batch;
        self._drop_signal = _drop_signal;

        Ok(())
    }
}

/// Helper structure to prepare a producer
///
/// generated from [Pulsar::producer]
pub struct ProducerBuilder<'a, Topic, Exe: Executor + ?Sized> {
    pulsar: &'a Pulsar<Exe>,
    topic: Topic,
    name: Option<String>,
    producer_options: Option<ProducerOptions>,
}


impl<'a, Exe: Executor + ?Sized> ProducerBuilder<'a, Set<String>, Exe> {
    pub async fn build(self) -> Result<Producer<Exe>, Error> {
        let ProducerBuilder {
            pulsar,
            topic: Set(topic),
            name,
            producer_options,
        } = self;

        pulsar
            .create_producer(topic, name, producer_options.unwrap_or_default())
            .await
    }
}

use crate::consumer::{Set, Unset};
impl<'a, Exe: Executor + ?Sized> ProducerBuilder<'a, Unset, Exe> {
    pub fn new(pulsar: &'a Pulsar<Exe>) -> Self {
        ProducerBuilder {
            pulsar,
            topic: Unset,
            name: None,
            producer_options: None,
        }
    }
}

impl<'a, Exe: Executor + ?Sized> ProducerBuilder<'a, Unset, Exe> {
    pub fn with_topic<S: Into<String>>(self, topic: S) -> ProducerBuilder<'a, Set<String>, Exe> {
        ProducerBuilder {
            pulsar: self.pulsar,
            topic: Set(topic.into()),
            name: self.name,
            producer_options: self.producer_options,
        }
    }
}

impl<'a, Topic, Exe: Executor + ?Sized> ProducerBuilder<'a, Topic, Exe> {
    pub fn with_name<S: Into<String>>(self, name: S) -> ProducerBuilder<'a, Topic, Exe> {
        ProducerBuilder {
            pulsar: self.pulsar,
            topic: self.topic,
            name: Some(name.into()),
            producer_options: self.producer_options,
        }
    }
}

impl<'a, Topic, Exe: Executor + ?Sized> ProducerBuilder<'a, Topic, Exe> {
    pub fn with_options(self, options: ProducerOptions) -> ProducerBuilder<'a, Topic, Exe> {
        ProducerBuilder {
            pulsar: self.pulsar,
            topic: self.topic,
            name: self.name,
            producer_options: Some(options),
        }
    }
}

struct Batch {
    pub length: u32,
    // put it in a mutex because the design of Producer requires an immutable TopicProducer,
    // so we cannot have a mutable Batch in a send_raw(&mut self, ...)
    pub storage: Mutex<VecDeque<(oneshot::Sender<proto::CommandSendReceipt>, BatchedMessage)>>,
}

impl Batch {
    pub fn new(length: u32) -> Batch {
        Batch {
            length,
            storage: Mutex::new(VecDeque::with_capacity(length as usize)),
        }
    }

    pub fn is_full(&self) -> bool {
        self.storage.lock().unwrap().len() >= self.length as usize
    }

    pub fn push_back(&self, msg: (oneshot::Sender<proto::CommandSendReceipt>, ProducerMessage)) {
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
                payload_size: message.payload.len() as i32,
                ..Default::default()
            },
            payload: message.payload,
        };
        self.storage.lock().unwrap().push_back((tx, batched))
    }

    pub fn get_messages(
        &self,
    ) -> Vec<(oneshot::Sender<proto::CommandSendReceipt>, BatchedMessage)> {
        self.storage.lock().unwrap().drain(..).collect()
    }
}

/// Helper structure to prepare a message
///
/// generated with [Producer::create_message]
pub struct MessageBuilder<'a, Content, Exe: Executor + ?Sized> {
    producer: &'a mut Producer<Exe>,
    properties: HashMap<String, String>,
    partition_key: Option<String>,
    content: Content,
}

impl<'a, Exe: Executor + ?Sized> MessageBuilder<'a, Unset, Exe> {
    pub fn new(producer: &'a mut Producer<Exe>) -> Self {
        MessageBuilder {
            producer,
            properties: HashMap::new(),
            partition_key: None,
            content: Unset,
        }
    }
}

impl<'a, Exe: Executor + ?Sized> MessageBuilder<'a, Unset, Exe> {
    pub fn with_content<T>(self, content: T) -> MessageBuilder<'a, Set<T>, Exe> {
        MessageBuilder {
            producer: self.producer,
            properties: self.properties,
            partition_key: self.partition_key,
            content: Set(content),
        }
    }
}

impl<'a, Content, Exe: Executor + ?Sized> MessageBuilder<'a, Content, Exe> {
    pub fn with_partition_key<S: Into<String>>(
        self,
        partition_key: S,
    ) -> MessageBuilder<'a, Content, Exe> {
        MessageBuilder {
            producer: self.producer,
            properties: self.properties,
            partition_key: Some(partition_key.into()),
            content: self.content,
        }
    }
}

impl<'a, Content, Exe: Executor + ?Sized> MessageBuilder<'a, Content, Exe> {
    pub fn with_property<S1: Into<String>, S2: Into<String>>(
        mut self,
        key: S1,
        value: S2,
    ) -> MessageBuilder<'a, Content, Exe> {
        self.properties.insert(key.into(), value.into());
        self
    }
}

impl<'a, T: SerializeMessage + Sized, Exe: Executor + ?Sized> MessageBuilder<'a, Set<T>, Exe> {
    pub async fn send(self) -> Result<proto::CommandSendReceipt, Error> {
        let MessageBuilder {
            producer,
            properties,
            partition_key,
            content: Set(content),
        } = self;

        let mut message = T::serialize_message(&content)?;
        message.properties = properties;
        message.partition_key = partition_key;
        producer.send_raw(message.into()).await
    }
}
