use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::pin::Pin;
use std::io::Write;

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot;
use futures::{
    future::try_join_all,
    Future, FutureExt, TryFutureExt, Stream,
    task::{Context, Poll},
};
use rand;

use crate::client::SerializeMessage;
use crate::connection::{Authentication, Connection, SerialId};
use crate::error::ProducerError;
use crate::executor::Executor;
use crate::message::proto::{self, EncryptionKeys, Schema, CompressionType};
use crate::message::BatchedMessage;
use crate::{Error, Pulsar};

type ProducerId = u64;
type ProducerName = String;

#[derive(Debug, Clone, Default)]
pub struct Message {
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
    /// differentiate single and batch message metadata
    pub num_messages_in_batch: ::std::option::Option<i32>,
    /// the timestamp that this event occurs. it is typically set by applications.
    /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    pub event_time: ::std::option::Option<u64>,
    /// Contains encryption key name, encrypted key and metadata to describe the key
    pub encryption_keys: ::std::vec::Vec<EncryptionKeys>,
    /// Algorithm used to encrypt data key
    pub encryption_algo: ::std::option::Option<String>,
    /// Additional parameters required by encryption
    pub encryption_param: ::std::option::Option<Vec<u8>>,
    pub schema_version: ::std::option::Option<Vec<u8>>,
}

#[derive(Clone, Default)]
pub struct ProducerOptions {
    pub encrypted: Option<bool>,
    pub metadata: BTreeMap<String, String>,
    pub schema: Option<Schema>,
    pub batch_size: Option<u32>,
    pub compression: Option<proto::CompressionType>,
}

#[derive(Clone)]
pub struct Producer {
    message_sender: UnboundedSender<ProducerMessage>,
}

impl Producer {
    pub fn new(pulsar: Pulsar, options: ProducerOptions) -> Producer {
        let (tx, rx) = unbounded();
        let executor = pulsar.executor().clone();
        if let Err(_) = executor.spawn(Box::pin(ProducerEngine {
            pulsar,
            inbound: Box::pin(rx),
            producers: BTreeMap::new(),
            new_producers: BTreeMap::new(),
            producer_options: options,
        }.map(|res| trace!("ProducerEngine returned {:?}", res)))) {
            error!("the executor could not spawn the Producer engine future");
        }
        Producer { message_sender: tx }
    }

    pub async fn send<T: SerializeMessage + Sized, S: Into<String>>(
        &self,
        topic: S,
        message: T,
    ) -> Result<proto::CommandSendReceipt, Error> {
        let topic = topic.into();
        match T::serialize_message(&message) {
            Ok(message) => self.send_message(topic, message).await,
            Err(e) => Err(e),
        }
    }

    pub async fn send_all<'a, 'b, T, S, I>(
        &self,
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
                    v.push(self.send_message(topic.clone(), m));
                }

                try_join_all(v).await
            }
            Err(e) => Err(e),
        }
    }

    pub async fn send_message<S: Into<String>>(
        &self,
        topic: S,
        message: Message,
    ) ->  Result<proto::CommandSendReceipt, Error> {
        let (resolver, future) = oneshot::channel();
        match self.message_sender.unbounded_send(ProducerMessage {
            topic: topic.into(),
            message,
            resolver,
        }) {
            Ok(_) => {
                match future.await {
                    Ok(Ok(data)) => Ok(data),
                    Ok(Err(e)) => Err(e),
                    Err(oneshot::Canceled) => Err(ProducerError::Custom(
                        "Unexpected error: pulsar producer engine unexpectedly dropped".to_owned(),
                    )
                    .into()),
                }
            },
            Err(_) => Err(
                ProducerError::Custom(
                    "Unexpected error: pulsar producer engine unexpectedly dropped".to_owned(),
                )
                .into(),
            ),
        }
    }
}

struct ProducerEngine {
    pulsar: Pulsar,
    inbound: Pin<Box<UnboundedReceiver<ProducerMessage>>>,
    producers: BTreeMap<String, Arc<TopicProducer>>,
    new_producers: BTreeMap<String, Pin<Box<oneshot::Receiver<Result<Arc<TopicProducer>, Error>>>>>,
    producer_options: ProducerOptions,
}

impl Future for ProducerEngine {
    type Output = Result<(), ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        if !self.new_producers.is_empty() {
            let mut resolved_topics = Vec::new();
            let mut received_producers = vec![];
            for (topic, producer) in self.new_producers.iter_mut() {
                match producer.as_mut().poll(cx) {
                    Poll::Ready(Ok(Ok(producer))) => {
                        received_producers.push(producer);
                        resolved_topics.push(topic.clone());
                    }
                    Poll::Ready(Ok(Err(_))) | Poll::Ready(Err(_)) => resolved_topics.push(topic.clone()),
                    Poll::Pending => {}
                }
            }

            for producer in received_producers.drain(..) {
                self.producers.insert(producer.topic().to_owned(), producer);
            }
            for topic in resolved_topics {
                self.new_producers.remove(&topic);
            }
        }

        loop {
            let r = match self.inbound.as_mut().poll_next(cx) {
              Poll::Pending => return Poll::Pending,
              Poll::Ready(t) => t,
            };

            match r {
                Some(ProducerMessage {
                    topic,
                    message,
                    resolver,
                }) => {
                    match self.producers.get(&topic) {
                        Some(producer) => {
                            let p = producer.clone();
                            let f = async move {
                                p
                                    .send_raw(message)
                                    .map(|r| {
                                        if let Err(res) = resolver.send(r) {
                                          error!("send_message result receiver was dropped before getting the receipt: {:?}", res);
                                        }
                                    }).await;
                            };
                            if let Err(_) = self.pulsar.executor().spawn(Box::pin(f)) {
                                error!("the executor could not spawn the message sending future");
                            }
                        }
                        None => {
                            let pending = self.new_producers.remove(&topic).unwrap_or_else(|| {
                                let (tx, rx) = oneshot::channel::<Result<Arc<TopicProducer>, Error>>();
                                let pulsar = self.pulsar.clone();
                                let topic = topic.clone();
                                let producer_options = self.producer_options.clone();
                                let f = async move {
                                   let r = pulsar
                                        .create_producer(
                                            topic,
                                            None,
                                            producer_options,
                                        ).await;

                                   if let Err(_) = tx.send(r.map(Arc::new)) {
                                       error!("create_producer result receiver was dropped before getting the prducer");
                                   }
                                };
                                if let Err(_) = self.pulsar.executor().spawn(Box::pin(f)) {
                                    error!("the executor could not spawn the producer creation future");
                                }
                                Box::pin(rx)
                            });
                            let (tx, rx) = oneshot::channel::<Result<Arc<TopicProducer>, Error>>();
                            let f = async {
                                match pending
                                    .await {
                                    Ok(Ok(producer)) => {
                                        let _ = tx.send(Ok(producer.clone()));
                                        let r = producer
                                            .send_raw(message).await;

                                        if let Err(res) = resolver.send(r) {
                                          error!("send_message result receiver was dropped before getting the receipt: {:?}", res);
                                        }
                                    },
                                    Ok(Err(e)) => {
                                        let _ = resolver.send(Err(e));
                                        // TODO find better error propagation here
                                        // we send an error to tx to signal that the new producer
                                        // failed, since tx is added to new_producers in all cases
                                        let _ = tx.send(Err(Error::Producer(
                                                    ProducerError::Custom("producer creation failed".to_string()),
                                                    )));
                                    }
                                    Err(_) => {
                                        let _ = resolver.send(Err(Error::Producer(
                                                    ProducerError::Custom("producer creation failed".to_string()),
                                                    )));
                                        // TODO find better error propagation here
                                        // we send an error to tx to signal that the new producer
                                        // failed, since tx is added to new_producers in all cases
                                        let _ = tx.send(Err(Error::Producer(
                                                    ProducerError::Custom("()".to_string()),
                                                    )));
                                    }
                                }
                            };
                            tokio::spawn(f);
                            self.new_producers.insert(topic, Box::pin(rx));
                        }
                    }
                }
                None => return Poll::Ready(Ok(())),
            }
        }
    }
}

struct ProducerMessage {
    topic: String,
    message: Message,
    resolver: oneshot::Sender<Result<proto::CommandSendReceipt, Error>>,
}

pub struct TopicProducer {
    connection: Arc<Connection>,
    id: ProducerId,
    name: ProducerName,
    topic: String,
    message_id: SerialId,
    //putting it in a mutex because we must send multiple messages at once
    // while we might be pushing more messages from elsewhere
    batch: Option<Mutex<Batch>>,
    compression: Option<proto::CompressionType>,
}

impl TopicProducer {
    pub fn new<S1, S2, E: Executor+'static>(
        addr: S1,
        topic: S2,
        name: Option<String>,
        auth: Option<Authentication>,
        proxy_to_broker_url: Option<String>,
        options: ProducerOptions,
        executor: E,
    ) -> impl Future<Output = Result<TopicProducer, Error>>
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Connection::new(addr.into(), auth, proxy_to_broker_url, executor)
            .map_err(|e| e.into())
            .and_then(move |conn| {
                TopicProducer::from_connection::<_>(Arc::new(conn), topic.into(), name, options)
            })
    }

    pub async fn from_connection<S: Into<String>>(
        connection: Arc<Connection>,
        topic: S,
        name: Option<String>,
        options: ProducerOptions,
    ) -> Result<TopicProducer, Error> {
        let topic = topic.into();
        let producer_id = rand::random();
        let sequence_ids = SerialId::new();

        let sender = connection.sender().clone();
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

        let success = sender
            .create_producer(topic.clone(), producer_id, name, options).await?;
        Ok(TopicProducer {
            connection,
            id: producer_id,
            name: success.producer_name,
            topic,
            message_id: sequence_ids,
            batch: batch_size.map(Batch::new).map(Mutex::new),
            compression,
        })
    }

    pub fn is_valid(&self) -> bool {
        self.connection.is_valid()
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub async fn check_connection(&self) -> Result<(), Error> {
        self.connection
            .sender()
            .send_ping().await?;
        Ok(())
    }

    pub async fn send<T: SerializeMessage + Sized>(
        &self,
        message: T,
    ) -> Result<proto::CommandSendReceipt, Error> {
        match T::serialize_message(&message) {
            Ok(message) => self.send_raw(message).await,
            Err(e) => Err(e),
        }
    }

    pub fn error(&self) -> Option<Error> {
        self.connection
            .error()
            .map(|e| ProducerError::Connection(e).into())
    }

    pub async fn send_raw(
        &self,
        message: Message,
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
                    let message = Message {
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
        &self,
        mut message: Message,
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

        self.connection
            .sender()
            .send(
                self.id,
                self.name.clone(),
                self.message_id.get(),
                compressed_message,
                ).await
            .map_err(|e| ProducerError::Connection(e).into())
    }
}

impl Drop for TopicProducer {
    fn drop(&mut self) {
        let _ = self.connection.sender().close_producer(self.id);
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

  pub fn push_back(&self, msg: (oneshot::Sender<proto::CommandSendReceipt>, Message)) {
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

  pub fn get_messages(&self) -> Vec<(oneshot::Sender<proto::CommandSendReceipt>, BatchedMessage)> {
      self.storage.lock().unwrap().drain(..).collect()
  }
}
