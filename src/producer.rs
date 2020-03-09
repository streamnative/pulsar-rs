//use std::collections::BTreeMap;
//use std::collections::HashMap;
//use std::sync::Arc;
//
//use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
//use futures::sync::oneshot;
//use futures::{
//    future::{self, Either},
//    Future,
//};
//use rand;
//use tokio::prelude::*;
//use tokio::runtime::TaskExecutor;
//
//use crate::client::SerializeMessage;
//use crate::connection::{Authentication, Connection, SerialId};
//use crate::error::ProducerError;
use crate::proto::proto::{self, EncryptionKeys, Schema};
use std::collections::{HashMap, BTreeMap};
use regex::Regex;
use crate::connection::ConnectionHandle;
use crate::resolver::Resolver;
use futures::channel::mpsc;
use crate::connection_manager::ConnectionManagerHandle;
use futures::task::{Context, Poll};
use crate::error::Error;
use futures::Future;
use crate::message::{Send, SendReceipt};
use crate::util::SerialId;
use crate::client::SerializeMessage;
use crate::proto::CompressionType;
use std::mem;
use flate2::Compression;
use std::io::{Read, Write};
use tokio::macros::support::Pin;
use crate::proto::proto::command_lookup_topic_response::LookupType::Redirect;
use futures::lock::Mutex;
use std::sync::Arc;

//use crate::{Error, Pulsar};
//
//type ProducerId = u64;
//type ProducerName = String;
//
#[derive(Debug, Clone, Default)]
pub struct Message {
    pub payload: Vec<u8>,
    pub properties: Option<HashMap<String, String>>,
    ///key to decide partition for the msg
    pub partition_key: Option<String>,
    /// Override namespace's replication
    pub replicate_to: Vec<String>,
    pub compression: Option<CompressionType>,
    pub uncompressed_size: Option<u32>,
    /// Removed below checksum field from Metadata as
    /// it should be part of send-command which keeps checksum of header + payload
    ///optional sfixed64 checksum = 10;
    /// differentiate single and batch message metadata
    pub num_messages_in_batch: Option<i32>,
    /// the timestamp that this event occurs. it is typically set by applications.
    /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    pub event_time: Option<u64>,
    /// Contains encryption key name, encrypted key and metadata to describe the key
    pub encryption_keys: Vec<EncryptionKeys>,
    /// Algorithm used to encrypt data key
    pub encryption_algo: Option<String>,
    /// Additional parameters required by encryption
    pub encryption_param: Option<Vec<u8>>,
    pub schema_version: Option<Vec<u8>>,
}

impl Message {
    fn compress(mut self, compression: CompressionType) -> Self {
        match compression {
            CompressionType::None => self,
            CompressionType::Lz4 => {
                self.payload = lz4_compress::compress(&self.payload);
                self.compression = Some(compression);
                self
            }
            CompressionType::Zlib => {
                let mut bytes = Vec::new();
                let mut encoder = flate2::write::ZlibEncoder::new(bytes, Compression::default());
                encoder.write_all(&self.payload);
                // This is safe, as the `Read` impl of &[u8] and the `Write` impl of `Vec<u8>` both
                // never return errors
                self.payload = encoder.finish().unwrap();
                self.compression = Some(compression);
                self
            }
        }
    }
    fn uncompress(mut self) -> Result<Self, Error> {
        match self.compression {
            Some(CompressionType::None) | None => Ok(self),
            Some(CompressionType::Lz4) => {
                let bytes = lz4_compress::decompress(&self.payload)?;
                self.payload = bytes;
                self.compression = None;
                Ok(self)
            }
            Some(CompressionType::Zlib) => {
                let mut decoder = flate2::read::ZlibDecoder::new(self.payload.as_slice());
                let mut bytes = Vec::new();
                decoder.read_to_end(&mut bytes)?;
                self.payload = bytes;
                self.compression = None;
                Ok(self)
            }
        }
    }
}

pub struct TopicPartition {
    pub root_topic: String,
    pub partition_topic: String,
    pub partition_number: u64,
}

impl TopicPartition {
    pub fn new(topic: String, partition_number: u64) -> TopicPartition {
        TopicPartition {
            partition_topic: format!("{}-partition-{}", &topic, partition_number),
            root_topic: topic,
            partition_number,
        }
    }
}

pub trait RouteMessage {
    /// Caller must guarantee partitions is non-empty.
    fn select_partition(&mut self, message: &Message, partitions: &[TopicPartition]) -> &TopicPartition;
}

pub struct RoundRobinRouter {
    next_partition: usize,
}

impl Default for RoundRobinRouter {
    fn default() -> Self {
        RoundRobinRouter { next_partition: 0 }
    }
}

impl RouteMessage for RoundRobinRouter {
    fn select_partition(&mut self, _: &Message, partitions: &[TopicPartition]) -> &TopicPartition {
        let mut partition_index = self.next_partition;
        if partition_index >= partitions.len() {
            partition_index = 0
        }
        self.next_partition = partition_index + 1;
        &partitions[partition_index]
    }
}

pub struct SinglePartitionRouter {
    partition: Option<usize>
}

impl RouteMessage for SinglePartitionRouter {
    fn select_partition(&mut self, _: &Message, partitions: &[TopicPartition]) -> &TopicPartition {
        if self.partition.is_none() || self.partition >= partitions.len() {
            self.partition = Some(rand::random() % partitions.len());
        }
        &partitions[self.partition.unwrap()]
    }
}

pub(crate) struct CreateProducer {
    pub topic: String,
    pub options: ProducerOptions,
    pub resolver: Resolver<mpsc::UnboundedSender<Message>>,
}

struct SendMessage {
    message: Message,
    resolver: Resolver<Box<dyn Future<Output=Result<SendReceipt, Error>>>>,
}

struct PendingProducer {
    producer: Box<dyn Future<Output=Result<ProducerEngine, Error>>>,
    resolver: Resolver<Producer>,
}

pub struct Producers {
    producers: BTreeMap<u64, ProducerEngine>,
    pending: BTreeMap<u64, PendingProducer>,
    connections: ConnectionManagerHandle,
    producer_ids: Arc<Mutex<SerialId>>
}

impl Future for Producers {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut resolved =  BTreeMap::new();
        for (id, pending) in &mut self.pending {
            match pending.producer.poll() {
                Poll::Ready(Ok(producer)) => {
                    let handle = producer.handle();
                    resolved.insert(*id, Ok(handle));
                    self.producers.insert(*id, producer);
                },
                Poll::Ready(Err(e)) => {
                    resolved.insert(*id, Err(e));
                }
                Poll::Pending => {}
            }
        }
        for (id, handle) in resolved {
            self.pending.remove(&id).into_iter()
                .for_each(|p| {
                    p.resolver.resolve(handle);
                });
        }
        let mut shutdown = Vec::new();
        for (id, producer) in &mut self.producers {
            match producer.poll(cx) {
                Poll::Ready(()) => {
                    shutdown.push(*id);
                }
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }
}

impl Producers {
    fn create_producer(
        &mut self,
        topic: String,
        options: ProducerOptions,
        routing_mode: Option<Box<dyn RouteMessage>>,
        resolver: Resolver<Producer>,
    ) {
        let topic = topic.clone();
        let conn = self.connections.get_base_connection();
        let producer = async {
            let conn = conn.await?;
            let partitions = conn.lookup_partitioned_topic(topic.clone()).await?;
            if partitions.partitions > 1 {

            } else {
                let metdata = conn.lookup_topic(topic, None).await?;
                if metdata.proxy_through_service_url
            }
        }
        self.connections.get_base_connection()
            .and_then(move |c| c.lookup_topic())
    }
}

async fn connect_producer(
    conn_manager: ConnectionManagerHandle,
    topic: String,
    options: ProducerOptions,
    routing_mode: Option<Box<dyn RouteMessage>>,
    ids: Arc<Mutex<SerialId>>,
) -> Result<Vec<ProducerEngine>, Error> {
    let conn = conn_manager.get_base_connection().await?;
    let partitions = conn.lookup_partitioned_topic(topic.clone()).await?;
    let topics = if partitions.partitions > 1 {
        (0..partitions.partitions)
            .map(|p| format!("{}-partition-{}", &topic, p))
            .collect()
    } else {
        vec![topic]
    };
    topics.into_iter().map(|topic| async {
        let mut conn = conn.clone();
        let mut metadata = conn.lookup_topic(topic.clone(), None).await?;
        while metadata.response == TopicLookupType::Redirect {
            conn = conn_manager.get_conn(metadata.broker_service_url).await?;
            metadata = conn.lookup_topic(topic.clone(), Some(metadata.authoritative)).await?;
        }
        conn = conn_manager.get_conn(metadata.broker_service_url).await?;
        let id = ids.lock().await.next();
        let create_success = conn.create_producer(topic, id, options.clone()).await?;
        Ok()
    })
    Ok()
}

#[derive(Clone, Default)]
pub struct ProducerOptions {
    pub producer_name: Option<String>,
    pub encrypted: Option<bool>,
    pub metadata: Option<BTreeMap<String, String>>,
    pub schema: Option<Schema>,
}

pub(crate) struct PartitionProducer {
    name: String,
    topic: String,
    partition: TopicPartition,
    connection: ConnectionHandle,
    sequence_id: SerialId,
}

impl PartitionProducer {
    pub async fn send(&self, msg: SendMessage) -> Result<(), Error> {
        self.connection.s
    }
}


pub(crate) struct ProducerEngine {
    name: String,
    topic: String,
    options: ProducerOptions,
    message_routing_mode: Box<dyn RouteMessage>,
    partitions: Vec<TopicPartition>,
    connections: BTreeMap<u64, ConnectionHandle>,
    messages: mpsc::UnboundedReceiver<SendMessage>,
    handle: ProducerHandle,
    sequence_id: SerialId,
    shutdown: Option<Box<dyn Future<Output=()>>>,
}

impl ProducerEngine {
    fn new(
        id: u64,
        name: String,
        topic: String,
        options: ProducerOptions,
        routing_mode: Box<dyn RouteMessage>,
    )

    fn shutdown(&mut self) {
        self.shutdown = Some(future::join_all(
            self.connections.values()
                .map(|conn| conn.close_producer(self.id))
                .then(|_| Ok(()))
        ));
    }

    fn handle(&self) -> ProducerHandle {
        self.handle.clone()
    }
}

impl Future for ProducerEngine {
    type Output = ();

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(shutdown) = &mut self.shutdown {
            return shutdown.poll(cx);
        }
        let partitions: Vec<_> = self.connections.keys().collect();
        while let Poll::Ready(msg) = self.messages.poll_next(cx) {
            match ready!(self.messages.poll(cx)) {
                Some(SendMessage { message, resolver }) => {
                    let next_partition = self.message_routing_mode.select_partition(&message, &self.partitions);
                    let conn = self.connections.get(&next_partition.partition_number).unwrap();
                    let _ = resolver.resolve(Ok(Box::new(conn.send(Send {
                        producer_id: self.id,
                        producer_name: self.name.clone(),
                        sequence_id: self.sequence_id.next(),
                        num_messages: message.num_messages_in_batch, //Is this correct?
                        message
                    }))));
                },
                None => self.shutdown(),
            }
        }
        Poll::Pending
    }
}

#[derive(Clone)]
pub(crate) struct ProducerHandle {
    sender: mpsc::UnboundedSender<SendMessage>,
}

impl ProducerHandle {
    pub async fn send(&self, message: Message) -> Result<(), Error> {
        let (resolver, f) = Resolver::new();
        self.sender.unbounded_send(SendMessage::Send { message, resolver })
            .map_err(|e| Error::unexpected("pulsar engine is not running"))?;
        f.await?;
        Ok(())
    }

    pub fn close(&self) -> Result<(), Error> {
        self.sender.unbounded_send(SendMessage::Close)
            .map_err(|e| Error::unexpected("pulsar engine is not running"))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Producer {
    inner: ProducerHandle,
}

impl Producer {
    pub async fn send<T: SerializeMessage>(&self, msg: T) -> Result<(), Error> {
        let msg = T::serialize_message(msg)?;
        self.inner.send(msg)
    }
}

//
//#[derive(Clone)]
//pub struct Producer {
//    message_sender: UnboundedSender<ProducerMessage>,
//}
//
//impl Producer {
//    pub fn new(pulsar: Pulsar, options: ProducerOptions) -> Producer {
//        let (tx, rx) = unbounded();
//        let executor = pulsar.executor().clone();
//        executor.spawn(ProducerEngine {
//            pulsar,
//            inbound: rx,
//            producers: BTreeMap::new(),
//            new_producers: BTreeMap::new(),
//            producer_options: options,
//        });
//        Producer { message_sender: tx }
//    }
//
//    pub fn send<T: SerializeMessage, S: Into<String>>(
//        &self,
//        topic: S,
//        message: &T,
//    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> {
//        let topic = topic.into();
//        match T::serialize_message(message) {
//            Ok(message) => Either::A(self.send_message(topic, message)),
//            Err(e) => Either::B(future::failed(e.into())),
//        }
//    }
//
//    pub fn send_all<'a, 'b, T, S, I>(
//        &self,
//        topic: S,
//        messages: I,
//    ) -> impl Future<Item = Vec<proto::CommandSendReceipt>, Error = Error>
//    where
//        'b: 'a,
//        T: 'b + SerializeMessage,
//        I: IntoIterator<Item = &'a T>,
//        S: Into<String>,
//    {
//        let topic = topic.into();
//        // TODO determine whether to keep this approach or go with the partial send, but more mem friendly lazy approach.
//        // serialize all messages before sending to avoid a partial send
//        match messages
//            .into_iter()
//            .map(|m| T::serialize_message(m))
//            .collect::<Result<Vec<_>, _>>()
//        {
//            Ok(messages) => Either::A(future::collect(
//                messages
//                    .into_iter()
//                    .map(|m| self.send_message(topic.clone(), m))
//                    .collect::<Vec<_>>(),
//            )),
//            Err(e) => Either::B(future::failed(e.into())),
//        }
//    }
//
//    fn send_message<S: Into<String>>(
//        &self,
//        topic: S,
//        message: Message,
//    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> + 'static {
//        let (resolver, future) = oneshot::channel();
//        match self.message_sender.unbounded_send(ProducerMessage {
//            topic: topic.into(),
//            message,
//            resolver,
//        }) {
//            Ok(_) => Either::A(future.then(|r| {
//                match r {
//                    Ok(Ok(data)) => Ok(data),
//                    Ok(Err(e)) => Err(e.into()),
//                    Err(oneshot::Canceled) => Err(ProducerError::Custom(
//                        "Unexpected error: pulsar producer engine unexpectedly dropped".to_owned(),
//                    )
//                    .into()),
//                }
//            })),
//            Err(_) => Either::B(future::failed(
//                ProducerError::Custom(
//                    "Unexpected error: pulsar producer engine unexpectedly dropped".to_owned(),
//                )
//                .into(),
//            )),
//        }
//    }
//}
//
//struct ProducerEngine {
//    pulsar: Pulsar,
//    inbound: UnboundedReceiver<ProducerMessage>,
//    producers: BTreeMap<String, Arc<TopicProducer>>,
//    new_producers: BTreeMap<String, oneshot::Receiver<Result<Arc<TopicProducer>, Error>>>,
//    producer_options: ProducerOptions,
//}
//
//impl Future for ProducerEngine {
//    type Item = ();
//    type Error = ();
//
//    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
//        if !self.new_producers.is_empty() {
//            let mut resolved_topics = Vec::new();
//            for (topic, producer) in self.new_producers.iter_mut() {
//                match producer.poll() {
//                    Ok(Async::Ready(Ok(producer))) => {
//                        self.producers.insert(producer.topic().to_owned(), producer);
//                        resolved_topics.push(topic.clone());
//                    }
//                    Ok(Async::Ready(Err(_))) | Err(_) => resolved_topics.push(topic.clone()),
//                    Ok(Async::NotReady) => {}
//                }
//            }
//            for topic in resolved_topics {
//                self.new_producers.remove(&topic);
//            }
//        }
//
//        loop {
//            match try_ready!(self.inbound.poll()) {
//                Some(ProducerMessage {
//                    topic,
//                    message,
//                    resolver,
//                }) => {
//                    match self.producers.get(&topic) {
//                        Some(producer) => {
//                            tokio::spawn(
//                                producer
//                                    .send_message(message, None)
//                                    .then(|r| resolver.send(r).map_err(drop)),
//                            );
//                        }
//                        None => {
//                            let pending = self.new_producers.remove(&topic).unwrap_or_else(|| {
//                                let (tx, rx) = oneshot::channel();
//                                tokio::spawn({
//                                    self.pulsar
//                                        .create_producer(
//                                            topic.clone(),
//                                            None,
//                                            self.producer_options.clone(),
//                                        )
//                                        .then(|r| {
//                                            tx.send(r.map(|producer| Arc::new(producer)))
//                                                .map_err(drop)
//                                        })
//                                });
//                                rx
//                            });
//                            let (tx, rx) = oneshot::channel();
//                            tokio::spawn(pending.map_err(drop).and_then(move |r| match r {
//                                Ok(producer) => {
//                                    let _ = tx.send(Ok(producer.clone()));
//                                    Either::A(
//                                        producer
//                                            .send_message(message, None)
//                                            .then(|r| resolver.send(r))
//                                            .map_err(drop),
//                                    )
//                                }
//                                Err(e) => {
//                                    // TODO find better error propogation here
//                                    let _ = resolver.send(Err(Error::Producer(
//                                        ProducerError::Custom(e.to_string()),
//                                    )));
//                                    let _ = tx.send(Err(e));
//                                    Either::B(future::failed(()))
//                                }
//                            }));
//                            self.new_producers.insert(topic, rx);
//                        }
//                    }
//                }
//                None => return Ok(Async::Ready(())),
//            }
//        }
//    }
//}
//
//struct ProducerMessage {
//    topic: String,
//    message: Message,
//    resolver: oneshot::Sender<Result<proto::CommandSendReceipt, Error>>,
//}
//
//pub struct TopicProducer {
//    connection: Arc<Connection>,
//    id: ProducerId,
//    name: ProducerName,
//    topic: String,
//    message_id: SerialId,
//}
//
//impl TopicProducer {
//    pub fn new<S1, S2>(
//        addr: S1,
//        topic: S2,
//        name: Option<String>,
//        auth: Option<Authentication>,
//        proxy_to_broker_url: Option<String>,
//        options: ProducerOptions,
//        executor: TaskExecutor,
//    ) -> impl Future<Item = TopicProducer, Error = Error>
//    where
//        S1: Into<String>,
//        S2: Into<String>,
//    {
//        Connection::new(addr.into(), auth, proxy_to_broker_url, executor)
//            .map_err(|e| e.into())
//            .and_then(move |conn| {
//                TopicProducer::from_connection(Arc::new(conn), topic.into(), name, options)
//            })
//    }
//
//    pub fn from_connection<S: Into<String>>(
//        connection: Arc<Connection>,
//        topic: S,
//        name: Option<String>,
//        options: ProducerOptions,
//    ) -> impl Future<Item = TopicProducer, Error = Error> {
//        let topic = topic.into();
//        let producer_id = rand::random();
//        let sequence_ids = SerialId::new();
//
//        let sender = connection.sender().clone();
//        connection
//            .sender()
//            .lookup_topic(topic.clone(), false)
//            .map_err(|e| e.into())
//            .and_then({
//                let topic = topic.clone();
//                move |_| {
//                    sender
//                        .create_producer(topic.clone(), producer_id, name, options)
//                        .map_err(|e| e.into())
//                }
//            })
//            .map(move |success| TopicProducer {
//                connection,
//                id: producer_id,
//                name: success.producer_name,
//                topic,
//                message_id: sequence_ids,
//            })
//    }
//
//    pub fn is_valid(&self) -> bool {
//        self.connection.is_valid()
//    }
//
//    pub fn topic(&self) -> &str {
//        &self.topic
//    }
//
//    pub fn check_connection(&self) -> impl Future<Item = (), Error = Error> {
//        self.connection
//            .sender()
//            .lookup_topic("test", false)
//            .map(|_| ())
//            .map_err(|e| e.into())
//    }
//
//    pub fn send_raw(
//        &self,
//        message: Message,
//    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> {
//        self.connection
//            .sender()
//            .send(
//                self.id,
//                self.name.clone(),
//                self.message_id.get(),
//                None,
//                message,
//            )
//            .map_err(|e| e.into())
//    }
//
//    pub fn send<T: SerializeMessage>(
//        &self,
//        message: &T,
//        num_messages: Option<i32>,
//    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> {
//        match T::serialize_message(message) {
//            Ok(message) => Either::A(self.send_message(message, num_messages)),
//            Err(e) => Either::B(future::failed(e.into())),
//        }
//    }
//
//    pub fn error(&self) -> Option<Error> {
//        self.connection
//            .error()
//            .map(|e| ProducerError::Connection(e).into())
//    }
//
//    fn send_message(
//        &self,
//        message: Message,
//        num_messages: Option<i32>,
//    ) -> impl Future<Item = proto::CommandSendReceipt, Error = Error> {
//        self.connection
//            .sender()
//            .send(
//                self.id,
//                self.name.clone(),
//                self.message_id.get(),
//                num_messages,
//                message,
//            )
//            .map_err(|e| ProducerError::Connection(e).into())
//    }
//}
//
//impl Drop for TopicProducer {
//    fn drop(&mut self) {
//        let _ = self.connection.sender().close_producer(self.id);
//    }
//}
