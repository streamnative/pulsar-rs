use connection::Connection;
use error::Error;
use futures::Future;
use serde::de::DeserializeOwned;
use serde_json;
use message::{Message, Payload, proto::{self, command_subscribe::SubType}};
use rand;
use tokio::runtime::TaskExecutor;
use futures::{Stream, sync::mpsc, Async};
use std::sync::Arc;

pub struct Consumer<T> {
    connection: Arc<Connection>,
    id: u64,
    messages: mpsc::UnboundedReceiver<Message>,
    deserialize: Box<dyn Fn(Payload) -> Result<T, Error> + Send>,
    batch_size: u32,
    remaining_messages: u32,
}

impl<T: DeserializeOwned> Consumer<T> {
    pub fn new(
        addr: String,
        topic: String,
        subscription: String,
        sub_type: SubType,
        consumer_id: Option<u64>,
        consumer_name: Option<String>,
        executor: TaskExecutor,
        deserialize: Box<dyn Fn(Payload) -> Result<T, Error> + Send>,
        batch_size: Option<u32>,
    ) -> impl Future<Item=Consumer<T>, Error=Error> {
        let consumer_id = consumer_id.unwrap_or_else(rand::random);
        let (resolver, messages) = mpsc::unbounded();
        let batch_size = batch_size.unwrap_or(1000);

        Connection::new(addr, executor.clone())
            .and_then(move |conn|
                conn.sender().subscribe(resolver, topic, subscription, sub_type, consumer_id, consumer_name)
                    .map(move |resp| (resp, conn)))
            .and_then(move |(_, conn)| {
                conn.sender().send_flow(consumer_id, batch_size)
                    .map(move |()| conn)
            })
            .map(move |connection| {
                Consumer {
                    connection: Arc::new(connection),
                    id: consumer_id,
                    messages,
                    deserialize,
                    batch_size,
                    remaining_messages: batch_size
                }
            })
    }
}

pub struct Ack {
    consumer_id: u64,
    message_id: Vec<proto::MessageIdData>,
    connection: Arc<Connection>,
}

impl Ack {
    pub fn new(consumer_id: u64, msg: proto::MessageIdData, connection: Arc<Connection>) -> Ack {
        Ack { consumer_id, message_id: vec![msg], connection }
    }

    pub fn join(mut self, other: Ack) -> Self {
        self.message_id.extend(other.message_id);
        self
    }

    pub fn extend<I: IntoIterator<Item=Ack>>(mut self, others: I) -> Self {
        self.message_id.extend(others.into_iter().flat_map(|ack| ack.message_id));
        self
    }

    pub fn ack(self) {
        let _ = self.connection.sender().send_ack(self.consumer_id, self.message_id);
    }
}

impl<T> Stream for Consumer<T> {
    type Item = Result<(T, Ack), Error>;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        if !self.connection.is_valid() {
            if let Some(err) = self.connection.error() {
                return Err(err);
            }
        }

        if self.remaining_messages <= self.batch_size / 2 {
            self.connection.sender().send_flow(self.id, self.batch_size - self.remaining_messages)?;
            self.remaining_messages = self.batch_size;
        }

        let message: Option<Option<(proto::CommandMessage, Payload)>> = try_ready!(self.messages.poll().map_err(|_| Error::Disconnected))
            .map(| Message { command, payload }: Message|
                command.message
                    .and_then(move |msg| payload
                        .map(move |payload| (msg, payload))));

        if message.is_some() {
            self.remaining_messages -= 1;
        }

        match message {
            Some(Some((message, payload))) => {
                Ok(Async::Ready(Some({
                    let ack = Ack::new(self.id, message.message_id, self.connection.clone());
                    match (&self.deserialize)(payload) {
                        Ok(data) => Ok((data, ack)),
                        Err(e) => Err(e)
                    }
                })))
            },
            Some(None) => Ok(Async::Ready(Some(Err(Error::Unexpected(format!("Missing payload for message {:?}", message)))))),
            None => Ok(Async::Ready(None))
        }
    }

}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        let _ = self.connection.sender().close_consumer(self.id);
    }
}

pub struct Set<T>(T);
pub struct Unset;

pub struct ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
    addr: String,
    topic: Topic,
    subscription: Subscription,
    subscription_type: SubscriptionType,
    consumer_id: Option<u64>,
    consumer_name: Option<String>,
    executor: TaskExecutor,
    deserialize: Option<Box<dyn Fn(Payload) -> Result<DataType, Error> + Send>>,
    batch_size: Option<u32>,
}

impl ConsumerBuilder<Unset, Unset, Unset, Unset> {
    pub fn new<S: Into<String>>(addr: S, executor: TaskExecutor) -> Self {
        ConsumerBuilder {
            addr: addr.into(),
            topic: Unset,
            subscription: Unset,
            subscription_type: Unset,
            consumer_id: None,
            consumer_name: None,
            executor,
            deserialize: None,
            batch_size: None,
        }
    }
}

impl<Subscription, SubscriptionType, DataType> ConsumerBuilder<Unset, Subscription, SubscriptionType, DataType> {
    pub fn with_topic<S: Into<String>>(self, topic: S) -> ConsumerBuilder<Set<String>, Subscription, SubscriptionType, DataType> {
        ConsumerBuilder {
            topic: Set(topic.into()),
            addr: self.addr,
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            executor: self.executor,
            deserialize: self.deserialize,
            batch_size: self.batch_size,
        }
    }
}

impl<Topic, SubscriptionType, DataType> ConsumerBuilder<Topic, Unset, SubscriptionType, DataType> {
    pub fn with_subscription<S: Into<String>>(self, subscription: S) -> ConsumerBuilder<Topic, Set<String>, SubscriptionType, DataType> {
        ConsumerBuilder {
            subscription: Set(subscription.into()),
            topic: self.topic,
            addr: self.addr,
            subscription_type: self.subscription_type,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            executor: self.executor,
            deserialize: self.deserialize,
            batch_size: self.batch_size,
        }
    }
}

impl<Topic, Subscription, DataType> ConsumerBuilder<Topic, Subscription, Unset, DataType> {
    pub fn with_subscription_type(self, subscription_type: SubType) -> ConsumerBuilder<Topic, Subscription, Set<SubType>, DataType> {
        ConsumerBuilder {
            subscription_type: Set(subscription_type),
            topic: self.topic,
            addr: self.addr,
            subscription: self.subscription,
            consumer_id: self.consumer_id,
            consumer_name: self.consumer_name,
            executor: self.executor,
            deserialize: self.deserialize,
            batch_size: self.batch_size,
        }
    }
}

impl<Topic, Subscription, SubscriptionType> ConsumerBuilder<Topic, Subscription, SubscriptionType, Unset> {
    pub fn with_deserializer<T, F>(self, deserializer: F) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, T>
        where F: Fn(Payload) -> Result<T, Error> + Send + 'static
    {
        ConsumerBuilder {
            deserialize: Some(Box::new(deserializer)),
            topic: self.topic,
            addr: self.addr,
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_name: self.consumer_name,
            consumer_id: self.consumer_id,
            executor: self.executor,
            batch_size: self.batch_size,
        }
    }
}

impl<Topic, Subscription, SubscriptionType, DataType> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
    pub fn with_consumer_id(self, consumer_id: u64) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
        ConsumerBuilder {
            consumer_id: Some(consumer_id),
            topic: self.topic,
            addr: self.addr,
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_name: self.consumer_name,
            executor: self.executor,
            deserialize: self.deserialize,
            batch_size: self.batch_size,
        }
    }

    pub fn with_consumer_name<S: Into<String>>(self, consumer_name: S) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
        ConsumerBuilder {
            consumer_name: Some(consumer_name.into()),
            topic: self.topic,
            addr: self.addr,
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_id: self.consumer_id,
            executor: self.executor,
            deserialize: self.deserialize,
            batch_size: self.batch_size,
        }
    }

    pub fn with_batch_size(self, batch_size: u32) -> ConsumerBuilder<Topic, Subscription, SubscriptionType, DataType> {
        ConsumerBuilder {
            batch_size: Some(batch_size),
            topic: self.topic,
            addr: self.addr,
            subscription: self.subscription,
            subscription_type: self.subscription_type,
            consumer_name: self.consumer_name,
            consumer_id: self.consumer_id,
            executor: self.executor,
            deserialize: self.deserialize,
        }
    }
}

impl ConsumerBuilder<Set<String>, Set<String>, Set<SubType>, Unset> {
    pub fn build<T: DeserializeOwned>(self) -> impl Future<Item=Consumer<T>, Error=Error> {
        let deserialize = Box::new(|payload: Payload| {
            serde_json::from_slice(&payload.data).map_err(|e| e.into())
        });
        let ConsumerBuilder {
            addr,
            topic: Set(topic),
            subscription: Set(subscription),
            subscription_type: Set(sub_type),
            consumer_id,
            consumer_name,
            executor,
            batch_size,
            ..
        } = self;
        Consumer::new(addr, topic, subscription, sub_type, consumer_id, consumer_name, executor, deserialize, batch_size)
    }
}

impl<T: DeserializeOwned> ConsumerBuilder<Set<String>, Set<String>, Set<SubType>, T> {
    pub fn build(self) -> impl Future<Item=Consumer<T>, Error=Error> {
        let ConsumerBuilder {
            addr,
            topic: Set(topic),
            subscription: Set(subscription),
            subscription_type: Set(sub_type),
            consumer_id,
            consumer_name,
            executor,
            deserialize,
            batch_size,
        } = self;
        let deserialize = deserialize.unwrap();
        Consumer::new(addr, topic, subscription, sub_type, consumer_id, consumer_name, executor, deserialize, batch_size)
    }
}