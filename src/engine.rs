use crate::connection::{Connection, ConnectionOptions, ConnectionMessage, ConnectionMessageResolver, messages, ConnectionHandle};
use std::collections::{BTreeMap, VecDeque};
use crate::util::SerialId;
use futures::{future, Stream, Future, Sink, TryFutureExt, FutureExt};
use futures::task::{Context, Poll};
use futures::channel::{mpsc, oneshot};
use std::pin::Pin;
use crate::producer::{ProducerOptions, RouteMessage};
use tokio::net::ToSocketAddrs;
use crate::proto::{RequestKey, Message};
use crate::error::Error;
use std::mem;
use crate::{connection, message, producer};
use crate::consumer::ConsumerOptions;
use crate::proto::proto;
use crate::proto::proto::command_subscribe::SubType;
use futures::future::Either;
use crate::resolver::Resolver;
use crate::proto::proto::CommandError;
use crate::message::{PartitionedTopicMetadata, LookupTopicResponse, CreateProducerSuccess};
use crate::proto::command_lookup_topic_response::LookupType as TopicLookupType;
use std::time::Duration;
use crate::connection_manager::ConnectionManager;

pub struct ProducerReady {
    sender: mpsc::Sender<producer::Message>
}

pub enum ConsumerMessage {
    Message(Result<Message, Error>),
    Batch(Result<Vec<Message>, Error>),
    CloseConsumer(u64),
    EndOfTopic(u64),
}

pub struct ConnectProducer {
    pub topic: String,
    pub connection_options: Option<ConnectionOptions>,
    pub producer_options: Option<ProducerOptions>,
    pub resolver: Resolver<proto::CommandProducerSuccess>,
}

struct LookupTopicPartitions {
    topic: String,
    resolver: Resolver<proto::CommandPartitionedTopicMetadataResponse>,
}

struct LookupTopic {
    topic: String,
    authoritative: Option<bool>,
    resolver: Resolver<proto::CommandLookupTopicResponse>,
}

//handle send_error
pub struct SendMessage {

}

pub struct Ack {

}

pub struct Subscribe {
    pub topic: String,
    pub subscription: String,
    pub sub_type: SubType,
    pub connection_options: Option<ConnectionOptions>,
    pub consumer_options: Option<ConsumerOptions>,
    pub resolver: mpsc::UnboundedSender<Message>,
}

pub struct Unsubscribe {
    pub consumer_id: u64,
}

pub struct Flow {

}

pub enum EngineMessage {
    ConnectProducer(ConnectProducer),
    LookupPartitionedTopic(LookupTopicPartitions),
    LookupTopic(LookupTopic),
    Send(SendMessage),
    CloseProducer,
    Subscribe(Subscribe),
    Ack(Ack),
    Flow(Flow),
    Unsubscribe(Unsubscribe),
    CloseConsumer,
    RedeliverUnacknowledgedMessages,
    Seek,
}

enum ProducerState {
    LookupPartitions {
        f: Box<dyn Future<Output=Result<Vec<LookupTopicResponse>, Error>>>,
        resolver: Option<Resolver<ProducerReady>>,
    },
    LookupTopics {
        redirects: BTreeMap<String, u64>,
        resolver: Option<Resolver<ProducerReady>>,
    },
    Connecting {
        connections: Vec<u64>,
        resolver: Option<Resolver<ProducerReady>>,
    },
    CreatingProducer {
        connections: Vec<u64>,
        f: Box<dyn Future<Output=Result<Vec<CreateProducerSuccess>, Error>>>,
        resolver: Option<Resolver<ProducerReady>>,
    },
    Ready {
        connections: Vec<u64>,
    }
}

struct Producer {
    id: u64,
    topic: String,
    options: Option<ProducerOptions>,
    router: Option<Box<dyn RouteMessage>>,
    state: Option<ProducerState>,
    sender: mpsc::Sender<producer::Message>,
    receiver: mpsc::Receiver<producer::Message>,
}

struct Engine {
    connections: ConnectionManager,
    producers: ProducerManager,
}

impl Engine {
    fn shutdown(&self) -> bool {
        if self.inbound.is_some() {
            return false;
        }
        !self.connections.values().any(|c| c.has_pending_messages())
    }

    fn poll_connections(&mut self, cx: &mut Context<'_>) {
        let mut disconnects = Vec::new();
        for (id, connection) in self.connections.iter_mut() {
            match connection.poll(cx) {
                Poll::Ready(Ok(())) | Poll::Ready(Err(_)) => {
                    disconnects.push(*id);
                }
                Poll::Pending => {}
            }
        }
        for disconnect in disconnects {
            self.connections.remove(&disconnect);
        }
    }

    fn poll_producers(&mut self, cx: &mut Context<'_>) {
        for (id, producer) in self.producers.iter_mut() {
            let next_state = match producer.state.take() {
                None => {
                    let conn = self.base_connection.get().handle();
                    let topic = producer.topic.clone();
                    let options = producer.options.unwrap_or_default();
                    let f = Box::new(conn.lookup_partitioned_topic(producer.topic.clone())
                        .and_then(move |partition_metadata| {
                            if partition_metadata.partitions > 1 {
                                future::join_all((0..partition_metadata.partitions)
                                    .map(|p| conn.lookup_topic(format!("{}-partition-{}", topic, p), None)))
                            } else {
                                conn.lookup_topic(topic, None).map(|r| vec![r])
                            }
                        }));

                    Some(ProducerState::LookupPartitions { f, resolver: None })
                }
                Some(ProducerState::LookupPartitions { f, resolver }) => {
                    match f.poll(cx) {
                        Poll::Pending => ProducerState::LookupPartitions { f, resolver, },
                        Poll::Ready(Ok(topics)) => {
                            let redirects: Vec<&LookupTopicResponse> = topics.iter()
                                .filter(|r| r.response == TopicLookupType::Redirect)
                                .collect();
                            if redirects.is_empty() {

                            } else {

                            }
                        }
                        Poll::Ready(Err(e)) => {

                        }
                    }
                }
                Some(ProducerState::LookupTopics { redirects, resolver }) => {

                }
                Some(ProducerState::Connecting { connections, resolver }) => {

                }
                Some(ProducerState::CreatingProducer { connections, f, resolver }) => {

                }
                Some(ProducerState::Ready { connections }) => {

                },
            };
            producer.state = next_state;
        }
    }

    fn receive_inbound(&mut self, cx: &mut Context<'_>) {
        loop {
            let msg: Poll<Option<EngineMessage>> = self.self_rx.poll_next(cx);
            match msg {
                Poll::Ready(Some(msg)) => {
                    self.handle_message(msg);
                },
                Poll::Pending => break,
                Poll::Ready(None) => unreachable!()
            }
        }
        if let Some(inbound) = &mut self.inbound {
            loop {
                let msg: Poll<Option<EngineMessage>> = self.inbound.poll_next(cx);
                match msg {
                    Poll::Ready(Some(msg)) => {
                        self.handle_message(msg);
                    },
                    Poll::Ready(None) => {
                        self.inbound = None;
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }
    }

    fn handle_message(&mut self, message: EngineMessage) {
        use crate::connection::messages;
        match message {
            EngineMessage::ConnectProducer(msg) => {

            }
            EngineMessage::Send(msg) => {

            }
            EngineMessage::CloseProducer => {

            }
            EngineMessage::Subscribe(msg) => {

            }
            EngineMessage::Ack(msg) => {

            }
            EngineMessage::Flow(msg) => {

            }
            EngineMessage::Unsubscribe(msg) => {

            }
            EngineMessage::CloseConsumer => {

            }
            EngineMessage::RedeliverUnacknowledgedMessages => {

            }
            EngineMessage::Seek => {

            }
        }
    }

    async fn connect_producer(&mut self, msg: ConnectProducer) {
        let conn = self.base_connection.get();

        let resolver: Resolver<Message> = msg.resolver
            .and_then(|msg: Message| {
                if let Some(msg) = msg.command.producer_success {
                    return Ok(msg);
                }
                if let Some(error) = msg.command.error {
                    return Err(error.into())
                }
                Err(Error::unexpected_response(format!("unexpected response to `producer`: {:?}", msg.command)))
            });
        self.base_connection.get().enqueue_message(ConnectionMessage {
            message: messages::create_producer(msg.topic, msg.producer_options),
            resolver: Some(resolver)
        })
    }

    fn lookup_topic_partitions(&mut self, msg: LookupTopicPartitions) {
        let conn = self.base_connection.get();
        msg.resolver.resolve_fut({
            conn.request(messages::lookup_partitioned_topic(msg.topic.clone(), conn.request_id()))
                .and_then(|msg| extract_message!("lookup_partitioned_topic", msg, msg.command.partition_metadata_response))
        });
    }

    fn lookup_topic(conn: &mut Connection, msg: LookupTopic) {
        msg.resolver.resolve_fut( {
            conn.request(messages::lookup_topic(msg.topic, msg.authoritative, conn.request_id()))
                .and_then(|msg| extract_message!("lookup_topic", msg, msg.command.lookup_topic_response))
        });
    }
}

impl Future for Engine {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(self.base_connection.check_ready());
        self.poll_connections(cx);
        self.receive_inbound(cx);
        if self.shutdown() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct Ids {
    connection: SerialId,
    consumer: SerialId,
    producer: SerialId,
}



