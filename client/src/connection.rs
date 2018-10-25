use error::{Error, SharedError};

use futures::{self, Async, Future, Stream, Sink, IntoFuture, future::{self, Either}, sync::{mpsc, oneshot},
              AsyncSink};
use message::{proto, Codec, Message};
use rand;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::net::TcpStream;
use tokio_codec;

type Pulsar = tokio_codec::Framed<TcpStream, Codec>;

pub struct Receiver<S: Stream<Item=Message, Error=Error>> {
    inbound: S,
    outbound: mpsc::UnboundedSender<Message>,
    error: SharedError,
    pending_requests: BTreeMap<u64, oneshot::Sender<Message>>,
    new_requests: mpsc::UnboundedReceiver<(u64, oneshot::Sender<Message>)>,
    shutdown: oneshot::Receiver<()>,
}

impl<S: Stream<Item=Message, Error=Error>> Receiver<S> {
    pub fn new(
        inbound: S,
        outbound: mpsc::UnboundedSender<Message>,
        error: SharedError,
        new_requests: mpsc::UnboundedReceiver<(u64, oneshot::Sender<Message>)>,
        shutdown: oneshot::Receiver<()>,
    ) -> Receiver<S> {
        Receiver {
            inbound,
            outbound,
            error,
            pending_requests: BTreeMap::new(),
            new_requests,
            shutdown,
        }
    }
}

impl<S: Stream<Item=Message, Error=Error>> Future for Receiver<S> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        match self.shutdown.poll() {
            Ok(Async::Ready(())) | Err(_) => return Err(()),
            Ok(Async::NotReady) => {}
        }

        //Are we worries about starvation here?
        loop {
            match self.new_requests.poll() {
                Ok(Async::Ready(Some((request_id, resolver)))) => {
                    self.pending_requests.insert(request_id, resolver);
                },
                Ok(Async::Ready(None)) | Err(_) => {
                    self.error.set(Error::Disconnected);
                    return Err(());
                },
                Ok(Async::NotReady) => break,
            }
        }

        loop {
            match self.inbound.poll() {
                Ok(Async::Ready(Some(msg))) => {
                    if msg.command.ping.is_some() {
                        let _ = self.outbound.unbounded_send(messages::pong());
                    } else {
                        if let Some(request_id) = msg.request_id() {
                            if let Some(resolver) = self.pending_requests.remove(&request_id) {
                                // We don't care if the receiver has dropped their future
                                let _ = resolver.send(msg);
                            } else {
                                println!("Resolver missing for message {:?}", msg);
                            }
                        } else {
                            println!("Received message with no request_id; dropping. Message: {:?}", msg.command);
                        }
                    }
                },
                Ok(Async::Ready(None)) => {
                    self.error.set(Error::Disconnected);
                    return Err(())
                },
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => {
                    self.error.set(e);
                    return Err(())
                }
            }
        }

    }
}

pub struct Sender<S: Sink<SinkItem=Message, SinkError=Error>> {
    sink: S,
    outbound: mpsc::UnboundedReceiver<Message>,
    buffered: Option<Message>,
    error: SharedError,
    shutdown: oneshot::Receiver<()>,
}

impl <S: Sink<SinkItem=Message, SinkError=Error>> Sender<S> {
    pub fn new(
        sink: S,
        outbound: mpsc::UnboundedReceiver<Message>,
        error: SharedError,
        shutdown: oneshot::Receiver<()>
    ) -> Sender<S> {
        Sender {
            sink,
            outbound,
            buffered: None,
            error,
            shutdown,
        }
    }

    fn try_start_send(&mut self, item: Message) -> futures::Poll<(), Error> {
        if let AsyncSink::NotReady(item) = self.sink.start_send(item)? {
            self.buffered = Some(item);
            return Ok(Async::NotReady)
        }
        Ok(Async::Ready(()))
    }
}

impl<S: Sink<SinkItem=Message, SinkError=Error>> Future for Sender<S> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        match self.shutdown.poll() {
            Ok(Async::Ready(())) | Err(futures::Canceled) => return Err(()),
            Ok(Async::NotReady) => {},
        }

        if let Some(item) = self.buffered.take() {
            try_ready!(self.try_start_send(item).map_err(|e| self.error.set(e)))
        }

        loop {
            match self.outbound.poll()? {
                Async::Ready(Some(item)) => try_ready!(self.try_start_send(item).map_err(|e| self.error.set(e))),
                Async::Ready(None) => {
                    try_ready!(self.sink.close().map_err(|e| self.error.set(e)));
                    return Ok(Async::Ready(()))
                }
                Async::NotReady => {
                    try_ready!(self.sink.poll_complete().map_err(|e| self.error.set(e)));
                    return Ok(Async::NotReady)
                }
            }
        }
    }
}

pub struct Connection {
    tx: mpsc::UnboundedSender<Message>,
    new_requests: mpsc::UnboundedSender<(u64, oneshot::Sender<Message>)>,
    request_id: u64,
    error: SharedError,
    sender_shutdown: Option<oneshot::Sender<()>>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
}

impl Connection {
    pub fn new(addr: String) -> impl Future<Item=(Connection, impl Future<Item=(), Error=()>), Error=Error> {
        SocketAddr::from_str(&addr).into_future()
            .map_err(|e| Error::SocketAddr(e.to_string()))
            .and_then(|addr| {
                TcpStream::connect(&addr)
                    .map_err(|e| e.into())
                    .map(|stream| tokio_codec::Framed::new(stream, Codec))
                    .and_then(|stream| send_single_message(stream, messages::connect(), |r| r.command.connected))
                    .map(|(_success, stream)| {
                        println!("Connection established");
                        stream
                    })
            })
            .map(move |pulsar| {
                let (sink, stream) = pulsar.split();
                let (tx, rx) = mpsc::unbounded();
                let (new_requests_tx, new_requests_rx) = mpsc::unbounded();
                let error = SharedError::new();
                let (receiver_shutdown_tx, receiver_shutdown_rx) = oneshot::channel();
                let (sender_shutdown_tx, sender_shutdown_rx) = oneshot::channel();

                let receiver = Receiver::new(
                    stream,
                    tx.clone(),
                    error.clone(),
                    new_requests_rx,
                    receiver_shutdown_rx,
                );

                let sender = Sender::new(
                    sink,
                    rx,
                    error.clone(),
                    sender_shutdown_rx
                );

                let connection = Connection {
                    tx,
                    new_requests: new_requests_tx,
                    request_id: 0,
                    error,
                    sender_shutdown: Some(sender_shutdown_tx),
                    receiver_shutdown: Some(receiver_shutdown_tx),
                };

                (connection, Future::join(receiver, sender).map(|((),())| ()))
            })
    }

    pub fn is_valid(&self) -> bool {
        self.error.is_set()
    }

    pub fn send_ping(&self) -> Result<(), Error> {
        self.tx.unbounded_send(messages::ping())
            .map_err(|_| Error::Disconnected)
    }

    pub fn lookup_topic(&mut self, topic: String) -> impl Future<Item=proto::CommandLookupTopicResponse, Error=Error> {
        self.send_message(|req_id| messages::lookup_topic(topic, req_id), |resp| resp.command.lookup_topic_response)
    }

    pub fn create_producer(&mut self,
                           topic: String,
                           producer_name: Option<String>,
                           producer_id: Option<u64>
    ) -> impl Future<Item=proto::CommandProducerSuccess, Error=Error> {
        self.send_message(
            move |req_id| messages::create_producer(
                topic,
                producer_name,
                producer_id.unwrap_or_else(rand::random),
                req_id),
            |resp| resp.command.producer_success
        )
    }

    fn next_request_id(&mut self) -> u64 {
        let request_id = self.request_id;
        self.request_id += 1;
        request_id
    }

    fn send_message<R, Build, Extract>(&mut self, build: Build, extract: Extract) -> impl Future<Item=R, Error=Error>
        where Extract: FnOnce(Message) -> Option<R>,
              Build: FnOnce(u64) -> Message
    {
        let request_id = self.next_request_id();
        let (tx, rx) = oneshot::channel();

        let resp = rx.map_err(|oneshot::Canceled| Error::Disconnected)
            .and_then(|message: Message| {
                if message.command.error.is_some() {
                    Err(Error::PulsarError(format!("{:?}", message.command.error.unwrap())))
                } else if message.command.send_error.is_some() {
                    Err(Error::PulsarError(format!("{:?}", message.command.error.unwrap())))
                } else {
                    let cmd = message.command.clone();
                    if let Some(extracted) = extract(message) {
                        Ok(extracted)
                    } else {
                        Err(Error::UnexpectedResponse(format!("{:?}", cmd)))
                    }
                }
            });

        match (self.new_requests.unbounded_send((request_id, tx)), self.tx.unbounded_send(build(request_id))) {
            (Ok(_), Ok(_)) => Either::A(resp),
            _ => Either::B(future::err(Error::Disconnected))
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(shutdown) = self.sender_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.receiver_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

fn send_single_message<T, F: Fn(Message) -> Option<T>>(
    stream: Pulsar,
    message: Message,
    extract_resp: F
) -> impl Future<Item=(T, Pulsar), Error=Error> {
    stream.send(message)
        .and_then(|stream| stream.into_future().map_err(|(err, _)| err))
        .and_then(move |(msg, stream)| match msg {
            Some(Message { command: proto::BaseCommand { error: Some(error), .. }, .. }) =>
                Err(Error::PulsarError(format!("{:?}", error))),
            Some(msg) => {
                let cmd = msg.command.clone();
                extract_resp(msg)
                    .ok_or_else(|| Error::PulsarError(format!("Unexpected message from pulsar: {:?}", cmd)))
                    .map(|msg| (msg, stream))
            },
            None =>
                Err(Error::Disconnected)
        })
}

mod messages {
    use message::{Message, proto::{self, base_command::Type as CommandType}};

    pub fn connect() -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Connect as i32,
                connect: Some(proto::CommandConnect {
                    auth_data: None,
                    client_version: String::from("2.0.1-incubating"),
                    protocol_version: Some(12),
                    .. Default::default()
                }),
                .. Default::default()
            },
            payload: None,
        }
    }

    pub fn ping() -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Ping as i32,
                ping: Some(proto::CommandPing {}),
                .. Default::default()
            },
            payload: None,
        }
    }

    pub fn pong() -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Pong as i32,
                pong: Some(proto::CommandPong {}),
                .. Default::default()
            },
            payload: None,
        }
    }

    pub fn create_producer(topic: String, producer_name: Option<String>, producer_id: u64, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Producer as i32,
                producer: Some(proto::CommandProducer {
                    topic,
                    producer_id,
                    request_id,
                    producer_name,
                    .. Default::default()
                }),
                .. Default::default()
            },
            payload: None,
        }
    }

    pub fn lookup_topic(topic: String, request_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Lookup as i32,
                lookup_topic: Some(proto::CommandLookupTopic {
                    topic,
                    request_id,
                    .. Default::default()
                }),
                .. Default::default()
            },
            payload: None,
        }
    }
}
