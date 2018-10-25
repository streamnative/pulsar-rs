use super::{messages, pulsar, Pulsar, Error};
use futures::{self, Async, Future, Stream, IntoFuture, future::{self, Either}, sync::{mpsc, oneshot},
              AsyncSink};
use std::collections::BTreeMap;
use message::{proto, Message};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::net::SocketAddr;
use std::str::FromStr;
use rand;

#[derive(Clone)]
pub struct SharedError {
    error_set: Arc<AtomicBool>,
    error: Arc<Mutex<Option<Error>>>,
}

impl SharedError {
    pub fn new() -> SharedError {
        SharedError {
            error_set: Arc::new(AtomicBool::new(false)),
            error: Arc::new(Mutex::new(None)),
        }
    }

    pub fn is_set(&self) -> bool {
        self.error_set.load(Ordering::Relaxed)
    }

    pub fn remove(&self) -> Option<Error> {
        let mut lock = self.error.lock().unwrap();
        let error = lock.take();
        self.error_set.store(false, Ordering::Release);
        error
    }

    pub fn set(&self, error: Error) {
        let mut lock = self.error.lock().unwrap();
        *lock = Some(error);
        self.error_set.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct RequestId(Arc<AtomicUsize>);

impl RequestId {
    pub fn new() -> RequestId {
        RequestId(Arc::new(AtomicUsize::new(0)))
    }

    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) as u64
    }
}

pub enum SendResponse {
    Receipt(proto::CommandSendReceipt),
    Error(proto::CommandSendError),
}

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

pub struct Sender<S: futures::Sink<SinkItem=Message, SinkError=Error>> {
    sink: S,
    outbound: mpsc::UnboundedReceiver<Message>,
    buffered: Option<Message>,
    error: SharedError,
    shutdown: oneshot::Receiver<()>,
}

impl <S: futures::Sink<SinkItem=Message, SinkError=Error>> Sender<S> {
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

impl<S: futures::Sink<SinkItem=Message, SinkError=Error>> Future for Sender<S> {
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

pub struct ProducerConnection {
    tx: mpsc::UnboundedSender<Message>,
    new_requests: mpsc::UnboundedSender<(u64, oneshot::Sender<Message>)>,
    producer_id: u64,
    request_id: RequestId,
    error: SharedError,
    sender_shutdown: Option<oneshot::Sender<()>>,
    receiver_shutdown: Option<oneshot::Sender<()>>,
}

impl ProducerConnection {
    pub fn new(addr: String, topic: String) -> impl Future<Item=(ProducerConnection, impl Future<Item=(), Error=()>), Error=Error> {
        let producer_id = rand::random();
        let request_id = RequestId::new();

        ProducerConnection::connect(&addr, topic.clone(), producer_id, request_id.clone())
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

                let producer = ProducerConnection {
                    tx,
                    new_requests: new_requests_tx,
                    producer_id,
                    request_id,
                    error,
                    sender_shutdown: Some(sender_shutdown_tx),
                    receiver_shutdown: Some(receiver_shutdown_tx),
                };

                (producer, Future::join(receiver, sender).map(|((),())| ()))
            })
    }

    fn send_message<R, Build, Extract>(&self, build: Build, extract: Extract) -> impl Future<Item=R, Error=Error>
        where Extract: FnOnce(Message) -> Option<R>,
              Build: FnOnce(u64) -> Message
    {
        let request_id = self.request_id.next();
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

    pub fn is_valid(&self) -> bool {
        self.error.is_set()
    }

    pub fn ping(&self) -> Result<(), Error> {
        self.tx.unbounded_send(messages::ping())
            .map_err(|_| Error::Disconnected)
    }

    pub fn lookup_topic(&self, topic: String) -> impl Future<Item=proto::CommandLookupTopicResponse, Error=Error> {
        self.send_message(|req_id| messages::lookup_topic(topic, req_id), |resp| resp.command.lookup_topic_response)
    }

        /// Connects to pulsar as a producer on a given topic, building send / receive handlers
    fn connect(addr: &str, topic: String, producer_id: u64, req_id: RequestId) -> impl Future<Item=Pulsar, Error=Error> {
        let another_req_id = req_id.clone();
        let topic_ = topic.clone();
        SocketAddr::from_str(addr).into_future().map_err(|e| Error::SocketAddr(e.to_string()))
            .and_then(|addr| pulsar::connect(&addr))
            .and_then(move |stream| pulsar::lookup_topic(stream, topic_, req_id.next()))
            .and_then(move |stream| pulsar::create_producer(stream, topic, producer_id, another_req_id.next()))
    }
}

impl Drop for ProducerConnection {
    fn drop(&mut self) {
        if let Some(shutdown) = self.sender_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.receiver_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}