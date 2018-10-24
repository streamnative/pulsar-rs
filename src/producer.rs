use super::{messages, pulsar, Pulsar, Error};
use futures::{Future, Stream, IntoFuture, future::{self, Either}, sync::{mpsc, oneshot}};
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

pub struct Connection {

}

pub struct ProducerConnection {
    tx: mpsc::UnboundedSender<Message>,
    pending: Arc<Mutex<BTreeMap<u64, oneshot::Sender<Message>>>>,
    producer_id: u64,
    request_id: RequestId,
    error: SharedError,
}

impl ProducerConnection {
    pub fn new(addr: String, topic: String) -> impl Future<Item=(ProducerConnection, impl Future<Item=(), Error=()>), Error=Error> {
        let producer_id = rand::random();
        let request_id = RequestId::new();

        ProducerConnection::connect(&addr, topic.clone(), producer_id, request_id.clone())
            .map(move |pulsar| {
                let (tx, rx) = mpsc::unbounded();
                let pending: Arc<Mutex<BTreeMap<u64, oneshot::Sender<Message>>>> = Arc::new(Mutex::new(BTreeMap::new()));

                let error = SharedError::new();

                let (sink, stream) = pulsar.split();

                let receiver = {
                    let tx = tx.clone();
                    let error = error.clone();
                    let pending = pending.clone();
                    stream
                        .for_each(move |msg: Message| {
                            if msg.command.ping.is_some() {
                                let _ = tx.send(messages::pong());
                            } else {
                                if let Some(request_id) = msg.request_id() {
                                    let resolver = {
                                        let mut pending = pending.lock().unwrap();
                                        pending.remove(&request_id)
                                    };
                                    if let Some(resolver) = resolver {
                                        let _ = resolver.send(msg);
                                    }
                                } else {
                                    eprintln!("Received message with no request_id; dropping. Message: {:?}", msg.command)
                                }
                            }
                            Ok(())
                        })
                        .map_err(move |e| error.set(e))
                };
                let sender = {
                    let error = error.clone();
                    rx.map_err(|e| Error::Disconnected)
                        .forward(sink)
                        .map(|_| ())
                        .map_err(move |e| error.set(e))
                };
                let producer = ProducerConnection {
                    tx,
                    pending,
                    producer_id,
                    request_id,
                    error,
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
        {
            let mut lock = self.pending.lock().unwrap();
            lock.insert(request_id, tx);
        }
        let resp = rx.map_err(|oneshot::Canceled| Error::Disconnected)
            .and_then(|message| {
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

        let pending = self.pending.clone();
        match self.tx.send(build(request_id)) {
            Ok(_) => Either::A(resp),
            Err(_) => {
                let mut lock = pending.lock().unwrap();
                let _ = lock.remove(&request_id);
                Either::B(future::err(Error::Disconnected))
            }
        }
    }

    pub fn is_valid(&self) -> bool {
        self.error.is_set()
    }

    pub fn ping(&self) -> Result<(), Error> {
        self.tx.send(messages::ping())
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