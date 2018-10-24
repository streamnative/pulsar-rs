extern crate bytes;
extern crate crc;
extern crate futures;
extern crate prost;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate tokio_codec;
#[macro_use] extern crate failure;
#[macro_use] extern crate nom;
#[macro_use] extern crate prost_derive;

pub mod message;
pub mod producer;

use futures::{Sink, Stream, sync::mpsc};
use tokio::net::TcpStream;
use tokio::prelude::*;
use std::io;
use std::net::SocketAddr;
use std::str::FromStr;

use message::{Message, proto, Codec};

pub(crate) type Pulsar = tokio_codec::Framed<TcpStream, Codec>;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Io(io::Error),
    #[fail(display = "Disconnected")]
    Disconnected,
    #[fail(display = "{}", _0)]
    PulsarError(String),
    #[fail(display = "{}", _0)]
    Unexpected(String),
    #[fail(display = "Error decoding message: {}", _0)]
    Decoding(String),
    #[fail(display = "Error encoding message: {}", _0)]
    Encoding(String),
    #[fail(display = "Error obtaining socket address: {}", _0)]
    SocketAddr(String),
    #[fail(display = "Unexpected response from pulsar: {}", _0)]
    UnexpectedResponse(String)
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

use futures::sync::oneshot;



mod pulsar {
    use super::*;
    pub fn connect(addr: &SocketAddr) -> impl Future<Item = Pulsar, Error = Error> {
        TcpStream::connect(&addr)
            .map_err(|e| e.into())
            .map(|stream| tokio_codec::Framed::new(stream, Codec))
            .and_then(|messages| send_message(messages, messages::connect(), |r| r.command.connected))
            .map(|(_success, stream)| {
                println!("Connection established");
                stream
            })
    }

    pub fn create_producer(stream: Pulsar, topic: String, producer_id: u64, req_id: u64) -> impl Future<Item=Pulsar, Error=Error> {
        send_message(stream, messages::create_producer(topic.clone(), producer_id, req_id), |m| m.command.producer_success)
            .map(move |(resp, stream)| {
                println!("Producer '{}' created on topic '{}'", resp.producer_name, topic);
                stream
            })
    }

    pub fn lookup_topic(stream: Pulsar, topic: String, req_id: u64) -> impl Future<Item=Pulsar, Error=Error> {
        send_message(stream, messages::lookup_topic(topic, req_id), |m| m.command.lookup_topic_response)
            .map(move |(resp, stream)| {
                println!("Lookup topic success: {:?}", resp);
                stream
            })
    }

    fn send_message<T, F: Fn(Message) -> Option<T>>(
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
}
//
//pub struct Producer {
//    pub sink: mpsc::UnboundedSender<Message>,
//    pub errs: mpsc::UnboundedReceiver<Error>,
//    socket_addr: String,
//    topic: String,
//}
//
///// Stream will propagate disconnects; users should manually reconnect
//impl Sink for Producer {
//    type SinkItem = Message;
//    type SinkError = Error;
//
//    fn start_send(&mut self, item: Message) -> Result<AsyncSink<Message>, Self::SinkError> {
//        match self.errs.poll() {
//            Ok(Async::NotReady) => {},
//            Ok(Async::Ready(Some(Error::Disconnected))) | Ok(Async::Ready(None)) => {
//                self.reconnect();
//                return Err(Error::Disconnected);
//            },
//            Ok(Async::Ready(Some(err))) => {
//                self.reconnect();
//                return Err(err);
//            },
//            Err(()) => return Err(Error::Unexpected(String::from("Unexpected Error in mpsc stream")))
//        }
//
//        match self.sink.start_send(item) {
//            Ok(send) => Ok(send),
//            Err(_) => Err(Error::Disconnected)
//        }
//    }
//
//    fn poll_complete(&mut self) -> Result<Async<()>, Error> {
//        self.sink.poll_complete().map_err(|_| Error::Disconnected)
//    }
//
//    fn close(&mut self) -> Result<Async<()>, Error> {
//        self.sink.close().map_err(|_| Error::Disconnected)
//    }
//}
//
//impl Producer {
//    /// Builds a future to connect to pulsar, and creates the Producer object to use said connection.
//    /// The future returned (second item of tuple) should be ran in an executor to actually start
//    /// the connection
//    pub fn new(addr: String, topic: String) -> impl Future<Item=(Producer, impl Future<Item=(), Error=()>), Error=Error> {
//        let (tx, rx) = mpsc::unbounded();
//        let (err_tx, err_rx) = mpsc::unbounded();
//        Producer::connect(&addr, topic.clone())
//            .map(move |stream| {
//                let producer = Producer {
//                    sink: tx.clone(),
//                    errs: err_rx,
//                    socket_addr: addr,
//                    topic: topic.clone(),
//                };
//                let handlers = Producer::build_connection_handlers(stream, tx, rx, err_tx);
//                (producer, handlers)
//            })
//    }
//
//    /// Attempts to reconnect to pulsar, rebuilding connection machinery
//    fn reconnect(&mut self) -> impl Future<Item=(), Error=Error> {
//        let (tx, rx) = mpsc::unbounded();
//        let (err_tx, err_rx) = mpsc::unbounded();
//        self.errs = err_rx;
//        self.sink = tx.clone();
//        Producer::connect(&self.socket_addr, self.topic.clone())
//            .and_then(move |stream| {
//                Producer::build_connection_handlers(stream, tx, rx, err_tx)
//                    .map_err(|()| Error::Unexpected(String::from("Unexpected error. This is a bug!")))
//            })
//    }
//
//    /// Connects to pulsar as a producer on a given topic, building send / receive handlers
//    fn connect(addr: &str, topic: String, producer_id: u64, req_id: &mut u64) -> impl Future<Item=Pulsar, Error=Error> {
//        let lookup_topic_req_id = req_id.clone();
//        *req_id += 1;
//
//        let create_producer_req_id = req_id.clone();
//        *req_id += 1;
//
//        let topic_ = topic.clone();
//        SocketAddr::from_str(addr).into_future().map_err(|e| Error::SocketAddr(e.to_string()))
//            .and_then(|addr| pulsar::connect(&addr))
//            .and_then(move |stream| pulsar::lookup_topic(stream, topic_, lookup_topic_req_id))
//            .and_then(move |stream| pulsar::create_producer(stream, topic, producer_id, create_producer_req_id))
//    }
//
//    fn build_connection_handlers(
//        stream: Pulsar,
//        message_tx: mpsc::UnboundedSender<Message>,
//        message_rx: mpsc::UnboundedReceiver<Message>,
//        err_tx: mpsc::UnboundedSender<Error>,
//    ) -> impl Future<Item=(), Error=()> {
//        let reader_err_tx = err_tx.clone();
//        let writer_err_tx = err_tx.clone();
//
//        let (sink, stream) = stream.split();
//        let receiver = message_rx
//            .map_err(|()| Error::Unexpected(String::from("Unexpected mpsc error")))
//            .forward(sink)
//            .map(|_| ())
//            .map_err(move |err| {
//                let _ = reader_err_tx.unbounded_send(err);
//            });
//
//        let sender = stream
//            .for_each(move |message: Message| {
//                if message.command.ping.is_some() {
//                    match message_tx.unbounded_send(messages::pong()) {
//                        Ok(_) => Ok(()),
//                        Err(_) => Err(Error::Disconnected)
//                    }
//                } else if message.command.close_producer.is_some() {
//                    Err(Error::Disconnected)
//                } else {
//                    println!("Unhandled message: {:?}", message);
//                    Ok(())
//                }
//            }.into_future())
//            .map_err(move |err| {
//                let _ = writer_err_tx.unbounded_send(err);
//            });
//
//        receiver.join(sender).map(|((), ())| ())
//    }
//}

pub(crate) mod messages {
    use rand;
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

    pub fn create_producer(topic: String, producer_id: u64, req_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Producer as i32,
                producer: Some(proto::CommandProducer {
                    topic,
                    producer_id: 0,
                    request_id: rand::random(),
                    .. Default::default()
                }),
                .. Default::default()
            },
            payload: None,
        }
    }

    pub fn lookup_topic(topic: String, req_id: u64) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Lookup as i32,
                lookup_topic: Some(proto::CommandLookupTopic {
                    topic,
                    request_id: rand::random(),
                    .. Default::default()
                }),
                .. Default::default()
            },
            payload: None,
        }
    }
}


#[cfg(test)]
mod tests {
//    extern crate tokio;
//    use tokio::io;
//    use tokio::net::TcpStream;
//    use tokio::prelude::*;
//    use Client;
//    use std::net::SocketAddr;
//    use messages;
//
//    #[test]
//    fn connect() {
//        let addr = "127.0.0.1:6650".parse().unwrap();
//        tokio::run(
//            Client::connect(&addr)
//                .and_then(|pulsar| {
//                    pulsar.send()
//                })
//                .map_err(|e| eprintln!("Error: {}", e))
//        );
//        panic!()
//    }
}
