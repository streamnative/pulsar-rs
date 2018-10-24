//extern crate bytes;
//extern crate crc;
//extern crate futures;
//extern crate prost;
//extern crate tokio;
//extern crate tokio_codec;
//#[macro_use] extern crate failure;
//#[macro_use] extern crate nom;
//#[macro_use] extern crate prost_derive;
//
//pub mod message;
//mod codec;
//
//use tokio::net::TcpStream;
//use tokio::prelude::*;
//use std::net::SocketAddr;
//use futures::{Stream, Sink};
//
//use failure::Error;
//use message::{Message, proto};
//
//#[derive(Debug)]
//pub struct Client {
//
//}
//
//type Messages = tokio_codec::Framed<TcpStream, codec::PularCodec>;
//
//impl Client {
//    pub fn connect(addr: &SocketAddr) -> impl Future<Item = Messages, Error = Error> {
//        TcpStream::connect(&addr)
//            .map_err(|e| format_err!("{}", e))
//            .map(|stream| tokio_codec::Framed::new(stream, codec::PularCodec))
//            .and_then(|messages| {
//                messages.send(messages::connect())
//                    .and_then(|messages| {
//                        messages.into_future()
//                            .map_err(|(e, _)| e)
//                            .and_then(|(message, messages)| {
//                                future::result(match message {
//                                    Some(Message { command: proto::BaseCommand { connected: Some(connected), .. }, .. }) => {
//                                        println!("Successfully connected: {:?}", connected);
//                                        Ok(messages)
//                                    },
//                                    Some(Message { command: proto::BaseCommand { error: Some(error), .. }, .. }) => {
//                                        Err(format_err!("Error connecting to Pulsar: {:?}", error))
//                                    },
//                                    None => {
//                                        Err(format_err!("Pulsar connection dropped while attempting to connect"))
//                                    },
//                                    message => {
//                                        Err(format_err!("Unexpected error connecting to pulsar. Received message {:?}", message))
//                                    }
//                                })
//                            })
//                            .and_then(|messages| {
//                                messages.filter_map(|msg| match msg.command {
//                                    proto::BaseCommand { ping: Some(ping) } => {
//
//                                    }
//                                })
//                            })
//                    })
//            })
//    }
//}
//
//struct Subscription {
//
//}
//
//struct Reader {
//
//}
//
//struct Producer {
//
//}
//
//mod messages {
//    use message::{Message, proto};
//    pub fn connect() -> Message {
//        Message {
//            command: proto::BaseCommand {
//                type_: 2,
//                connect: Some(proto::CommandConnect {
//                    auth_data: None,
//                    client_version: "2.0.1-incubating".to_owned(),
//                    protocol_version: Some(12),
//                    .. Default::default()
//                }),
//                .. Default::default()
//            },
//            payload: None,
//        }
//    }
//
//    pub fn
//}
//
//#[cfg(test)]
//mod tests {
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
//}
