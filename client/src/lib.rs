extern crate bytes;
extern crate crc;
extern crate prost;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate tokio_codec;
#[macro_use] extern crate futures;
#[macro_use] extern crate failure;
#[macro_use] extern crate nom;
#[macro_use] extern crate prost_derive;

pub mod message;
mod producer;
mod error;
mod connection;

pub use error::Error;
pub use connection::Connection;


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
