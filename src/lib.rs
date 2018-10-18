extern crate bytes;
extern crate crc;
extern crate futures;
extern crate prost;
extern crate tokio;
#[macro_use] extern crate failure;
#[macro_use] extern crate nom;
#[macro_use] extern crate prost_derive;

pub mod message;

use tokio::io;
use tokio::net::TcpStream;
use tokio::prelude::*;
use std::net::SocketAddr;

use std::io::Error as IoError;


#[derive(Debug)]
pub struct Client {

}

impl Client {
    pub fn connect(addr: &SocketAddr) -> impl Future<Item = (), Error=()> {
        let connect_req = message::proto::CommandConnect{ auth_data: None,
            client_version: "2.0.1-incubating".to_string(),
            protocol_version: Some(12),
            .. Default::default()
        };
        let base_command =  message::proto::BaseCommand{ type_ : 2,
            connect: Some(connect_req),
            .. Default::default()
         };
        let message = message::Message { command: base_command,
            payload: None };
        let connection_bytes = message.encode_vec().unwrap();


        // let a: Box<Future<Item=TcpStream,Error=IoError>> = Box::new(TcpStream::connect(&addr));
        // let b: Box<Future<Item=TcpStream, Error=IoError>> = Box::new(a.and_then(|stream| {
        //     io::write_all(stream, connection_bytes).then(|result| {
        //       println!("wrote to stream; success={:?}", result.is_ok());
        //       Ok(result)
        //     })
        // });
        //
        // return b;


        let client = TcpStream::connect(&addr).and_then(|stream| {
            io::write_all(stream, connection_bytes).then(|stream| match stream {
                Ok((_, _)) => {
                    println!("wrote to stream successfully");
                    Ok(())
                },
                Err(e) => {
                    eprintln!("Error writing to stream: {}", e);
                    Err(e)
                }
            })
        })
        .map_err(|e| {
            eprintln!("An Error!!! {}", e);
        });

        println!("About to create the stream and write to it...");
        client
    }
}

struct Subscription {

}

struct Reader {

}

struct Producer {

}

#[cfg(test)]
mod tests {
    extern crate tokio;
    use tokio::io;
    use tokio::net::TcpStream;
    use tokio::prelude::*;
    use Client;
    use std::net::SocketAddr;

    #[test]
    fn connect() {
        let addr = "127.0.0.1:6650".parse().unwrap();
        let pulsarClient = Client::connect(&addr);
        tokio::run(pulsarClient);
    }
}
