extern crate bytes;
extern crate chrono;
extern crate crc;
extern crate futures_timer;
extern crate prost;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate tokio_codec;
#[macro_use] extern crate failure;
#[macro_use] extern crate futures;
#[macro_use] extern crate nom;
#[macro_use] extern crate prost_derive;

#[cfg(test)] #[macro_use] extern crate serde_derive;

pub mod message;
mod producer;
mod error;
mod connection;

pub use error::Error;
pub use connection::Connection;
pub use producer::Producer;


#[cfg(test)]
mod tests {

    use std::time::Duration;
    use tokio;
    use futures::{Future, future};
    use futures_timer::FutureExt;
    use super::*;

    #[derive(Debug, Serialize)]
    struct TestData {
        pub data: &'static str
    }

    #[test]
    fn connect() {
        let (producer, a, b) = Producer::new("127.0.0.1:6650", "test", None)
            .timeout(Duration::from_secs(2))
            .wait()
            .unwrap();

        ::std::thread::spawn(move || tokio::run(a));
        ::std::thread::spawn(move || tokio::run(b));

        let mut producer: Producer<TestData> = producer.wait().unwrap();

        let mut batch_n = 1;
        loop {
            let batch = {
                let producer = &mut producer;
                future::join_all((1..5000).map(move |_| {
                    producer.send(&TestData { data: "data" })
                }))
            };
            if let Err(err) = batch.wait() {
                println!("batch error: {}", err);
                break;
            }
            println!("Sent {} messages", batch_n * 5000);
            batch_n += 1;
        }

        println!("Error: {:?}", producer.error());
        panic!()
    }
}
