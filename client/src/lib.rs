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
mod consumer;
mod producer;
mod error;
mod connection;

pub use error::Error;
pub use connection::Connection;
pub use producer::Producer;
pub use consumer::{Consumer, ConsumerBuilder, Ack};
pub use message::proto;
pub use message::proto::command_subscribe::SubType;


#[cfg(test)]
mod tests {
    use tokio;
    use futures::{Future, Stream, future};
    use super::*;
    use message::proto::command_subscribe::SubType;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestData {
        pub data: String
    }

    #[test]
    fn connect() {
        let addr = "127.0.0.1:6650";
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let mut producer = Producer::new(addr, "test_producer", runtime.executor())
            .wait()
            .unwrap();

        let consumer = ConsumerBuilder::new(addr, runtime.executor())
            .with_topic("test")
            .with_consumer_name("test_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("test_subscription")
            .build()
            .wait()
            .unwrap();

        {
            let producer = &mut producer;
            future::join_all((0..5000).map(move |_| {
                producer.send_json("test", &TestData { data: "data".to_string() })
            })).wait().unwrap();
            println!("Sent {} messages", 5000);
        }

        let mut consumed = 0;
        let consumer_result = consumer.for_each(move |data: Result<(TestData, Ack), Error>| {
            consumed += 1;
            match data {
                Ok((_msg, ack)) => {
                    ack.ack();
                    if consumed >= 5000 {
                        println!("Finished consuming");
                        Err(Error::Disconnected)
                    } else {
                        Ok(())
                    }
                },
                Err(e) => {
                    println!("Error: {}", e);
                    Ok(())
                }
            }
        }).wait();

        println!("Producer Error: {:?}", producer.error());
        println!("Consumer Result: {:?}", consumer_result);
        runtime.shutdown_now().wait().unwrap();
    }
}
