#[macro_use] extern crate futures;
#[macro_use] extern crate nom;
#[macro_use] extern crate prost_derive;
#[macro_use] extern crate log;

#[cfg(test)] #[macro_use] extern crate serde_derive;

pub mod message;
mod consumer;
mod producer;
mod error;
mod connection;

pub use error::{Error, ConnectionError, ConsumerError, ProducerError};
pub use connection::{Connection, Authentication};
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
    #[ignore]
    fn connect() {
        let addr = "127.0.0.1:6650";
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let mut producer = Producer::new(addr, "test_producer", None, None, runtime.executor())
            .wait()
            .unwrap();

        let produce = {
            let producer = &mut producer;
            future::join_all((0..5000).map(move |_| {
                producer.send_json("test", &TestData { data: "data".to_string() })
            }))
        };

        let consumer = ConsumerBuilder::new(addr, runtime.executor())
            .with_topic("test")
            .with_consumer_name("test_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("test_subscription")
            .build()
            .wait()
            .unwrap();

        produce.wait().unwrap();

        let mut consumed = 0;
        let _ = consumer.for_each(move |data: Result<(TestData, Ack), ConsumerError>| {
            consumed += 1;
            match data {
                Ok((_msg, ack)) => {
                    ack.ack();
                    if consumed >= 5000 {
                        println!("Finished consuming");
                        Err(ConsumerError::Connection(ConnectionError::Disconnected))
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

        runtime.shutdown_now().wait().unwrap();
    }
}
