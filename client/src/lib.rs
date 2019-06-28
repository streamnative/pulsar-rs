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
mod connection_manager;
mod service_discovery;
mod client;

pub use error::{Error, ConnectionError, ConsumerError, ProducerError, ServiceDiscoveryError};
pub use connection::{Connection, Authentication};
pub use connection_manager::ConnectionManager;
pub use producer::Producer;
pub use consumer::{Consumer, ConsumerBuilder, MultiTopicConsumer, ConsumerState, Message, Ack};
pub use service_discovery::ServiceDiscovery;
pub use client::{Pulsar, DeserializeMessage};
pub use message::proto;
pub use message::proto::command_subscribe::SubType;

#[cfg(test)]
mod tests {
    use tokio;
    use futures::{Future, Stream, future};
    use super::*;
    use message::proto::command_subscribe::SubType;
    use crate::consumer::Message;
    use crate::message::Payload;
    use crate::client::SerializeMessage;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestData {
        pub data: String
    }

    impl SerializeMessage for TestData {
        fn serialize_message(input: &Self) -> Result<producer::Message, ProducerError> {
            let payload = serde_json::to_vec(input)?;
            Ok(producer::Message { payload, ..Default::default() })
        }
    }

    impl DeserializeMessage for TestData {
        type Output = Result<TestData, serde_json::Error>;

        fn deserialize_message(payload: Payload) -> Self::Output {
            serde_json::from_slice(&payload.data)
        }
    }

    #[test]
    #[ignore]
    fn connect() {
        let addr = "127.0.0.1:6650".parse().unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let pulsar: Pulsar = Pulsar::new(addr, None, runtime.executor())
            .wait().unwrap();

        let producer = pulsar.producer::<TestData>();

        let produce = {
            future::join_all((0..5000).map(|_| {
                producer.send("test", &TestData { data: "data".to_string() })
            }))
        };

        let consumer: Consumer<TestData> = pulsar.consumer()
            .with_topic("test")
            .with_consumer_name("test_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("test_subscription")
            .build()
            .wait()
            .unwrap();

        produce.wait().unwrap();

        let mut consumed = 0;
        let _ = consumer.for_each(move |Message { payload, ack, .. }| {
            consumed += 1;
            ack.ack();
            if let Err(e) = payload {
                println!("Error: {}", e);
            }
            if consumed >= 5000 {
                println!("Finished consuming");
                Err(ConsumerError::Connection(ConnectionError::Disconnected))
            } else {
                Ok(())
            }
        }).wait();

        runtime.shutdown_now().wait().unwrap();
    }
}
