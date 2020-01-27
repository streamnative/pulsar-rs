#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
#[macro_use]
extern crate nom;
#[macro_use]
extern crate prost_derive;

#[cfg(test)]
#[macro_use]
extern crate serde;
//
//pub use client::{DeserializeMessage, Pulsar, SerializeMessage};
//pub use connection::{Authentication, Connection};
//pub use connection_manager::ConnectionManager;
//pub use consumer::{
//    Ack, Consumer, ConsumerBuilder, ConsumerOptions, ConsumerState, Message, MultiTopicConsumer,
//};
//pub use error::{ConnectionError, ConsumerError, Error, ProducerError, ServiceDiscoveryError};
//pub use message::proto;
//pub use message::proto::command_subscribe::SubType;
//pub use producer::{Producer, ProducerOptions, TopicProducer};
//pub use service_discovery::ServiceDiscovery;

//mod client;
mod connection;
mod connection_manager;
mod consumer;
mod error;
mod engine;
mod util;
mod resolver;
mod proto;
mod message;
pub mod producer;
//mod service_discovery;

//#[cfg(test)]
//mod tests {
//    use std::time::{Duration, Instant};
//
//    use futures::{future, Future, Stream};
//    use futures_timer::ext::FutureExt;
//    use tokio;
//
//    use message::proto::command_subscribe::SubType;
//
//    use crate::client::SerializeMessage;
//    use crate::consumer::Message;
//    use crate::message::Payload;
//    use crate::Error as PulsarError;
//
//    use super::*;
//    use std::collections::BTreeMap;
//    use nom::lib::std::collections::BTreeSet;
//    use rand::Rng;
//    use rand::distributions::Alphanumeric;
//
//    #[derive(Debug, Serialize, Deserialize)]
//    struct TestData {
//        pub data: String,
//    }
//
//    impl SerializeMessage for TestData {
//        fn serialize_message(input: &Self) -> Result<producer::Message, PulsarError> {
//            let payload =
//                serde_json::to_vec(input).map_err(|e| PulsarError::Custom(e.to_string()))?;
//            Ok(producer::Message {
//                payload,
//                ..Default::default()
//            })
//        }
//    }
//
//    impl DeserializeMessage for TestData {
//        type Output = Result<TestData, serde_json::Error>;
//
//        fn deserialize_message(payload: Payload) -> Self::Output {
//            serde_json::from_slice(&payload.data)
//        }
//    }
//
//    #[derive(Debug)]
//    enum Error {
//        Pulsar(PulsarError),
//        Message(String),
//        Timeout(std::io::Error),
//        Serde(serde_json::Error),
//    }
//
//    impl From<std::io::Error> for Error {
//        fn from(e: std::io::Error) -> Self {
//            Error::Timeout(e)
//        }
//    }
//
//    impl From<PulsarError> for Error {
//        fn from(e: PulsarError) -> Self {
//            Error::Pulsar(e)
//        }
//    }
//
//    impl From<serde_json::Error> for Error {
//        fn from(e: serde_json::Error) -> Self {
//            Error::Serde(e)
//        }
//    }
//
//    impl std::fmt::Display for Error {
//        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//            match self {
//                Error::Pulsar(e) => write!(f, "{}", e),
//                Error::Message(e) => write!(f, "{}", e),
//                Error::Timeout(e) => write!(f, "{}", e),
//                Error::Serde(e) => write!(f, "{}", e),
//            }
//        }
//    }
//
//    #[test]
//    #[ignore]
//    fn round_trip() {
//        let addr = "127.0.0.1:6650".parse().unwrap();
//        let runtime = tokio::runtime::Runtime::new().unwrap();
//
//        let pulsar: Pulsar = Pulsar::new(addr, None, runtime.executor()).wait().unwrap();
//
//        let producer = pulsar.producer(None);
//
//        future::join_all((0..5000).map(|_| {
//            producer.send(
//                "test",
//                &TestData {
//                    data: "data".to_string(),
//                },
//            )
//        }))
//        .map_err(|e| Error::from(e))
//        .timeout(Duration::from_secs(5))
//        .wait()
//        .unwrap();
//
//        let consumer: Consumer<TestData> = pulsar
//            .consumer()
//            .with_topic("test")
//            .with_consumer_name("test_consumer")
//            .with_subscription_type(SubType::Exclusive)
//            .with_subscription("test_subscription")
//            .build()
//            .wait()
//            .unwrap();
//
//        consumer
//            .take(5000)
//            .map_err(|e| e.into())
//            .for_each(move |Message { payload, ack, .. }| {
//                ack.ack();
//                let data = payload?;
//                if data.data.as_str() == "data" {
//                    Ok(())
//                } else {
//                    Err(Error::Message(format!(
//                        "Unexpected payload: {}",
//                        &data.data
//                    )))
//                }
//            })
//            .timeout(Duration::from_secs(5))
//            .wait()
//            .unwrap();
//    }
//
//    #[test]
//    #[ignore]
//    fn redelivery() {
//        let addr = "127.0.0.1:6650".parse().unwrap();
//        let runtime = tokio::runtime::Runtime::new().unwrap();
//
//        let pulsar: Pulsar = Pulsar::new(addr, None, runtime.executor()).wait().unwrap();
//
//        let producer = pulsar.producer(None);
//
//        let topic: String = std::iter::repeat(())
//            .map(|()| rand::thread_rng().sample(Alphanumeric))
//            .take(7)
//            .collect();
//
//        let resend_delay = Duration::from_secs(4);
//
//        let consumer: Consumer<TestData> = pulsar
//            .consumer()
//            .with_topic(&topic)
//            .with_consumer_name("test_consumer")
//            .with_subscription_type(SubType::Exclusive)
//            .with_subscription("test_subscription")
//            .with_unacked_message_resend_delay(Some(resend_delay))
//            .build()
//            .wait()
//            .unwrap();
//
//        let message_count = 10;
//
//        future::join_all((0..message_count)
//            .map(|i| {
//                producer.send(
//                    topic.clone(),
//                    &TestData {
//                        data: i.to_string(),
//                    },
//                )
//            }))
//            .map_err(|e| Error::from(e))
//            .timeout(Duration::from_secs(5))
//            .wait()
//            .unwrap();
//
//        let (tx, rx) = std::sync::mpsc::channel();
//
//        let mut seen = BTreeSet::new();
//        let start = Instant::now();
//        runtime.executor().spawn(
//            consumer
//                .take(message_count * 2)
//                .map_err(|e| Error::from(e))
//                .for_each(move |Message { payload, ack, .. }| {
//                    let data = payload?;
//                    tx.send(data.data.clone()).unwrap();
//                    if !seen.contains(&data.data) {
//                        seen.insert(data.data);
//                    } else {
//                        //ack the second time around
//                        ack.ack();
//                    }
//                    Ok(())
//                    // no ack
//                })
//                .timeout(Duration::from_secs(15))
//                .map_err(|e| panic!("{}", e))
//        );
//
//        let mut counts = BTreeMap::new();
//
//        let timeout = Instant::now() + Duration::from_secs(2);
//        let mut read_count = 0;
//
//        while read_count < message_count {
//            if let Ok(data) = rx.try_recv() {
//                read_count += 1;
//                *counts.entry(data).or_insert(0) += 1;
//            } else {
//                std::thread::sleep(Duration::from_millis(10));
//            }
//            if Instant::now() > timeout {
//                panic!("timed out waiting for messages to be read");
//            }
//        };
//        //check all messages we received are correct
//        (0..message_count).for_each(|i| {
//            let count = counts.get(&i.to_string());
//            if counts.get(&i.to_string()) != Some(&1) {
//                println!("Expected {} count to be {}, found {}", i, 1, count.cloned().unwrap_or(0));
//                panic!("{:?}", counts);
//            }
//        });
//        let mut redelivery_start = None;
//        let timeout = Instant::now() + resend_delay + Duration::from_secs(4);
//        while read_count < 2 * message_count {
//            if let Ok(data) = rx.try_recv() {
//                if redelivery_start.is_none() {
//                    redelivery_start = Some(Instant::now());
//                }
//                read_count += 1;
//                *counts.entry(data).or_insert(0) += 1;
//            } else {
//                std::thread::sleep(Duration::from_millis(10));
//            }
//            if Instant::now() > timeout {
//                println!("{:?}", counts);
//                panic!("timed out waiting for messages to be read");
//            }
//        };
//        let redelivery_start = redelivery_start.unwrap();
//        let expected_redelivery_start = start + resend_delay;
//        println!(
//            "start: 0ms, delay: {}ms, redelivery_start: {}ms, expected_redelivery: {}ms",
//            resend_delay.as_millis(), (redelivery_start - start).as_millis(), (expected_redelivery_start - start).as_millis()
//        );
//        assert!(redelivery_start > expected_redelivery_start - Duration::from_secs(1));
//        assert!(redelivery_start < expected_redelivery_start + Duration::from_secs(1));
//        (0..message_count).for_each(|i| {
//            let count = counts.get(&i.to_string());
//            if count != Some(&2) {
//                println!("Expected {} count to be {}, found {}", i, 2, count.cloned().unwrap_or(0));
//                panic!("{:?}", counts);
//            }
//        });
//    }
//}
