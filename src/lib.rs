//! # Pure Rust async await client for Apache Pulsar
//!
//! This is a pure Rust client for Apache Pulsar that does not depend on the
//! C++ Pulsar library. It provides an async/await based API, compatible with
//! [Tokio](https://tokio.rs/) and [async-std](https://async.rs/).
//!
//! Features:
//! - URL based (`pulsar://` and `pulsar+ssl://`) connections with DNS lookup
//! - multi topic consumers (based on a regex)
//! - TLS connection
//! - configurable executor (Tokio or async-std)
//! - automatic reconnection with exponential back off
//! - message batching
//! - compression with LZ4, zlib, zstd or Snappy (can be deactivated with Cargo features)
//!
//! ## Examples
//!
//! ### Producing
//! ```rust,no_run
//! #[macro_use]
//! extern crate serde;
//! use pulsar::{
//!     message::proto, producer, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
//! };
//!
//! #[derive(Serialize, Deserialize)]
//! struct TestData {
//!     data: String,
//! }
//!
//! impl SerializeMessage for TestData {
//!     fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
//!         let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
//!         Ok(producer::Message {
//!             payload,
//!             ..Default::default()
//!         })
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), pulsar::Error> {
//!     env_logger::init();
//!
//!     let addr = "pulsar://127.0.0.1:6650";
//!     let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
//!     let mut producer = pulsar
//!         .producer()
//!         .with_topic("non-persistent://public/default/test")
//!         .with_name("my producer")
//!         .with_options(producer::ProducerOptions {
//!             schema: Some(proto::Schema {
//!                 type_: proto::schema::Type::String as i32,
//!                 ..Default::default()
//!             }),
//!             ..Default::default()
//!         })
//!         .build()
//!         .await?;
//!
//!     let mut counter = 0usize;
//!     loop {
//!         producer
//!             .send(TestData {
//!                 data: "data".to_string(),
//!             })
//!             .await?;
//!
//!         counter += 1;
//!         println!("{} messages", counter);
//!         tokio::time::delay_for(std::time::Duration::from_millis(2000)).await;
//!     }
//! }
//! ```
//!
//! ### Consuming
//! ```rust,no_run
//! #[macro_use]
//! extern crate serde;
//! use futures::TryStreamExt;
//! use pulsar::{
//!     message::proto::command_subscribe::SubType, message::Payload, Consumer, DeserializeMessage,
//!     Pulsar, TokioExecutor,
//! };
//!
//! #[derive(Serialize, Deserialize)]
//! struct TestData {
//!     data: String,
//! }
//!
//! impl DeserializeMessage for TestData {
//!     type Output = Result<TestData, serde_json::Error>;
//!
//!     fn deserialize_message(payload: &Payload) -> Self::Output {
//!         serde_json::from_slice(&payload.data)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), pulsar::Error> {
//!     env_logger::init();
//!
//!     let addr = "pulsar://127.0.0.1:6650";
//!     let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
//!
//!     let mut consumer: Consumer<TestData, _> = pulsar
//!         .consumer()
//!         .with_topic("test")
//!         .with_consumer_name("test_consumer")
//!         .with_subscription_type(SubType::Exclusive)
//!         .with_subscription("test_subscription")
//!         .build()
//!         .await?;
//!
//!     let mut counter = 0usize;
//!     while let Some(msg) = consumer.try_next().await? {
//!         consumer.ack(&msg).await?;
//!         let data = match msg.deserialize() {
//!             Ok(data) => data,
//!             Err(e) => {
//!                 log::error!("could not deserialize message: {:?}", e);
//!                 break;
//!             }
//!         };
//!
//!         if data.data.as_str() != "data" {
//!             log::error!("Unexpected payload: {}", &data.data);
//!             break;
//!         }
//!         counter += 1;
//!         log::info!("got {} messages", counter);
//!     }
//!
//!     Ok(())
//! }
//! ```

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

pub use client::{DeserializeMessage, Pulsar, SerializeMessage};
pub use connection::Authentication;
pub use connection_manager::{BackOffOptions, BrokerAddress, TlsOptions};
pub use consumer::{Consumer, ConsumerBuilder, ConsumerOptions};
pub use error::Error;
#[cfg(feature = "async-std-runtime")]
pub use executor::AsyncStdExecutor;
pub use executor::Executor;
#[cfg(feature = "tokio-runtime")]
pub use executor::TokioExecutor;
pub use message::proto::command_subscribe::SubType;
pub use producer::{MultiTopicProducer, Producer, ProducerOptions};
pub use message::{Payload, proto::{self, CommandSendReceipt}};

mod client;
mod connection;
mod connection_manager;
pub mod consumer;
pub mod error;
mod executor;
pub mod message;
pub mod producer;
mod service_discovery;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::{Duration, Instant};
    use log::{LevelFilter, Metadata, Record};
    use futures::{future::try_join_all, StreamExt};

    #[cfg(feature = "tokio-runtime")]
    use tokio::time::timeout;

    #[cfg(feature = "tokio-runtime")]
    use crate::executor::TokioExecutor;

    use crate::client::SerializeMessage;
    use crate::message::Payload;
    use crate::Error as PulsarError;
    use crate::consumer::Message;
    use crate::message::proto::command_subscribe::SubType;

    use super::*;


    #[derive(Debug, Serialize, Deserialize)]
    struct TestData {
        pub id: u64,
        pub data: String,
    }

    impl<'a> SerializeMessage for &'a TestData {
        fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
            let payload =
                serde_json::to_vec(input).map_err(|e| PulsarError::Custom(e.to_string()))?;
            Ok(producer::Message {
                payload,
                ..Default::default()
            })
        }
    }

    impl DeserializeMessage for TestData {
        type Output = Result<TestData, serde_json::Error>;

        fn deserialize_message(payload: &Payload) -> Self::Output {
            serde_json::from_slice(&payload.data)
        }
    }

    #[derive(Debug)]
    enum Error {
        Pulsar(PulsarError),
        Timeout(std::io::Error),
        Serde(serde_json::Error),
        Utf8(std::string::FromUtf8Error),
    }

    impl From<std::io::Error> for Error {
        fn from(e: std::io::Error) -> Self {
            Error::Timeout(e)
        }
    }

    impl From<PulsarError> for Error {
        fn from(e: PulsarError) -> Self {
            Error::Pulsar(e)
        }
    }

    impl From<serde_json::Error> for Error {
        fn from(e: serde_json::Error) -> Self {
            Error::Serde(e)
        }
    }

    impl From<std::string::FromUtf8Error> for Error {
        fn from(err: std::string::FromUtf8Error) -> Self {
            Error::Utf8(err)
        }
    }

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                Error::Pulsar(e) => write!(f, "{}", e),
                Error::Timeout(e) => write!(f, "{}", e),
                Error::Serde(e) => write!(f, "{}", e),
                Error::Utf8(e) => write!(f, "{}", e),
            }
        }
    }

    pub struct SimpleLogger {
        pub tag: &'static str,
    }
    impl log::Log for SimpleLogger {
        fn enabled(&self, _metadata: &Metadata) -> bool {
            //metadata.level() <= Level::Info
            true
        }

        fn log(&self, record: &Record) {
            if self.enabled(record.metadata()) {
                println!(
                    "{} {} {}\t{}\t{}",
                    chrono::Utc::now(),
                    self.tag,
                    record.level(),
                    record.module_path().unwrap(),
                    record.args()
                );
            }
        }
        fn flush(&self) {}
    }

    pub static TEST_LOGGER: SimpleLogger = SimpleLogger { tag: "" };

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn round_trip() {
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);

        let addr = "pulsar://127.0.0.1:6650";
        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        // random topic to better allow multiple test runs while debugging
        let topic = format!("test_{}", rand::random::<u16>());

        let mut producer = pulsar.producer().with_topic(&topic).build().await.unwrap();
        info!("producer created");

        let message_ids: BTreeSet<u64> = (0..100).collect();

        info!("will send message");
        let mut sends = Vec::new();
        for &id in &message_ids {
            let message = TestData {
                data: "data".to_string(),
                id,
            };
            sends.push(producer.send(&message).await.unwrap());
        }
        try_join_all(sends).await.unwrap();

        info!("sent");

        let mut consumer: Consumer<TestData, _> = pulsar
            .consumer()
            .with_topic(&topic)
            .with_consumer_name("test_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("test_subscription")
            .with_options(ConsumerOptions {
                initial_position: Some(1),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        info!("consumer created");

        let topics = consumer.topics();
        debug!("consumer connected to {:?}", topics);
        assert_eq!(topics.len(), 1);
        assert!(topics[0].ends_with(&topic));

        let mut received = BTreeSet::new();
        while let Ok(Some(msg)) = timeout(Duration::from_secs(10), consumer.next()).await {
            let msg: Message<TestData> = msg.unwrap();
            received.insert(msg.deserialize().unwrap().id);
            consumer.ack(&msg).await.unwrap();
            if received.len() == message_ids.len() {
                break;
            }
        }
        assert_eq!(received.len(), message_ids.len());
        assert_eq!(received, message_ids);
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn unsized_data() {
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);

        let addr = "pulsar://127.0.0.1:6650";
        let test_id: u16 = rand::random();
        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        // test &str
        {
            let topic = format!("test_unsized_data_str_{}", test_id);
            let send_data = "some unsized data";

            pulsar
                .send(&topic, send_data.to_string())
                .await
                .unwrap()
                .await
                .unwrap();

            let mut consumer = pulsar
                .consumer()
                .with_topic(&topic)
                .with_subscription_type(SubType::Exclusive)
                .with_subscription("test_subscription")
                .with_options(ConsumerOptions {
                    initial_position: Some(1),
                    ..Default::default()
                })
                .build::<String>()
                .await
                .unwrap();

            let msg = timeout(Duration::from_secs(1), consumer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            consumer.ack(&msg).await.unwrap();

            let data = msg.deserialize().unwrap();
            if data.as_str() != send_data {
                panic!("Unexpected payload in &str test: {}", &data);
            }
        }

        // test &[u8]
        {
            let topic = format!("test_unsized_data_bytes_{}", test_id);
            let send_data: &[u8] = &[0, 1, 2, 3];

            pulsar
                .send(&topic, send_data.to_vec())
                .await
                .unwrap()
                .await
                .unwrap();

            let mut consumer = pulsar
                .consumer()
                .with_topic(&topic)
                .with_subscription_type(SubType::Exclusive)
                .with_subscription("test_subscription")
                .with_options(ConsumerOptions {
                    initial_position: Some(1),
                    ..Default::default()
                })
                .build::<Vec<u8>>()
                .await
                .unwrap();

            let msg: Message<Vec<u8>> = timeout(Duration::from_secs(1), consumer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            consumer.ack(&msg).await.unwrap();
            let data = msg.deserialize();
            if data.as_slice() != send_data {
                panic!("Unexpected payload in &[u8] test: {:?}", &data);
            }
        }
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn redelivery() {
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);

        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("test_redelivery_{}", rand::random::<u16>());

        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();
        pulsar
            .send(&topic, String::from("data"))
            .await
            .unwrap()
            .await
            .unwrap();

        let mut consumer: Consumer<String, _> = pulsar
            .consumer()
            .with_topic(topic)
            .with_unacked_message_resend_delay(Some(Duration::from_millis(100)))
            .with_options(ConsumerOptions {
                initial_position: Some(1),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        let _first_receipt = timeout(Duration::from_secs(2), consumer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let first_received = Instant::now();
        let second_receipt = timeout(Duration::from_secs(2), consumer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let redelivery = first_received.elapsed();
        consumer.ack(&second_receipt).await.unwrap();

        assert!(redelivery < Duration::from_secs(1));
    }
}
