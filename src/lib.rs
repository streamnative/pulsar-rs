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
//! Copy this into your project's Cargo.toml:
//!
//! ```toml
//! [dependencies]
//! env_logger = "0.9"
//! pulsar = "4.1.1"
//! serde = { version = "1.0", features = ["derive"] }
//! serde_json = "1.0"
//! tokio = { version = "1.0", features = ["macros", "rt-multi-thread"] }
//! log = "0.4.6"
//! futures = "0.3"
//! ```
//!
//! ### Producing
//! ```rust,no_run
//! use pulsar::{
//!     message::proto, producer, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct TestData {
//!     data: String,
//! }
//!
//! impl SerializeMessage for TestData {
//!     fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
//!         let payload =
//!             serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
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
//!                 r#type: proto::schema::Type::String as i32,
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
//!         tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
//!     }
//! }
//! ```
//!
//! ### Consuming
//! ```rust,no_run
//! use futures::TryStreamExt;
//! use pulsar::{
//!     message::{proto::command_subscribe::SubType, Payload},
//!     Consumer, DeserializeMessage, Pulsar, TokioExecutor,
//! };
//! use serde::{Deserialize, Serialize};
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

#![recursion_limit = "256"]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::large_enum_variant)]
extern crate futures;
#[macro_use]
extern crate log;
extern crate nom;
extern crate prost_derive;

#[cfg(test)]
#[macro_use]
extern crate serde;

#[cfg(all(feature = "tokio-rustls-runtime", feature = "tokio-runtime"))]
compile_error!("You have selected both features \"tokio-rustls-runtime\" and \"tokio-runtime\" which are exclusive, please choose one of them");
#[cfg(all(feature = "async-std-rustls-runtime", feature = "async-std-runtime"))]
compile_error!("You have selected both features \"async-std-rustls-runtime\" and \"async-std-runtime\" which are exclusive, please choose one of them");

pub use client::{DeserializeMessage, Pulsar, PulsarBuilder, SerializeMessage};
pub use connection::Authentication;
pub use connection_manager::{
    BrokerAddress, ConnectionRetryOptions, OperationRetryOptions, TlsOptions,
};
pub use consumer::{Consumer, ConsumerBuilder, ConsumerOptions};
pub use error::Error;
#[cfg(any(feature = "async-std-runtime", feature = "async-std-rustls-runtime"))]
pub use executor::AsyncStdExecutor;
pub use executor::Executor;
#[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
pub use executor::TokioExecutor;
pub use message::{
    proto::{self, command_subscribe::SubType, CommandSendReceipt},
    Payload,
};
pub use producer::{MultiTopicProducer, Producer, ProducerOptions};
pub use transaction::{Transaction, TransactionBuilder};

pub mod authentication;
mod client;
pub mod compression;
mod connection;
mod connection_manager;
pub mod consumer;
pub mod error;
pub mod executor;
pub mod message;
pub mod producer;
pub mod reader;
mod retry_op;
mod service_discovery;
pub mod transaction;

#[cfg(all(
    any(feature = "tokio-rustls-runtime", feature = "async-std-rustls-runtime"),
    not(any(feature = "tokio-runtime", feature = "async-std-runtime"))
))]
pub(crate) type Certificate = rustls::pki_types::CertificateDer<'static>;
#[cfg(any(feature = "tokio-runtime", feature = "async-std-runtime"))]
pub(crate) type Certificate = native_tls::Certificate;

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        time::{Duration, Instant},
    };

    use futures::{future::try_join_all, StreamExt};
    use log::{LevelFilter, Metadata, Record};
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    use tokio::time::timeout;

    use super::*;
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    use crate::executor::TokioExecutor;
    use crate::{
        client::SerializeMessage,
        consumer::{InitialPosition, Message},
        message::{proto::command_subscribe::SubType, Payload},
        Error as PulsarError,
    };

    #[derive(Debug, Serialize, Deserialize)]
    struct TestData {
        pub id: u64,
        pub data: String,
    }

    impl SerializeMessage for &TestData {
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
                Error::Pulsar(e) => write!(f, "{e}"),
                Error::Timeout(e) => write!(f, "{e}"),
                Error::Serde(e) => write!(f, "{e}"),
                Error::Utf8(e) => write!(f, "{e}"),
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
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    async fn round_trip() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

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
            sends.push(producer.send_non_blocking(&message).await.unwrap());
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
                initial_position: InitialPosition::Earliest,
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
            info!("id: {:?}", msg.message_id());
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
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    async fn unsized_data() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let addr = "pulsar://127.0.0.1:6650";
        let test_id: u16 = rand::random();
        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        // test &str
        {
            let topic = format!("test_unsized_data_str_{test_id}");
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
                    initial_position: InitialPosition::Earliest,
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
            let topic = format!("test_unsized_data_bytes_{test_id}");
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
                    initial_position: InitialPosition::Earliest,
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
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    async fn redelivery() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

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
                initial_position: InitialPosition::Earliest,
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

    #[tokio::test]
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    async fn transactions() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let addr = "pulsar://127.0.0.1:6650";
        let output_topic = format!("test_transactions_output_{}", rand::random::<u16>());

        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor)
            .with_transactions()
            .build()
            .await
            .unwrap();

        let mut output_producer = pulsar
            .producer()
            .with_topic(&output_topic)
            .build()
            .await
            .unwrap();

        let mut output_consumer: Consumer<String, _> = pulsar
            .consumer()
            .with_topic(&output_topic)
            .build()
            .await
            .unwrap();

        let txn = pulsar
            .new_txn()
            .unwrap()
            .with_timeout(Duration::from_secs(10))
            .build()
            .await
            .unwrap();

        output_producer
            .create_message()
            .with_content("test 1".to_string())
            .with_txn(&txn)
            .send_non_blocking()
            .await
            .unwrap();

        // Ensure that consumers cannot see messages from an open transaction
        assert!(timeout(Duration::from_secs(1), output_consumer.next())
            .await
            .is_err());

        txn.commit().await.unwrap();

        let txn = pulsar
            .new_txn()
            .unwrap()
            .with_timeout(Duration::from_secs(10))
            .build()
            .await
            .unwrap();

        // Ensure that consumers see messages from a committed transaction
        let msg = output_consumer.next().await.unwrap().unwrap();
        assert_eq!("test 1", std::str::from_utf8(&msg.payload.data).unwrap());
        output_consumer.txn_ack(&msg, &txn).await.unwrap();

        output_producer
            .create_message()
            .with_content("test 2".to_string())
            .with_txn(&txn)
            .send_non_blocking()
            .await
            .unwrap();

        txn.commit().await.unwrap();

        // We shouldn't see "test 2" now
        let msg = output_consumer.next().await.unwrap().unwrap();
        assert_eq!("test 2", std::str::from_utf8(&msg.payload.data).unwrap());

        let txn = pulsar
            .new_txn()
            .unwrap()
            .with_timeout(Duration::from_secs(10))
            .build()
            .await
            .unwrap();

        output_producer
            .create_message()
            .with_content("test 3".to_string())
            .with_txn(&txn)
            .send_non_blocking()
            .await
            .unwrap();

        txn.abort().await.unwrap();

        // We shouldn't see any more messages
        assert!(timeout(Duration::from_secs(1), output_consumer.next())
            .await
            .is_err());
    }

    #[tokio::test]
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    async fn batching() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("test_batching_{}", rand::random::<u16>());

        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();
        let mut producer = pulsar
            .producer()
            .with_topic(&topic)
            .with_options(ProducerOptions {
                batch_size: Some(5),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        let mut consumer: Consumer<String, _> =
            pulsar.consumer().with_topic(topic).build().await.unwrap();

        let mut send_receipts = Vec::new();
        for i in 0..4 {
            send_receipts.push(producer.send_non_blocking(i.to_string()).await.unwrap());
        }
        assert!(timeout(Duration::from_millis(100), consumer.next())
            .await
            .is_err());

        send_receipts.push(producer.send_non_blocking(5.to_string()).await.unwrap());

        timeout(Duration::from_millis(100), try_join_all(send_receipts))
            .await
            .unwrap()
            .unwrap();

        let mut count = 0;
        while let Some(message) = timeout(Duration::from_millis(100), consumer.next())
            .await
            .unwrap()
        {
            let message = message.unwrap();
            count += 1;
            let _ = consumer.ack(&message).await;
            if count >= 5 {
                break;
            }
        }

        assert_eq!(count, 5);
        let mut send_receipts = Vec::new();
        for i in 5..9 {
            send_receipts.push(producer.send_non_blocking(i.to_string()).await.unwrap());
        }
        producer.send_batch().await.unwrap();
        timeout(Duration::from_millis(100), try_join_all(send_receipts))
            .await
            .unwrap()
            .unwrap();
        while let Some(message) = timeout(Duration::from_millis(100), consumer.next())
            .await
            .unwrap()
        {
            let message = message.unwrap();
            count += 1;
            let _ = consumer.ack(&message).await;
            if count >= 9 {
                break;
            }
        }
        assert_eq!(count, 9);
    }
}
