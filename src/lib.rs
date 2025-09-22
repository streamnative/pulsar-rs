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

    use assert_matches::assert_matches;
    use futures::{future::try_join_all, StreamExt};
    use log::{LevelFilter, Metadata, Record};
    use serde_json::Value;
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    use tokio::time::timeout;

    use super::*;
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    use crate::executor::TokioExecutor;
    use crate::{
        client::SerializeMessage,
        consumer::{InitialPosition, Message},
        error::ProducerError,
        message::{proto::command_subscribe::SubType, Payload},
        producer::SendFuture,
        proto::MessageIdData,
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

    const EMPTY_VALUES: Vec<String> = vec![];

    #[tokio::test]
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    async fn batching() {
        use assert_matches::assert_matches;

        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("test_batching_{}", rand::random::<u16>());

        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();
        let mut consumer: Consumer<String, _> =
            pulsar.consumer().with_topic(&topic).build().await.unwrap();

        let to_strings = |v: Vec<&str>| v.iter().map(|s| s.to_string()).collect::<Vec<String>>();

        // Case 1: batching with size enabled
        let mut producer =
            create_batched_producer(pulsar.clone(), &topic, Some(5), None, None).await;
        let mut send_futures = send_messages(&mut producer, vec!["0", "1", "2", "3"]).await;
        assert_eq!(receive_messages(&mut consumer, 1, 500).await, EMPTY_VALUES);

        send_futures.append(send_messages(&mut producer, vec!["4"]).await.as_mut());
        assert_eq!(
            receive_messages(&mut consumer, 5, 500).await,
            to_strings(vec!["0", "1", "2", "3", "4"])
        );
        assert_eq!(
            get_send_msg_ids(send_futures).await,
            vec![(0, 0, 5), (0, 1, 5), (0, 2, 5), (0, 3, 5), (0, 4, 5)]
        );

        // Case 2: batching with byte size enabled
        let mut producer =
            create_batched_producer(pulsar.clone(), &topic, None, Some(500), None).await;
        let mut send_futures = send_messages(&mut producer, vec!["first"]).await;
        assert_eq!(receive_messages(&mut consumer, 1, 500).await, EMPTY_VALUES);

        let value_with_large_size = "a".repeat(500);
        send_futures.push(
            producer
                .send_non_blocking(value_with_large_size.clone())
                .await
                .unwrap(),
        );
        assert_eq!(
            receive_messages(&mut consumer, 2, 500).await,
            vec!["first".to_string(), value_with_large_size.clone()]
        );
        assert_eq!(
            get_send_msg_ids(send_futures).await,
            vec![(1, 0, 2), (1, 1, 2)]
        );

        // Case 3: batching with timeout enabled
        let mut producer = create_batched_producer(
            pulsar.clone(),
            &topic,
            None,
            None,
            Some(Duration::from_secs(1)),
        )
        .await;
        let send_futures = send_messages(&mut producer, vec!["a"]).await;
        assert_eq!(receive_messages(&mut consumer, 1, 700).await, EMPTY_VALUES);
        assert_eq!(
            receive_messages(&mut consumer, 1, 700).await,
            vec!["a".to_string()]
        );
        assert_eq!(get_send_msg_ids(send_futures).await, vec![(2, 0, 1)]);

        pulsar.executor.delay(Duration::from_millis(800)).await;
        // If the previous timer was not reset after flushing, the batched messages will be flushed
        // again 200ms later. Here we verify the batch timer will only be scheduled after the 1st
        // message is sent.
        let send_futures = send_messages(&mut producer, vec!["b"]).await;
        assert_eq!(receive_messages(&mut consumer, 1, 300).await, EMPTY_VALUES);
        assert_eq!(
            receive_messages(&mut consumer, 1, 800).await,
            vec!["b".to_string()]
        );
        assert_eq!(get_send_msg_ids(send_futures).await, vec![(3, 0, 1)]);

        // Case 4: batching with multiple limitations
        let mut producer = create_batched_producer(
            pulsar.clone(),
            &topic,
            Some(3),
            Some(500),
            Some(Duration::from_secs(1)),
        )
        .await;
        // size limit reached
        let send_futures = send_messages(&mut producer, vec!["a", "b", "c"]).await;
        assert_eq!(
            receive_messages(&mut consumer, 3, 500).await,
            to_strings(vec!["a", "b", "c"])
        );
        assert_eq!(
            get_send_msg_ids(send_futures).await,
            vec![(4, 0, 3), (4, 1, 3), (4, 2, 3)]
        );
        // byte size limit reached
        let send_receipt = producer
            .send_non_blocking(value_with_large_size.clone())
            .await
            .unwrap();
        assert_eq!(
            receive_messages(&mut consumer, 3, 500).await,
            vec![value_with_large_size.clone()]
        );
        assert_eq!(get_send_msg_ids(vec![send_receipt]).await, vec![(5, 0, 1)]);
        // timeout reached
        let send_futures = send_messages(&mut producer, vec!["d", "e"]).await;
        assert_eq!(
            receive_messages(&mut consumer, 2, 1300).await,
            to_strings(vec!["d", "e"])
        );
        assert_eq!(
            get_send_msg_ids(send_futures).await,
            vec![(6, 0, 2), (6, 1, 2)]
        );

        // send operations after close will fail
        let _ = producer.close().await;
        let error = producer.send_non_blocking("msg").await.err().unwrap();
        assert_matches!(error, PulsarError::Producer(ProducerError::Closed));
    }

    #[tokio::test]
    #[cfg(any(feature = "tokio-runtime", feature = "tokio-rustls-runtime"))]
    async fn flush() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("test_flush_{}", rand::random::<u16>());

        let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();
        let mut consumer: Consumer<String, _> =
            pulsar.consumer().with_topic(&topic).build().await.unwrap();
        let mut producer =
            create_batched_producer(pulsar.clone(), &topic, Some(100), None, None).await;
        let send_futures = send_messages(&mut producer, vec!["0", "1", "2"]).await;
        assert_eq!(receive_messages(&mut consumer, 1, 500).await, EMPTY_VALUES);

        producer.send_batch().await.unwrap();
        // The send futures should be all completed after flush
        let msg_ids: Vec<(u64, i32, i32)> = send_futures
            .into_iter()
            .map(|mut future| {
                future
                    .0
                    .try_recv()
                    .unwrap()
                    .unwrap()
                    .unwrap()
                    .message_id
                    .unwrap()
            })
            .map(|msg_id| msg_id_to_tuple(&msg_id))
            .collect();
        assert_eq!(msg_ids, vec![(0, 0, 3), (0, 1, 3), (0, 2, 3)]);

        // Flush 0 messages should be ok
        producer.send_batch().await.unwrap();

        assert!(!is_publishers_empty(&topic).await);

        let send_futures = send_messages(&mut producer, vec!["3", "4"]).await;
        producer.close().await.unwrap();
        // After the CloseProducer RPC is done, the producer should be removed from the publishers
        // list in topic stats
        assert!(is_publishers_empty(&topic).await);

        for send_future in send_futures {
            let error = send_future.await.err().unwrap();
            assert_matches!(error, PulsarError::Producer(ProducerError::Closed));
        }

        let error = producer.send_batch().await.err().unwrap();
        assert_matches!(error, PulsarError::Producer(ProducerError::Closed));
    }

    async fn create_batched_producer<Exe>(
        pulsar: Pulsar<Exe>,
        topic: &str,
        batch_size: Option<u32>,
        batch_byte_size: Option<usize>,
        batch_timeout: Option<Duration>,
    ) -> Producer<Exe>
    where
        Exe: Executor,
    {
        pulsar
            .producer()
            .with_topic(topic)
            .with_options(ProducerOptions {
                batch_size,
                batch_byte_size,
                batch_timeout,
                ..Default::default()
            })
            .build()
            .await
            .unwrap()
    }

    async fn get_send_msg_ids(send_futures: Vec<SendFuture>) -> Vec<(u64, i32, i32)> {
        timeout(Duration::from_millis(100), try_join_all(send_futures))
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|receipt| {
                let msg_id = receipt.message_id.unwrap();
                msg_id_to_tuple(&msg_id)
            })
            .collect()
    }

    fn msg_id_to_tuple(msg_id: &MessageIdData) -> (u64, i32, i32) {
        (
            msg_id.entry_id,
            msg_id.batch_index.unwrap_or(-1),
            msg_id.batch_size.unwrap_or(-1),
        )
    }

    async fn send_messages(
        producer: &mut Producer<impl Executor>,
        values: Vec<&str>,
    ) -> Vec<SendFuture> {
        let mut send_receipts = Vec::new();
        for v in values {
            send_receipts.push(producer.send_non_blocking(v.to_string()).await.unwrap());
        }
        send_receipts
    }

    async fn receive_messages(
        consumer: &mut Consumer<String, impl Executor>,
        max_num_messages: usize,
        receive_timeout_ms: u64,
    ) -> Vec<String> {
        let mut actual_values = Vec::new();
        loop {
            match timeout(Duration::from_millis(receive_timeout_ms), consumer.next()).await {
                Ok(Some(msg)) => {
                    let msg = msg.unwrap();
                    consumer.ack(&msg).await.unwrap();
                    let data = msg.deserialize().unwrap();
                    actual_values.push(data);
                    if actual_values.len() >= max_num_messages {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    info!("timed out waiting for messages: {}", e);
                    break;
                }
            }
        }
        actual_values
    }

    async fn is_publishers_empty(topic: &String) -> bool {
        let stats_url =
            format!("http://127.0.0.1:8080/admin/v2/persistent/public/default/{topic}/stats");
        let response = reqwest::get(stats_url).await.unwrap();
        assert!(response.status().is_success());
        let json_value: Value = response.json().await.unwrap();
        if let Some(publishers) = json_value.get("publishers") {
            let publishers = publishers.as_array().unwrap();
            publishers.is_empty()
        } else {
            panic!("No publishers in the stats");
        }
    }
}
