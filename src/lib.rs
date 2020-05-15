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
pub use connection::{Authentication, Connection};
pub use connection_manager::ConnectionManager;
pub use consumer::{
    Ack, Consumer, ConsumerBuilder, ConsumerOptions, ConsumerState, Message, MultiTopicConsumer,
};
pub use error::{ConnectionError, ConsumerError, Error, ProducerError, ServiceDiscoveryError};
pub use executor::{Executor, TaskExecutor, TokioExecutor};
pub use message::proto;
pub use message::proto::command_subscribe::SubType;
pub use producer::{Producer, ProducerOptions, TopicProducer};
pub use service_discovery::ServiceDiscovery;

mod client;
mod connection;
mod connection_manager;
mod error;
mod executor;
pub mod consumer;
pub mod message;
pub mod producer;
mod service_discovery;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use futures::{StreamExt, TryStreamExt};
    use tokio;
    use tokio::runtime::Runtime;

    use message::proto::command_subscribe::SubType;

    use crate::client::SerializeMessage;
    use crate::consumer::Message;
    use crate::message::Payload;
    use crate::Error as PulsarError;
    use crate::executor::TokioExecutor;

    use super::*;
    use nom::lib::std::collections::BTreeSet;
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use std::collections::BTreeMap;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestData {
        pub data: String,
    }

    impl SerializeMessage for TestData {
        fn serialize_message(input: &Self) -> Result<producer::Message, PulsarError> {
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

        fn deserialize_message(payload: Payload) -> Self::Output {
            serde_json::from_slice(&payload.data)
        }
    }

    #[derive(Debug)]
    enum Error {
        Pulsar(PulsarError),
        Message(String),
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
                Error::Message(e) => write!(f, "{}", e),
                Error::Timeout(e) => write!(f, "{}", e),
                Error::Serde(e) => write!(f, "{}", e),
                Error::Utf8(e) => write!(f, "{}", e),
            }
        }
    }

    use log::{Record, Metadata, LevelFilter};
    pub struct SimpleLogger;
    impl log::Log for SimpleLogger {
        fn enabled(&self, _metadata: &Metadata) -> bool {
            //metadata.level() <= Level::Info
            true
        }

        fn log(&self, record: &Record) {
            if self.enabled(record.metadata()) {
                println!("{} {}\t{}\t{}", chrono::Utc::now(),
                  record.level(), record.module_path().unwrap(), record.args());
            }
        }
        fn flush(&self) {}
    }

    pub static TEST_LOGGER: SimpleLogger = SimpleLogger;

    #[test]
    #[ignore]
    fn round_trip() {
        let mut runtime = Runtime::new().unwrap();
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);

        let f = async {
            let addr = "127.0.0.1:6650";
            let executor = TokioExecutor(tokio::runtime::Handle::current());
            let pulsar: Pulsar = Pulsar::new(addr, None, executor).await.unwrap();
            let producer = pulsar.producer(None);

            for _ in 0u16..5000 {
                producer.send(
                    "test",
                    TestData {
                        data: "data".to_string(),
                    },
                ).await.unwrap();
            }

            let consumer: Consumer<TestData> = pulsar
                .consumer()
                .with_topic("test")
                .with_consumer_name("test_consumer")
                .with_subscription_type(SubType::Exclusive)
                .with_subscription("test_subscription")
                .build()
                .await
                .unwrap();

            let mut stream = consumer.take(5000);
            while let Some(res) = stream.next().await {
                    let Message { payload, ack, .. } = res.unwrap();
                    ack.ack();
                    let data = payload.unwrap();
                    if data.data.as_str() != "data" {
                        panic!("Unexpected payload: {}", &data.data);
                    }
                }
            // FIXME .timeout(Duration::from_secs(5))
        };

        runtime.block_on(Box::pin(f));
    }

    #[test]
    #[ignore]
    fn unsized_data() {
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let mut runtime = Runtime::new().unwrap();

        let f = async {
            let executor = TokioExecutor(tokio::runtime::Handle::current());
            let addr = "127.0.0.1:6650";
            let pulsar: Pulsar = Pulsar::new(addr, None, executor).await.unwrap();
            let producer = pulsar.producer(None);

            // test &str
            {
                let topic = "test_unsized_data_str";
                let send_data = "some unsized data";

                let consumer = pulsar
                    .consumer()
                    .with_topic(topic)
                    .with_subscription_type(SubType::Exclusive)
                    .with_subscription("test_subscription")
                    .build::<String>()
                    .await
                    .unwrap();

                producer.send(topic, send_data.to_string()).await.unwrap();

                let mut stream = consumer.take(1);
                while let Some(res) = stream.next().await {

                    let Message { payload, ack, .. } = res.unwrap();

                    ack.ack();
                    let data = payload.unwrap();
                    if data.as_str() != send_data {
                        panic!("Unexpected payload in &str test: {}", &data);
                    }
                }
                //FIXME .timeout(Duration::from_secs(1))
            }

            // test &[u8]
            {
                let topic = "test_unsized_data_bytes";
                let send_data: &[u8] = &[0, 1, 2, 3];

                let consumer = pulsar
                    .consumer()
                    .with_topic(topic)
                    .with_subscription_type(SubType::Exclusive)
                    .with_subscription("test_subscription")
                    .build::<Vec<u8>>()
                    .await
                    .unwrap();

                let message = producer::Message { payload: send_data.to_vec(), ..Default::default() };
                producer.send_message(topic, message).await.unwrap();


                let mut stream = consumer.take(1);
                while let Some(res) = stream.next().await {
                        let Message { payload, ack, .. } = res.unwrap();
                        ack.ack();
                        let data = payload;
                        if data.as_slice() != send_data {
                            panic!("Unexpected payload in &[u8] test: {:?}", &data);
                        }
                    }
                //FIXME .timeout(Duration::from_secs(1))
            }
        };

        runtime.block_on(Box::pin(f));
    }

    #[test]
    #[ignore]
    fn redelivery() {
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let runtime = Runtime::new().unwrap();

        let addr = "127.0.0.1:6650";
        let (tx, rx) = std::sync::mpsc::channel();

        let mut seen = BTreeSet::new();
        let start = Instant::now();
        let resend_delay = Duration::from_secs(4);

        let message_count = 10;
        let f = async move {
            let executor = TokioExecutor(tokio::runtime::Handle::current());
            let pulsar: Pulsar = Pulsar::new(addr, None, executor).await.unwrap();

            let producer = pulsar.producer(None);

            let topic: String = std::iter::repeat(())
                .map(|()| rand::thread_rng().sample(Alphanumeric))
                .take(7)
                .collect();

            let consumer: Consumer<TestData> = pulsar
                .consumer()
                .with_topic(&topic)
                .with_consumer_name("test_consumer")
                .with_subscription_type(SubType::Exclusive)
                .with_subscription("test_subscription")
                .with_unacked_message_resend_delay(Some(resend_delay))
                .build()
                .await
                .unwrap();


            for i in 0u8..message_count {
                producer.send(
                    topic.clone(),
                    TestData {
                        data: i.to_string(),
                    },
                    ).await.unwrap();
            }

            let mut stream = consumer
                .take(message_count as usize * 2)
                .map_err(|e| Error::from(e));
            while let Some(res) = stream.next().await {

                let Message { payload, ack, .. } = res.unwrap();
                let data = payload.unwrap();
                tx.send(data.data.clone()).unwrap();
                if !seen.contains(&data.data) {
                    seen.insert(data.data);
                } else {
                    //ack the second time around
                    ack.ack();
                }
            }
                //FIXME .timeout(Duration::from_secs(15))
        };


        runtime.spawn(Box::pin(f));

        let mut counts = BTreeMap::new();

        let timeout = Instant::now() + Duration::from_secs(2);
        let mut read_count = 0;

        while read_count < message_count {
            if let Ok(data) = rx.try_recv() {
                read_count += 1;
                *counts.entry(data).or_insert(0) += 1;
            } else {
                std::thread::sleep(Duration::from_millis(10));
            }
            if Instant::now() > timeout {
                panic!("timed out waiting for messages to be read");
            }
        }

        //check all messages we received are correct
        (0..message_count).for_each(|i| {
            let count = counts.get(&i.to_string());
            if counts.get(&i.to_string()) != Some(&1) {
                println!(
                    "Expected {} count to be 1, found {}",
                    i,
                    count.cloned().unwrap_or(0)
                );
                panic!("{:?}", counts);
            }
        });
        let mut redelivery_start = None;

        let timeout = Instant::now() + resend_delay + Duration::from_secs(4);
        while read_count < 2 * message_count {
            if let Ok(data) = rx.try_recv() {
                if redelivery_start.is_none() {
                    redelivery_start = Some(Instant::now());
                }
                read_count += 1;
                *counts.entry(data).or_insert(0) += 1;
            } else {
                std::thread::sleep(Duration::from_millis(10));
            }
            if Instant::now() > timeout {
                println!("{:?}", counts);
                panic!("timed out waiting for messages to be read");
            }
        }
        let redelivery_start = redelivery_start.unwrap();
        let expected_redelivery_start = start + resend_delay;
        println!(
            "start: 0ms, delay: {}ms, redelivery_start: {}ms, expected_redelivery: {}ms",
            resend_delay.as_millis(),
            (redelivery_start - start).as_millis(),
            (expected_redelivery_start - start).as_millis()
        );
        assert!(redelivery_start > expected_redelivery_start - Duration::from_secs(1));
        assert!(redelivery_start < expected_redelivery_start + Duration::from_secs(1));
        (0u8..message_count).for_each(|i| {
            let count = counts.get(&i.to_string());
            if count != Some(&2) {
                println!(
                    "Expected {} count to be 2, found {}",
                    i,
                    count.cloned().unwrap_or(0)
                );
                panic!("{:?}", counts);
            }
        });
    }
}
