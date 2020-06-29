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

pub use client::{DeserializeMessage, Pulsar, SerializeMessage, PulsarBuilder};
pub use connection::Authentication;
pub use connection_manager::{BackOffOptions, TlsOptions, BrokerAddress};
pub use consumer::{
    Consumer, ConsumerBuilder, ConsumerOptions, ConsumerState, MultiTopicConsumer,
};
pub use error::Error;
pub use executor::Executor;
#[cfg(feature = "tokio-runtime")]
pub use executor::TokioExecutor;
#[cfg(feature = "async-std-runtime")]
pub use executor::AsyncStdExecutor;
pub use message::proto;
pub use message::proto::command_subscribe::SubType;
pub use producer::{Producer, ProducerOptions, MultiTopicProducer};

mod client;
mod connection;
mod connection_manager;
pub mod error;
mod executor;
pub mod consumer;
pub mod message;
pub mod producer;
mod service_discovery;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use futures::{StreamExt, TryStreamExt};
    #[cfg(feature = "tokio-runtime")]
    use tokio;
    #[cfg(feature = "tokio-runtime")]
    use tokio::runtime::Runtime;

    use message::proto::command_subscribe::SubType;

    use crate::client::SerializeMessage;
    use crate::message::Payload;
    use crate::Error as PulsarError;
    #[cfg(feature = "tokio-runtime")]
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
    #[cfg(feature = "tokio-runtime")]
    fn round_trip() {
        let mut runtime = Runtime::new().unwrap();
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);

        let f = async {
            let addr = "pulsar://127.0.0.1:6650";
            let pulsar: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await.unwrap();

            let mut consumer: Consumer<TestData> = pulsar
                .consumer()
                .with_topic("test")
                .with_consumer_name("test_consumer")
                .with_subscription_type(SubType::Exclusive)
                .with_subscription("test_subscription")
                .build()
                .await
                .unwrap();

            info!("consumer created");

            let mut producer = pulsar.producer()
                .with_topic("test")
                .build()
                .await.unwrap();
            info!("producer created");

            info!("will send message");
            for _ in 0u16..5000 {
                producer.send(
                    TestData {
                        data: "data".to_string(),
                    },
                ).await.unwrap();
            }

            info!("sent");
            //let mut stream = consumer.take(5000);
            let mut count = 0usize;
            while let Ok(Some(msg)) = consumer.try_next().await {
            //let res =  consumer.next().await.unwrap();
                //let msg = res.unwrap();
                consumer.ack(&msg).await;
                let data = msg.deserialize().unwrap();
                if data.data.as_str() != "data" {
                    panic!("Unexpected payload: {}", &data.data);
                }

                count +=1;

                if count % 500 == 0 {
                    info!("messages received: {}", count);
                }
                if count == 5000 {
                    break;
                }
            }
            // FIXME .timeout(Duration::from_secs(5))
        };

        runtime.block_on(Box::pin(f));
    }

    #[test]
    #[cfg(feature = "tokio-runtime")]
    fn unsized_data() {
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let mut runtime = Runtime::new().unwrap();

        let f = async {
            let addr = "pulsar://127.0.0.1:6650";
            let pulsar: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await.unwrap();
            let mut producer = pulsar.create_multi_topic_producer(None);

            // test &str
            {
                let topic = "test_unsized_data_str";
                let send_data = "some unsized data";

                let mut consumer = pulsar
                    .consumer()
                    .with_topic(topic)
                    .with_subscription_type(SubType::Exclusive)
                    .with_subscription("test_subscription")
                    .build::<String>()
                    .await
                    .unwrap();

                producer.send(topic, send_data.to_string()).await.unwrap();

                let msg = consumer.next().await.unwrap().unwrap();
                consumer.ack(&msg).await;

                let data = msg.deserialize().unwrap();
                if data.as_str() != send_data {
                    panic!("Unexpected payload in &str test: {}", &data);
                }
                //FIXME .timeout(Duration::from_secs(1))
            }

            // test &[u8]
            {
                let topic = "test_unsized_data_bytes";
                let send_data: &[u8] = &[0, 1, 2, 3];

                let mut consumer = pulsar
                    .consumer()
                    .with_topic(topic)
                    .with_subscription_type(SubType::Exclusive)
                    .with_subscription("test_subscription")
                    .build::<Vec<u8>>()
                    .await
                    .unwrap();

                producer.send(topic, send_data.to_vec()).await.unwrap();

                let msg = consumer.next().await.unwrap().unwrap();
                consumer.ack(&msg).await;
                let data = msg.deserialize();
                if data.as_slice() != send_data {
                    panic!("Unexpected payload in &[u8] test: {:?}", &data);
                }
            }
        };

        runtime.block_on(Box::pin(f));
    }

    #[test]
    #[ignore]
    #[cfg(feature = "tokio-runtime")]
    fn redelivery() {
        let _ = log::set_logger(&TEST_LOGGER);
        let _ = log::set_max_level(LevelFilter::Debug);
        let runtime = Runtime::new().unwrap();

        let addr = "pulsar://127.0.0.1:6650";
        let (tx, rx) = std::sync::mpsc::channel();

        let mut seen = BTreeSet::new();
        let start = Instant::now();
        let resend_delay = Duration::from_secs(4);

        let message_count = 10;
        let f = async move {
            let pulsar: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await.unwrap();

            let topic: String = std::iter::repeat(())
                .map(|()| rand::thread_rng().sample(Alphanumeric))
                .take(7)
                .collect();
            let mut producer = pulsar.create_producer(&topic, None, ProducerOptions::default()).await.unwrap();

            let mut consumer: Consumer<TestData> = pulsar
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
                    TestData {
                        data: i.to_string(),
                    },
                    ).await.unwrap();
            }

            let mut count = 0usize;
            while let Some(res) = consumer.next().await {

                let message = res.unwrap();
                let data = message.deserialize().unwrap();
                tx.send(data.data.clone()).unwrap();
                if !seen.contains(&data.data) {
                    seen.insert(data.data.clone());
                } else {
                    //ack the second time around
                    consumer.ack(&message).await;
                }

                count += 1;
                if count == message_count as usize * 2 {
                    break;
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
