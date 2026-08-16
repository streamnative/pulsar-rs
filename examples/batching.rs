#![recursion_limit = "256"]
#[macro_use]
extern crate serde;

use futures::{future::join_all, TryStreamExt};
use pulsar::{
    compression::*,
    message::{proto::command_subscribe::SubType, Payload},
    producer, Consumer, DeserializeMessage, Error as PulsarError, Pulsar, SerializeMessage,
    TokioExecutor,
};

#[derive(Debug, Serialize, Deserialize)]
struct TestData {
    data: String,
}

impl SerializeMessage for TestData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
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

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();

    let addr = "pulsar://127.0.0.1:6650";
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
    let mut producer = pulsar
        .producer()
        .with_topic("test-batch-compression-snappy")
        .with_name("my-producer2".to_string())
        .with_options(producer::ProducerOptions {
            batch_size: Some(4),
            // compression: Some(Compression::Lz4(CompressionLz4::default())),
            // compression: Some(Compression::Zlib(CompressionZlib::default())),
            // compression: Some(Compression::Zstd(CompressionZstd::default())),
            compression: Some(Compression::Snappy(CompressionSnappy::default())),
            ..Default::default()
        })
        .build()
        .await?;

    producer
        .check_connection()
        .await
        .map(|_| println!("connection ok"))?;

    tokio::task::spawn(async move {
        let mut counter = 0usize;
        let mut v = Vec::new();
        loop {
            println!("will send");
            let receipt_rx = producer
                .send_non_blocking(TestData {
                    data: "data".to_string(),
                })
                .await
                .unwrap();
            v.push(receipt_rx);
            println!("sent");
            counter += 1;
            if counter % 4 == 0 {
                //producer.send_batch().await.unwrap();
                println!("sent {counter} messages");
                break;
            }
        }

        println!("receipts: {:?}", join_all(v).await);
    });

    let mut consumer: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic("test-batch-compression-snappy")
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("test_subscription")
        .build()
        .await?;

    let mut counter = 0usize;
    while let Some(msg) = consumer.try_next().await? {
        consumer.ack(&msg).await?;
        let data = msg.deserialize().unwrap();
        if data.data.as_str() != "data" {
            panic!("Unexpected payload: {}", &data.data);
        }
        println!("got message: {data:?}");
        counter += 1;
        if counter % 4 == 0 {
            println!("sent {counter} messages");
            break;
        }
    }

    Ok(())
}
