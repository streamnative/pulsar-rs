#[macro_use]
extern crate serde;
use futures::TryStreamExt;
use pulsar::{Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor};

#[derive(Serialize, Deserialize)]
struct TestData {
    data: String,
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

    let mut consumer: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic("non-persistent://public/default/test")
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("test_subscription")
        .build()
        .await?;

    let mut counter = 0usize;
    while let Some(msg) = consumer.try_next().await? {
        consumer.ack(&msg).await?;
        log::info!("metadata: {:?}", msg.metadata());
        let data = match msg.deserialize() {
            Ok(data) => data,
            Err(e) => {
                log::error!("could not deserialize message: {:?}", e);
                break;
            }
        };

        if data.data.as_str() != "data" {
            log::error!("Unexpected payload: {}", &data.data);
            break;
        }
        counter += 1;
        log::info!("got {} messages", counter);
    }

    Ok(())
}
