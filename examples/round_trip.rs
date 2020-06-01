#[macro_use]
extern crate serde;
use pulsar::{Consumer, Pulsar, TokioExecutor,
  message::Payload, SerializeMessage, DeserializeMessage, Error as PulsarError,
  message::proto::command_subscribe::SubType, producer,
  message::proto};
use futures::TryStreamExt;

#[derive(Serialize, Deserialize)]
struct TestData {
    data: String,
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

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();

    let addr = "pulsar://127.0.0.1:6650";
    let pulsar: Pulsar<TokioExecutor> = Pulsar::new(addr, None, None).await?;
    let mut producer = pulsar.create_producer("test", Some("my-producer".to_string()), producer::ProducerOptions {
        schema: Some(proto::Schema {
            type_: proto::schema::Type::String as i32,
            ..Default::default()
        }),
        ..Default::default()
    }).await?;

    tokio::task::spawn(async move {
        let mut counter = 0usize;
        loop {
            producer.send(
                TestData {
                    data: "data".to_string(),
                },
                ).await.unwrap();
            counter += 1;
            if counter %1000 == 0 {
                println!("sent {} messages", counter);
            }
        }
    });

    let pulsar2: Pulsar<TokioExecutor> = Pulsar::new(addr, None, None).await?;

    let mut consumer: Consumer<TestData> = pulsar2
        .consumer()
        .with_topic("test")
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
        counter += 1;
        if counter %1000 == 0 {
            println!("received {} messages", counter);
        }
    }


    Ok(())
}
