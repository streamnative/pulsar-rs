#![recursion_limit = "256"]
#[macro_use]
extern crate serde;
use futures::TryStreamExt;
use pulsar::{
    message::{proto, proto::command_subscribe::SubType, Payload},
    producer, Consumer, DeserializeMessage, Error as PulsarError, Pulsar, SerializeMessage,
    TokioExecutor,
};

#[derive(Serialize, Deserialize)]
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
        .with_topic("test")
        .with_name("my-producer")
        .with_options(producer::ProducerOptions {
            schema: Some(proto::Schema {
                r#type: proto::schema::Type::String as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .build()
        .await?;

    tokio::task::spawn(async move {
        let mut counter = 0usize;
        loop {
            producer
                .send(TestData {
                    data: "data".to_string(),
                })
                .await
                .unwrap()
                .await
                .unwrap();
            counter += 1;
            if counter % 1000 == 0 {
                println!("sent {counter} messages");
            }
        }
    });

    let pulsar2: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;

    let mut consumer: Consumer<TestData, _> = pulsar2
        .consumer()
        .with_topic("test")
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("test_subscription")
        .build()
        .await?;

    let mut counter = 0usize;
    while let Some(msg) = consumer.try_next().await? {
        log::info!("id: {:?}", msg.message_id());
        consumer.ack(&msg).await?;
        let data = msg.deserialize().unwrap();
        if data.data.as_str() != "data" {
            panic!("Unexpected payload: {}", &data.data);
        }
        counter += 1;
        if counter % 1000 == 0 {
            println!("received {counter} messages");
        }
    }

    Ok(())
}
