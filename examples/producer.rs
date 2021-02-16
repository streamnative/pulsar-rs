#[macro_use]
extern crate serde;
use pulsar::{
    message::proto, producer, Authentication, Error as PulsarError, Pulsar, SerializeMessage,
    TokioExecutor,
};
use std::env;

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

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();

    let addr = env::var("PULSAR_ADDRESS")
        .ok()
        .unwrap_or_else(|| "pulsar://127.0.0.1:6650".to_string());
    let topic = env::var("PULSAR_TOPIC")
        .ok()
        .unwrap_or_else(|| "non-persistent://public/default/test".to_string());

    let mut builder = Pulsar::builder(addr, TokioExecutor);

    if let Ok(token) = env::var("PULSAR_TOKEN") {
        let authentication = Authentication {
            name: "token".to_string(),
            data: token.into_bytes(),
        };

        builder = builder.with_auth(authentication);
    }

    let pulsar: Pulsar<_> = builder.build().await?;
    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .with_name("my producer")
        .with_options(producer::ProducerOptions {
            schema: Some(proto::Schema {
                r#type: proto::schema::Type::String as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .build()
        .await?;

    let mut counter = 0usize;
    loop {
        producer
            .send(TestData {
                data: "data".to_string(),
            })
            .await?
            .await
            .unwrap();

        counter += 1;
        println!("{} messages", counter);
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
    }
}
