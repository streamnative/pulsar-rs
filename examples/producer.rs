#[macro_use]
extern crate serde;
use pulsar::{
    message::proto, producer, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
};

#[derive(Serialize, Deserialize)]
struct TestData {
    data: String,
}

impl SerializeMessage for TestData {
    fn serialize_message(input: &Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();

    let addr = "pulsar://127.0.0.1:6650";
    let pulsar: Pulsar<TokioExecutor> = Pulsar::builder(addr).build().await?;
    let mut producer = pulsar
        .producer()
        .with_topic("non-persistent://public/default/test")
        .with_name("my producer")
        .with_options(producer::ProducerOptions {
            schema: Some(proto::Schema {
                type_: proto::schema::Type::String as i32,
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
            .await?;

        counter += 1;
        println!("{} messages", counter);
        tokio::time::delay_for(std::time::Duration::from_millis(2000)).await;
    }
}
