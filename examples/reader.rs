#[macro_use]
extern crate serde;
use futures::TryStreamExt;
use pulsar::{
    consumer::ConsumerOptions, proto::Schema, reader::Reader, Authentication, DeserializeMessage,
    Payload, Pulsar, TokioExecutor,
};
use std::env;

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

    let mut reader: Reader<TestData, _> = pulsar
        .reader()
        .with_topic(topic)
        .with_consumer_name("test_reader")
        .with_options(ConsumerOptions::default().with_schema(Schema {
            r#type: pulsar::proto::schema::Type::String as i32,
            ..Default::default()
        }))
        // subscription defaults to SubType::Exclusive
        .into_reader()
        .await?;
    // log::info!("created a reader");

    let mut counter = 0usize;

    // listen to 5 messages
    while let Some(msg) = reader.try_next().await? {
        log::info!("metadata: {:#?}", msg.metadata());

        log::info!("id: {:?}", msg.message_id());
        let data = match msg.deserialize() {
            Ok(data) => data,
            Err(e) => {
                log::error!("Could not deserialize message: {:?}", e);
                break;
            }
        };

        if data.data.as_str() != "data" {
            log::error!("Unexpected payload: {}", &data.data);
            break;
        }
        counter += 1;

        if counter > 5 {
            break;
        }
        log::info!("got {} messages", counter);
    }

    Ok(())
}
