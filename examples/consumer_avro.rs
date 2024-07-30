#[macro_use]
extern crate serde;
use futures::TryStreamExt;
use pulsar::{
    Authentication, Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor,
};
use std::env;
use std::io::Cursor;
use apache_avro::{AvroSchema, from_avro_datum, from_value, Schema};
use pulsar::authentication::oauth2::{OAuth2Authentication};

#[derive(Serialize, Deserialize)]
struct TestData {
    number: i32,
    data: String
}
impl DeserializeMessage for TestData {
    type Output = Result<TestData, apache_avro::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        let data = &payload.data;
        let v = from_avro_datum(&TestData::get_schema(), &mut Cursor::new(data), None).unwrap();
        from_value(&v)
    }
}

impl AvroSchema for TestData {
    // You can get the schema through the pulsar-admin command.
    // Example: pulsar-admin schema get <topic-name>
    // Then paste the schema section here.
    fn get_schema() -> Schema {
        let raw_schema = r#"{
      "type": "record",
      "name": "TestData",
      "namespace": "org.example.RustConsumeExample",
      "fields": [
        {
          "name": "data",
          "type": [
            "null",
            "string"
          ]
        },
        {
          "name": "number",
          "type": "int"
        }
      ]
    }
        "#;
        Schema::parse_str(raw_schema).unwrap()
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
        .unwrap_or_else(|| "persistent://public/default/test".to_string());

    let mut builder = Pulsar::builder(addr, TokioExecutor);

    if let Ok(token) = env::var("PULSAR_TOKEN") {
        let authentication = Authentication {
            name: "token".to_string(),
            data: token.into_bytes(),
        };

        builder = builder.with_auth(authentication);
    } else if let Ok(oauth2_cfg) = env::var("PULSAR_OAUTH2") {
        builder = builder.with_auth_provider(OAuth2Authentication::client_credentials(
            serde_json::from_str(oauth2_cfg.as_str())
                .expect(format!("invalid oauth2 config [{}]", oauth2_cfg.as_str()).as_str())));
    }

    let pulsar: Pulsar<_> = builder.build().await?;

    let mut consumer: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("test_subscription")
        .build()
        .await?;

    let mut counter = 0usize;
    while let Some(msg) = consumer.try_next().await? {
        consumer.ack(&msg).await?;
        log::info!("metadata: {:?}", msg.metadata());
        log::info!("id: {:?}", msg.message_id());
        let data = match msg.deserialize() {
            Ok(data) => data,
            Err(e) => {
                log::error!("could not deserialize message: {:?}", e);
                break;
            }
        };

        println!("{} : {}", data.data, data.number);
        counter += 1;
        log::info!("got {} messages", counter);
    }

    Ok(())
}
