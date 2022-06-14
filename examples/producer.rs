#[macro_use]
extern crate serde;
use pulsar::authentication::oauth2::OAuth2Authentication;
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
        .unwrap_or_else(|| "pulsar://localhost:6650".to_string());
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
    } else if let Ok(oauth2_cfg) = env::var("PULSAR_OAUTH2") {
        builder = builder.with_auth_provider(OAuth2Authentication::client_credentials(
            serde_json::from_str(oauth2_cfg.as_str())
                .expect(format!("invalid oauth2 config [{}]", oauth2_cfg.as_str()).as_str()),
        ));
    }

    let pulsar: Pulsar<_> = builder.build().await?;

    let pulsar_clone = pulsar.clone();
    let topic_clone = topic.clone();
    tokio::spawn(async move {
        let mut producer = pulsar_clone
            .producer()
            .with_topic(topic_clone)
            .with_name("my producer")
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                access_mode: Some(2),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        println!("latter dropped")
    });

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut producer = pulsar
        .producer()
        .with_topic(topic)
        .with_name("my producer")
        .with_options(producer::ProducerOptions {
            schema: Some(proto::Schema {
                r#type: proto::schema::Type::String as i32,
                ..Default::default()
            }),
            access_mode: Some(2),
            ..Default::default()
        })
        .build()
        .await?;

    // let mut counter = 0usize;
    // loop {
    //     producer
    //         .send(TestData {
    //             data: "data".to_string(),
    //         })
    //         .await?
    //         .await
    //         .unwrap();

    //     counter += 1;
    //     println!("{} messages", counter);
    //     tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    // }

    //if access mode is waitforexclusive, then producer should wait for the lock
    //when the lock is achieved, then it should receive commandproducersuccessmessage again parse that
    tokio::time::sleep(std::time::Duration::from_secs(2000)).await;

    Ok(())
}
