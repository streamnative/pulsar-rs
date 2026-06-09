#[macro_use]
extern crate serde;
use std::{env, time::Duration};

use futures::TryStreamExt;
use pulsar::{
    authentication::{basic::BasicAuthentication, oauth2::OAuth2Authentication},
    Authentication, Consumer, DeserializeMessage, MultiplierRedeliveryBackoff, Payload, Pulsar,
    SubType, TokioExecutor,
};

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
    } else if let Ok(oauth2_cfg) = env::var("PULSAR_OAUTH2") {
        builder = builder.with_auth_provider(OAuth2Authentication::client_credentials(
            serde_json::from_str(oauth2_cfg.as_str())
                .unwrap_or_else(|_| panic!("invalid oauth2 config [{}]", oauth2_cfg.as_str())),
        ));
    } else if let (Ok(username), Ok(password)) = (
        env::var("PULSAR_BASIC_USERNAME"),
        env::var("PULSAR_BASIC_PASSWORD"),
    ) {
        builder = builder.with_auth_provider(BasicAuthentication::new(&username, &password))
    }

    let pulsar: Pulsar<_> = builder.build().await?;
    let negative_ack_backoff = MultiplierRedeliveryBackoff::builder()
        .min_delay(Duration::from_secs(1))
        .max_delay(Duration::from_secs(30))
        .multiplier(2.0)
        .build()?;

    let mut consumer: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_consumer_name("negative_ack_consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("negative_ack_subscription")
        .with_nack_redelivery_delay(Duration::from_secs(5))
        .with_negative_ack_backoff(negative_ack_backoff)
        .build()
        .await?;

    let mut counter = 0usize;
    while let Some(msg) = consumer.try_next().await? {
        log::info!("metadata: {:?}", msg.metadata());
        log::info!("id: {:?}", msg.message_id());
        log::info!("redelivery_count: {}", msg.redelivery_count());

        let data = match msg.deserialize() {
            Ok(data) => data,
            Err(e) => {
                log::error!("could not deserialize message: {:?}", e);
                consumer.nack(&msg).await?;
                break;
            }
        };

        log::info!("payload data: {}", data.data);
        consumer.nack(&msg).await?;

        counter += 1;
        log::info!("nacked {} messages", counter);

        if counter > 10 {
            consumer.close().await.expect("Unable to close consumer");
            break;
        }
    }

    Ok(())
}
