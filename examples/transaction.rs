#[macro_use]
extern crate serde;
use std::{env, time::Duration};

use futures::TryStreamExt;
use pulsar::{
    authentication::oauth2::OAuth2Authentication, message::proto, producer, Authentication,
    Consumer, DeserializeMessage, Error as PulsarError, Payload, Pulsar, SerializeMessage,
    TokioExecutor,
};

#[derive(Serialize, Deserialize, Debug)]
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

    let addr = env::var("PULSAR_ADDRESS")
        .ok()
        .unwrap_or_else(|| "pulsar://127.0.0.1:6650".to_string());

    let mut builder = Pulsar::builder(addr, TokioExecutor).with_transactions();

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
    }

    let pulsar: Pulsar<_> = builder.build().await?;

    let input_topic = "persistent://public/default/test-input-topic";
    let output_topic_one = "persistent://public/default/test-output-topic-1";
    let output_topic_two = "persistent://public/default/test-output-topic-2";

    let producer_builder = pulsar.producer().with_options(producer::ProducerOptions {
        schema: Some(proto::Schema {
            r#type: proto::schema::Type::String as i32,
            ..Default::default()
        }),
        ..Default::default()
    });

    let mut input_producer = producer_builder
        .clone()
        .with_topic(input_topic)
        .build()
        .await?;

    let mut output_producer_one = producer_builder
        .clone()
        .with_topic(output_topic_one)
        .build()
        .await?;

    let mut output_producer_two = producer_builder
        .clone()
        .with_topic(output_topic_two)
        .build()
        .await?;

    let mut input_consumer: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic(input_topic)
        .with_subscription("test-input-subscription")
        .build()
        .await?;

    let mut output_consumer_one: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic(output_topic_one)
        .with_subscription("test-output-subscription-1")
        .build()
        .await?;

    let mut output_consumer_two: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic(output_topic_two)
        .with_subscription("test-output-subscription-2")
        .build()
        .await?;

    let count = 2;

    for i in 0..count {
        input_producer
            .send_non_blocking(TestData {
                data: format!("Hello Pulsar! count : {}", i),
            })
            .await?
            .await?;
    }

    for i in 0..count {
        let msg = input_consumer
            .try_next()
            .await?
            .expect("No message received");

        let txn = pulsar
            .new_txn()?
            .with_timeout(Duration::from_secs(10))
            .build()
            .await?;

        output_producer_one
            .create_message()
            .with_content(TestData {
                data: format!("Hello Pulsar! output_topic_one count : {}", i),
            })
            .with_txn(&txn)
            .send_non_blocking()
            .await?;

        output_producer_two
            .create_message()
            .with_content(TestData {
                data: format!("Hello Pulsar! output_topic_two count : {}", i),
            })
            .with_txn(&txn)
            .send_non_blocking()
            .await?;

        input_consumer.txn_ack(&msg, &txn).await?;

        if let Err(e) = txn.commit().await {
            match e {
                pulsar::Error::Transaction(pulsar::error::TransactionError::Conflict) => {
                    // If TransactionConflictException is not thrown,
                    // you need to redeliver or negativeAcknowledge this message,
                    // or else this message will not be received again.
                    input_consumer.nack(&msg).await?;
                }
                _ => (),
            }

            txn.abort().await?;

            return Err(e);
        }
    }

    for _ in 0..count {
        let msg = output_consumer_one
            .try_next()
            .await?
            .expect("No message received");

        println!("Received transaction message: {:?}", msg.deserialize());
    }

    for _ in 0..count {
        let msg = output_consumer_two
            .try_next()
            .await?
            .expect("No message received");

        println!("Received transaction message: {:?}", msg.deserialize());
    }

    Ok(())
}
