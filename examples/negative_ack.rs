//! Demonstrates negative acknowledgment redelivery delay and backoff configuration.
//!
//! Caveats:
//! - Message-based `nack()` uses the broker-supplied `redelivery_count` as the backoff input.
//! - `nack_with_id()` always uses fixed-delay behavior because message IDs do not carry
//!   `redelivery_count`.
//! - Client-side timers are best-effort and in-memory; they do not guarantee exact redelivery timing.
//! - Closing or crashing the consumer drops pending nack schedules, so redelivery then follows
//!   broker-side behavior.
//! - DLQ and retry-letter-topic routing behavior is not redesigned by this feature; existing DLQ
//!   settings continue to apply independently.

#[macro_use]
extern crate serde;
use std::{env, time::Duration};

use futures::TryStreamExt;
use pulsar::{
    Consumer, DeserializeMessage, MultiplierRedeliveryBackoff, NegativeAckBackoff, Payload, Pulsar,
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

#[derive(Debug)]
struct LinearBackoff(Duration);

impl NegativeAckBackoff for LinearBackoff {
    fn next(&self, redelivery_count: u32) -> Duration {
        self.0 * redelivery_count.saturating_add(1)
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

    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
    let _built_in_backoff_example = MultiplierRedeliveryBackoff::default();
    let _custom_backoff_example = LinearBackoff(Duration::from_secs(10));

    // HOW TO USE THIS EXAMPLE
    // Section 1 (default delay) is the active configuration and runs by default.
    // To try a different configuration:
    //   1. Comment out the Section 1 consumer builder block below.
    //   2. Uncomment exactly ONE of Sections 2-4.
    // Running multiple sections simultaneously is not supported; each section
    // creates its own consumer on the same subscription.

    // Section 1: Default 60s fixed delay.
    // As of this release, negative acknowledgments default to a 60 seconds fixed delay
    // before redelivery. Previously, the Rust client requested immediate redelivery.
    // To spell out that same fixed delay explicitly, add:
    // .with_nack_redelivery_delay(Duration::from_secs(60))
    let mut consumer: Consumer<TestData, _> = pulsar
        .consumer()
        .with_topic(topic)
        .with_consumer_name("negative_ack_default_delay")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("negative_ack_subscription")
        .build()
        .await?;

    // Section 2: Duration::ZERO migration escape hatch.
    // Restores pre-release immediate-redelivery behavior for consumers that cannot tolerate the
    // 60s delay.
    // Duration::ZERO is the pre-v* immediate-redelivery equivalent.
    //
    // let mut consumer: Consumer<TestData, _> = pulsar
    //     .consumer()
    //     .with_topic(topic)
    //     .with_consumer_name("negative_ack_zero_delay")
    //     .with_subscription_type(SubType::Exclusive)
    //     .with_subscription("negative_ack_subscription")
    //     .with_nack_redelivery_delay(Duration::ZERO)
    //     .build()
    //     .await?;

    // Section 3: Built-in MultiplierRedeliveryBackoff.
    // The default policy uses broker redelivery count with a 1s minimum and 10min maximum delay.
    //
    // let mut consumer: Consumer<TestData, _> = pulsar
    //     .consumer()
    //     .with_topic(topic)
    //     .with_consumer_name("negative_ack_multiplier_backoff")
    //     .with_subscription_type(SubType::Exclusive)
    //     .with_subscription("negative_ack_subscription")
    //     .with_negative_ack_backoff(MultiplierRedeliveryBackoff::default())
    //     .build()
    //     .await?;

    // Section 4: Custom NegativeAckBackoff policy.
    // Implement the trait when you need application-specific redelivery spacing.
    //
    // let mut consumer: Consumer<TestData, _> = pulsar
    //     .consumer()
    //     .with_topic(topic)
    //     .with_consumer_name("negative_ack_linear_backoff")
    //     .with_subscription_type(SubType::Exclusive)
    //     .with_subscription("negative_ack_subscription")
    //     .with_negative_ack_backoff(LinearBackoff(Duration::from_secs(10)))
    //     .build()
    //     .await?;

    let mut counter = 0usize;
    while let Some(msg) = consumer.try_next().await? {
        log::info!("metadata: {:?}", msg.metadata());
        log::info!("id: {:?}", msg.message_id());
        log::info!("redelivery_count: {}", msg.redelivery_count());

        match msg.deserialize() {
            Ok(data) => log::info!("payload data: {}", data.data),
            Err(e) => log::error!("could not deserialize message: {:?}", e),
        }

        consumer.nack(&msg).await?;

        // Section 5: nack_with_id note.
        // consumer.nack_with_id(&msg.topic, msg.message_id().clone()).await? is also supported
        // but always uses fixed delay only, because message IDs do not carry redelivery count.

        counter += 1;
        if counter > 10 {
            consumer.close().await?;
            break;
        }
    }

    Ok(())
}
