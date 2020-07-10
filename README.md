## Pulsar
#### Future-based Rust client for [Apache Pulsar](https://pulsar.apache.org/)

[Documentation](https://docs.rs/pulsar)

This is a pure Rust client for Apache Pulsar that does not depend on the
C++ Pulsar library. It provides an async/await based API, compatible with
[Tokio](https://tokio.rs/) and [async-std](https://async.rs/).

Features:
- URL based (`pulsar://` and `pulsar+ssl://`) connections with DNS lookup
- multi topic consumers (based on a regex or list)
- TLS connection
- configurable executor (Tokio or async-std)
- automatic reconnection with exponential back off
- message batching
- compression with LZ4, zlib, zstd or Snappy (can be deactivated with Cargo features)

### Getting Started
Cargo.toml
```toml
futures = "0.3"
pulsar = "1.0"
tokio = "0.2"
```
#### Producing
```rust
use serde::{Serialize, Deserialize};
use pulsar::{
    message::proto, producer, Error as PulsarError, Pulsar, SerializeMessage, TokioExecutor,
};

#[derive(Serialize, Deserialize)]
struct TestData {
    data: String,
}

impl<'a> SerializeMessage for &'a TestData {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
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
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;
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
```

#### Consuming
```rust
#[macro_use]
extern crate serde;
use futures::TryStreamExt;
use pulsar::{
    message::proto::command_subscribe::SubType, message::Payload, Consumer, DeserializeMessage,
    Pulsar, TokioExecutor,
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

    let addr = "pulsar://127.0.0.1:6650";
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;

    let mut consumer: Consumer<TestData, _> = pulsar
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
        let data = match msg.deserialize() {
            Ok(data) => data,
            Err(e) => {
                log::error!("could not deserialize message: {:?}", e);
                break;
            }
        };

        if data.data.as_str() != "data" {
            log::error!("Unexpected payload: {}", &data.data);
            break;
        }
        counter += 1;
        log::info!("got {} messages", counter);
    }

    Ok(())
}
```

### License
This library is licensed under the terms of both the MIT license and the Apache License (Version 2.0), and may include packages written by third parties which carry their own copyright notices and license terms.

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT), and
[COPYRIGHT](COPYRIGHT) for details.
