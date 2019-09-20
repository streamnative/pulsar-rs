## Pulsar
#### Future-based Rust bindings for [Apache Pulsar](https://pulsar.apache.org/)

[Documentation](https://docs.rs/pulsar)

Current status: Simple functionality, but expect API to change. Major API changes will come simultaneous with async-await stability, so look for that.
### Getting Started
Cargo.toml
```toml
futures = "0.1.23"
pulsar = "0.3.0"
tokio = "0.1.11"
```
#### Producing
```rust
use pulsar::{Pulsar, SerializeMessage, ProducerError, producer};
use tokio::prelude::*;

#[derive(Debug)]
pub struct SomeData {

}

impl SerializeMessage for SomeData {
    fn serialize_message(input: &Self) -> Result<producer::Message, ProducerError> {
        unimplemented!()
    }
}

fn run() {
    let addr = "127.0.0.1:6650".parse().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let pulsar: Pulsar = Pulsar::new(addr, None, runtime.executor())
        .wait().unwrap();

    let producer = pulsar.producer();

    let message = SomeData {};

    runtime.executor().spawn({
        producer.send(&message, "some_topic")
            .map(drop)
            .map_err(|e| eprintln!("Error handling! {}", e))
    });
}
```
#### Consuming
```rust
use pulsar::{Pulsar, Consumer, SubType, DeserializeMessage, consumer, message};
use tokio::prelude::*;

#[derive(Debug)]
pub struct SomeData {

}

impl DeserializeMessage for SomeData {
    type Output = Result<Self, ()>;

    fn deserialize_message(payload: message::Payload) -> Self::Output {
        unimplemented!()
    }
}

fn run() {
    let addr = "127.0.0.1:6650".parse().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let pulsar: Pulsar = Pulsar::new(addr, None, runtime.executor())
        .wait().unwrap();

    let consumer: Consumer<SomeData> = pulsar.consumer()
        .with_topic("some_topic")
        .with_consumer_name("some_consumer_name")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("some_subscription")
        .build()
        .wait()
        .unwrap();

    runtime.executor().spawn({
        consumer
            .for_each(move |consumer::Message { payload, ack, .. }| {
                ack.ack(); // or maybe not ack unless Ok - whatever makes sense in your use case
                match payload {
                    Ok(data) => {
                        //process data
                    },
                    Err(e) => {
                        // handle error
                    }
                }
                Ok(()) // or Err if you want the consumer to shutdown
            })
            .map_err(|_| { /* handle connection errors, etc */ })
    })
}
```

### License
This library is licensed under the terms of both the MIT license and the Apache License (Version 2.0), and may include packages written by third parties which carry their own copyright notices and license terms.

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT), and
[COPYRIGHT](COPYRIGHT) for details.
