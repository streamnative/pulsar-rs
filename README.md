## Pulsar
#### Future-based Rust bindings for [Apache Pulsar](https://pulsar.apache.org/)

[Documentation](https://docs.rs/pulsar)

Current status: Rough pass of simplest functionality. Should be in a good place to add additional features, but nothing but the barest of basics is implemented yet.
### Getting Started
Cargo.toml
```
futures = "0.1.23"
pulsar = "0.1.0"
tokio = "0.1.11"

# If you want connection pooling
r2d2 = "0.8.2"
r2d2_pulsar = "0.1.0"

# If you want to use serde
serde = "1.0.80"
serde_derive = "1.0"
serde_json = "1.0.32"
```
main.rs
```rust
extern crate pulsar;

// if you want connection pooling
extern crate r2d2_pulsar;
extern crate r2d2;

// if you want serde
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
```
#### Producing
```rust
use tokio::runtime::Runtime;
use futures::future;
use pulsar::Producer;

pub struct SomeData {
    ...
}

fn serialize(data: &SomeData) -> Vec<u8> {
    ...
}

fn main() {
    let pulsar_addr = "...";
    let producer_name = "some_producer_name";
    let runtime = Runtime::new().unwrap();

    let producer = Producer::new(pulsar_addr, producer_name, runtime.executor())
       .wait()
       .unwrap();

    let data = SomeData { ... };
    let serialized = serialize(&data);
    let send_1 = producer.send_raw("some_topic", serialized);
    let send_2 = producer.send_raw("some_topic", serialized);
    let send_3 = producer.send_raw("some_topic", serialized);

    future::join_all(vec![send_1, send_2, send_3]).wait().unwrap();
    runtime.shutdown_now().wait().unwrap();
}

```
#### Consuming
```rust
use tokio::runtime::Runtime;
use futures::{Stream, future};
use pulsar::{Ack, Consumer, Error, SubType};

pub struct SomeData {
    ...
}

fn deserialize(data: &[u8]) -> Result<SomeData, Error> {
    ...
}

fn main() {
    let pulsar_addr = "...";
    let runtime = Runtime::new().unwrap();

    let consumer = ConsumerBuilder::new(pulsar_addr, runtime.executor())
        .with_topic("some_topic")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("some_subscription_name")
        .with_deserializer(|payload| deserialize(&payload.data))
        .build()
        .wait()
        .unwrap();

    let consumption_result = consumer
        .for_each(|msg: Result<(SomeData, Ack), Error>| match msg {
            Ok((ack, data)) => {
                // process data
                ack.ack();
                Ok(())
            },
            Err(e) => {
                println!("Got an error: {}", e);
                Ok(())
                // return Err(_) to instead shutdown consumer
            }
        })
        .wait();

    // handle error, reconnect, etc

    runtime.shutdown_now().wait().unwrap();
}
```
### Connection Pooling
```rust
use r2d2_pulsar::ProducerConnectionManager;

let addr = "127.0.0.1:6650";
let runtime = Runtime::new().unwrap();

let pool = r2d2::Pool::new(ProducerConnectionManager::new(
    addr,
    "r2d2_test_producer",
    runtime.executor()
)).unwrap();

let mut a = pool.get().unwrap();
let mut b = pool.get().unwrap();

let data1: Vec<u8> = ...;
let data2: Vec<u8> = ...;

let send_1 = a.send_raw("some_topic", data1);
let send_2 = b.send_raw("some_topic", data2);

send_1.join(send_2).wait().unwrap();

runtime.shutdown_now().wait().unwrap();
```
### Serde
```rust
#[derive(Debug, Serialize, Deserialize)
struct SomeData {
    ...
}

fn process_data(data: Result<SomeData, Error>) -> Result<(), Error> {
    ...
}

fn main() {
    let pulsar_addr = "...";
    let runtime = Runtime::new().unwrap();

    let producer = Producer::new(pulsar_addr, "some_producer_name", runtime.executor())
        .wait()
        .unwrap();

    let consumer = ConsumerBuilder::new(pulsar_addr, runtime.executor())
        .with_topic("some_topic")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("some_subscription_name")
        .build()
        .wait()
        .unwrap();

    let send_1 = producer.send_json(&SomeData { ... }, serialized);
    let send_2 = producer.send_json(&SomeData { ... }, serialized);
    let send_3 = producer.send_json(&SomeData { ... }, serialized);

    future::join_all(vec![send_1, send_2, send_3]).wait().unwrap();

    let consumption_result = consumer
        .for_each(|msg| process_data(msg))
        .wait();

    runtime.shutdown_now().wait().unwrap();
}

```

