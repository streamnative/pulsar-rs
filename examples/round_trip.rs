#[macro_use]
extern crate serde;
use log::{Record, Metadata, LevelFilter};
use pulsar::{Consumer, Pulsar, TokioExecutor,
  message::Payload, SerializeMessage, DeserializeMessage, Error as PulsarError,
  message::proto::command_subscribe::SubType, producer,
  message::proto};
use tokio::runtime::Runtime;
use futures::StreamExt;

#[derive(Serialize, Deserialize)]
struct TestData {
    data: String,
}

impl SerializeMessage for TestData {
    fn serialize_message(input: &Self) -> Result<producer::Message, PulsarError> {
        let payload =
            serde_json::to_vec(input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl DeserializeMessage for TestData {
    type Output = Result<TestData, serde_json::Error>;

    fn deserialize_message(payload: Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

pub struct SimpleLogger;
impl log::Log for SimpleLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        //metadata.level() <= Level::Info
        true
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} {}\t{}\t{}", chrono::Utc::now(),
            record.level(), record.module_path().unwrap(), record.args());
        }
    }
    fn flush(&self) {
        use std::io::Write;
        std::io::stdout().flush().unwrap();
    }
}

pub static TEST_LOGGER: SimpleLogger = SimpleLogger;

fn main() {
    let mut runtime = Runtime::new().unwrap();
    let _ = log::set_logger(&TEST_LOGGER);
    let _ = log::set_max_level(LevelFilter::Debug);

    let producer = async {
        let addr = "localhost";
        let pulsar: Pulsar<TokioExecutor> = Pulsar::new(addr, None).await.unwrap();
        let producer = pulsar.create_producer("test", Some("my-producer".to_string()), producer::ProducerOptions {
            schema: Some(proto::Schema {
                      type_: proto::schema::Type::String as i32,
                      ..Default::default()
                    }),
            ..Default::default()
        }).await.unwrap();

        let mut counter = 0usize;
        loop {
            producer.send(
                TestData {
                    data: "data".to_string(),
                },
                ).await.unwrap();
            counter += 1;
            if counter %1000 == 0 {
                println!("sent {} messages", counter);
            }
        }
    };
    runtime.spawn(Box::pin(producer));

    let consumer = async {
        let addr = "localhost";
        let pulsar: Pulsar<TokioExecutor> = Pulsar::new(addr, None).await.unwrap();

        let mut consumer: Consumer<TestData> = pulsar
            .consumer()
            .with_topic("test")
            .with_consumer_name("test_consumer")
            .with_subscription_type(SubType::Exclusive)
            .with_subscription("test_subscription")
            .build()
            .await
            .unwrap();

        let mut counter = 0usize;
        while let Some(res) = consumer.next().await {
            let msg = res.unwrap();
            consumer.ack(&msg);
            let data = msg.payload.unwrap();
            /*let Message { payload, ack, .. } = res.unwrap();
            ack.ack();
            let data = payload.unwrap();
            */
            if data.data.as_str() != "data" {
                panic!("Unexpected payload: {}", &data.data);
            }
            counter += 1;
            if counter %1000 == 0 {
                println!("received {} messages", counter);
            }
        }
        // FIXME .timeout(Duration::from_secs(5))
    };

    runtime.block_on(Box::pin(consumer));
}
