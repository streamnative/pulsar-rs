use std::pin::Pin;

use chrono::{DateTime, Utc};
use futures::{
    channel::mpsc::SendError,
    task::{Context, Poll},
    Future, SinkExt, Stream,
};
use url::Url;

use crate::{
    client::DeserializeMessage,
    consumer::{ConsumerOptions, DeadLetterPolicy, EngineMessage, Message, TopicConsumer},
    error::Error,
    executor::Executor,
    message::proto::{command_subscribe::SubType, MessageIdData},
};

/// A client that acknowledges messages systematically
pub struct Reader<T: DeserializeMessage, Exe: Executor> {
    pub(crate) consumer: TopicConsumer<T, Exe>,
    pub(crate) state: Option<State<T>>,
}

impl<T: DeserializeMessage + 'static, Exe: Executor> Unpin for Reader<T, Exe> {}

pub enum State<T: DeserializeMessage> {
    PollingConsumer,
    PollingAck(
        Message<T>,
        Pin<Box<dyn Future<Output = Result<(), SendError>> + Send + Sync>>,
    ),
}

impl<T: DeserializeMessage + 'static, Exe: Executor> Stream for Reader<T, Exe> {
    type Item = Result<Message<T>, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.state.take().unwrap() {
            State::PollingConsumer => match Pin::new(&mut this.consumer).poll_next(cx) {
                Poll::Pending => {
                    this.state = Some(State::PollingConsumer);
                    Poll::Pending
                }

                Poll::Ready(None) => {
                    this.state = Some(State::PollingConsumer);
                    Poll::Ready(None)
                }

                Poll::Ready(Some(Ok(msg))) => {
                    let mut acker = this.consumer.acker();
                    let message_id = msg.message_id().clone();
                    this.state = Some(State::PollingAck(
                        msg,
                        Box::pin(
                            async move { acker.send(EngineMessage::Ack(message_id, false)).await },
                        ),
                    ));
                    Pin::new(this).poll_next(cx)
                }

                Poll::Ready(Some(Err(e))) => {
                    this.state = Some(State::PollingConsumer);
                    Poll::Ready(Some(Err(e)))
                }
            },
            State::PollingAck(msg, mut ack_fut) => match ack_fut.as_mut().poll(cx) {
                Poll::Pending => {
                    this.state = Some(State::PollingAck(msg, ack_fut));
                    Poll::Pending
                }

                Poll::Ready(res) => {
                    this.state = Some(State::PollingConsumer);
                    Poll::Ready(Some(
                        res.map_err(|err| Error::Consumer(err.into())).map(|()| msg),
                    ))
                }
            },
        }
    }
}

impl<T: DeserializeMessage, Exe: Executor> Reader<T, Exe> {
    // this is totally useless as calling ConsumerBuilder::new(&pulsar_client)
    // does just the same
    /*
    /// creates a [ReaderBuilder] from a client instance
    pub fn builder(pulsar: &Pulsar<Exe>) -> ConsumerBuilder<Exe> {
        ConsumerBuilder::new(pulsar)
    }
    */

    /// test that the connections to the Pulsar brokers are still valid
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connection(&mut self) -> Result<(), Error> {
        self.consumer.check_connection().await
    }

    /// returns topic this reader is subscribed on
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topic(&self) -> String {
        self.consumer.topic()
    }

    /// returns a list of broker URLs this reader is connected to
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn connections(&mut self) -> Result<Url, Error> {
        Ok(self.consumer.connection().await?.url().clone())
    }

    /// returns the consumer's configuration options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn options(&self) -> &ConsumerOptions {
        &self.consumer.config.options
    }

    // is this necessary?
    /// returns the consumer's dead letter policy options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn dead_letter_policy(&self) -> Option<&DeadLetterPolicy> {
        self.consumer.dead_letter_policy.as_ref()
    }

    /// returns the readers's subscription name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn subscription(&self) -> &str {
        &self.consumer.config.subscription
    }

    /// returns the reader's subscription type
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn sub_type(&self) -> SubType {
        self.consumer.config.sub_type
    }

    /// returns the reader's batch size
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn batch_size(&self) -> Option<u32> {
        self.consumer.config.batch_size
    }

    /// returns the reader's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn reader_name(&self) -> Option<&str> {
        self.consumer.config.consumer_name.as_deref()
    }

    /// returns the reader's id
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn reader_id(&self) -> u64 {
        self.consumer.consumer_id
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek(
        &mut self,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        self.consumer.seek(message_id, timestamp).await
    }

    /// returns the date of the last message reception
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.consumer.last_message_received()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(&mut self) -> Result<MessageIdData, Error> {
        self.consumer.get_last_message_id().await
    }

    /// returns the current number of messages received
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_received(&self) -> u64 {
        self.consumer.messages_received()
    }
}

#[cfg(test)]
mod tests {
    use crate::consumer::{DeadLetterPolicy, InitialPosition, Message};
    use crate::proto::MessageIdData;
    use crate::reader::Reader;
    use crate::{
        producer, ConsumerOptions, DeserializeMessage, Error, Executor, Payload, Pulsar,
        SerializeMessage, SubType, TokioExecutor,
    };
    use futures::StreamExt;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tokio::time::timeout;

    #[derive(Serialize, Deserialize)]
    struct TestData {
        data: String,
    }

    impl SerializeMessage for &TestData {
        fn serialize_message(input: Self) -> Result<producer::Message, Error> {
            let payload = serde_json::to_vec(&input).map_err(|e| Error::Custom(e.to_string()))?;
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

    #[tokio::test]
    async fn reader() {
        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("test_reader_{}", rand::random::<u16>());
        let dead_letter_policy = DeadLetterPolicy {
            max_redeliver_count: 1,
            dead_letter_topic: format!("{}_dead_letter", &topic),
        };
        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();
        let mut reader: Reader<TestData, _> = client
            .reader()
            .with_topic(&topic)
            .with_consumer_name("test_reader")
            .with_subscription("test_reader_subscription")
            .with_dead_letter_policy(dead_letter_policy)
            .with_options(ConsumerOptions::default())
            .into_reader()
            .await
            .unwrap();
        assert!(reader.check_connection().await.is_ok());
        assert_eq!(reader.topic(), topic);

        let url = reader.connections().await.unwrap();
        assert_eq!(url.as_str(), addr);

        let option = reader.options();
        assert_eq!(option.initial_position, InitialPosition::Latest);

        let policy = reader.dead_letter_policy().unwrap();
        assert_eq!(policy.max_redeliver_count, 1);
        assert_eq!(policy.dead_letter_topic, format!("{}_dead_letter", &topic));
        assert_eq!(reader.subscription(), "test_reader_subscription");
        assert_eq!(reader.sub_type(), SubType::Exclusive);
        assert_eq!(reader.batch_size(), None);
        assert_eq!(reader.reader_name().unwrap(), "test_reader");
        let reader_id = reader.reader_id();
        assert!(reader_id > 0);

        let message = TestData {
            data: "test_reader_data".to_string(),
        };
        let message_count = 10;
        let mut lastest_message_id: [u64; 2] = [0, 0];
        for index in 0..message_count {
            let receipt = client.send(&topic, &message).await.unwrap().await.unwrap();
            let message_id = receipt.message_id.unwrap();
            println!(
                "producer sends done, message_id: {}:{}",
                message_id.ledger_id, message_id.entry_id
            );
            if index == message_count - 1 {
                lastest_message_id[0] = message_id.ledger_id;
                lastest_message_id[1] = message_id.entry_id;
            }
        }

        let mut seek_message_id: Option<MessageIdData> = None;
        let messages = reader_messages(&mut reader, message_count, 500).await;
        assert_eq!(messages.len(), message_count);
        for (i, data) in messages.into_iter().enumerate() {
            let value = data.deserialize().unwrap();
            assert_eq!(value.data, "test_reader_data".to_string());
            if i == message_count / 2 {
                seek_message_id = Some(data.message_id.id.clone());
            }
        }
        let time = reader.last_message_received().unwrap();
        assert!(time <= chrono::Utc::now());

        let last_message_id_data = reader.get_last_message_id().await.unwrap();
        println!("last message id: {:?}", last_message_id_data);
        assert_eq!(last_message_id_data.ledger_id, lastest_message_id[0]);
        assert_eq!(last_message_id_data.entry_id, lastest_message_id[1]);

        let received = reader.messages_received();
        assert_eq!(received, message_count as u64);

        // seek to half message
        reader.seek(seek_message_id, None).await.unwrap();
        let seek_message = reader_messages(&mut reader, message_count / 2, 500).await;
        assert_eq!(seek_message.len(), message_count / 2);
    }

    async fn reader_messages(
        reader: &mut Reader<TestData, impl Executor>,
        max_num_messages: usize,
        receive_timeout_ms: u64,
    ) -> Vec<Message<TestData>> {
        let mut messages = Vec::new();
        loop {
            match timeout(Duration::from_millis(receive_timeout_ms), reader.next()).await {
                Ok(Some(msg)) => {
                    let msg = msg.unwrap();
                    messages.push(msg);
                    if messages.len() >= max_num_messages {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    info!("timed out waiting for messages: {}", e);
                    break;
                }
            }
        }
        messages
    }
}
