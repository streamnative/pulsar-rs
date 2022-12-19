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
                    let message_id = msg.message_id.clone();
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
