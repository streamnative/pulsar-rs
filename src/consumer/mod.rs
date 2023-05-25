//! Topic subscriptions

mod batched_message_iterator;
pub mod builder;
pub mod config;
pub mod data;
mod engine;
pub(crate) mod initial_position;
pub mod message;
mod multi;
pub mod options;
pub mod topic;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    pin::Pin,
    time::Duration,
};

pub use builder::ConsumerBuilder;
use chrono::{DateTime, Utc};
pub use data::{DeadLetterPolicy, EngineMessage};
use futures::{
    future::try_join_all,
    task::{Context, Poll},
    Stream, StreamExt,
};
pub use initial_position::InitialPosition;
pub use message::Message;
use multi::MultiTopicConsumer;
pub use options::ConsumerOptions;
pub use topic::TopicConsumer;
use url::Url;

use crate::{
    error::{ConsumerError, Error},
    executor::Executor,
    message::proto::{command_subscribe::SubType, MessageIdData},
    proto::CommandConsumerStatsResponse,
    DeserializeMessage, Pulsar,
};

enum InnerConsumer<T: DeserializeMessage, Exe: Executor> {
    Single(TopicConsumer<T, Exe>),
    Multi(MultiTopicConsumer<T, Exe>),
}

/// the consumer is used to subscribe to a topic
///
/// ```rust,no_run
/// use pulsar::{Consumer, SubType};
/// use futures::StreamExt;
///
/// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
/// # type TestData = String;
/// let mut consumer: Consumer<TestData, _> = pulsar
///     .consumer()
///     .with_topic("non-persistent://public/default/test")
///     .with_consumer_name("test_consumer")
///     .with_subscription_type(SubType::Exclusive)
///     .with_subscription("test_subscription")
///     .build()
///     .await?;
///
/// let mut counter = 0usize;
/// while let Some(Ok(msg)) = consumer.next().await {
///     consumer.ack(&msg).await?;
///     let data = match msg.deserialize() {
///         Ok(data) => data,
///         Err(e) => {
///             log::error!("could not deserialize message: {:?}", e);
///             break;
///         }
///     };
///
///     counter += 1;
///     log::info!("got {} messages", counter);
/// }
/// # Ok(())
/// # }
/// ```
pub struct Consumer<T: DeserializeMessage, Exe: Executor> {
    inner: InnerConsumer<T, Exe>,
}

impl<T: DeserializeMessage, Exe: Executor> Consumer<T, Exe> {
    /// creates a [ConsumerBuilder] from a client instance
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn builder(pulsar: &Pulsar<Exe>) -> ConsumerBuilder<Exe> {
        ConsumerBuilder::new(pulsar)
    }

    /// test that the connections to the Pulsar brokers are still valid
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connection(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.check_connection().await,
            InnerConsumer::Multi(c) => c.check_connections().await,
        }
    }

    /// get consumer stats
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_stats(&mut self) -> Result<Vec<CommandConsumerStatsResponse>, Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => Ok(vec![c.get_stats().await?]),
            InnerConsumer::Multi(c) => c.get_stats().await,
        }
    }

    /// acknowledges a single message
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.ack(msg).await,
            InnerConsumer::Multi(c) => c.ack(msg).await,
        }
    }

    /// acknowledges a single message with a given ID.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack_with_id(
        &mut self,
        topic: &str,
        msg_id: MessageIdData,
    ) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.ack_with_id(msg_id).await,
            InnerConsumer::Multi(c) => c.ack_with_id(topic, msg_id).await,
        }
    }

    /// acknowledges a message and all the preceding messages
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.cumulative_ack(msg).await,
            InnerConsumer::Multi(c) => c.cumulative_ack(msg).await,
        }
    }

    /// acknowledges a message and all the preceding messages with a given ID.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack_with_id(
        &mut self,
        topic: &str,
        msg_id: MessageIdData,
    ) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.cumulative_ack_with_id(msg_id).await,
            InnerConsumer::Multi(c) => c.cumulative_ack_with_id(topic, msg_id).await,
        }
    }

    /// negative acknowledgement
    ///
    /// the message will be sent again on the subscription
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.nack(msg).await,
            InnerConsumer::Multi(c) => c.nack(msg).await,
        }
    }

    /// negative acknowledgement
    ///
    /// the message with the given ID will be sent again on the subscription
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack_with_id(
        &mut self,
        topic: &str,
        msg_id: MessageIdData,
    ) -> Result<(), ConsumerError> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.nack_with_id(msg_id).await,
            InnerConsumer::Multi(c) => c.nack_with_id(topic, msg_id).await,
        }
    }

    /// seek currently destroys the existing consumer and creates a new one
    /// this is how java and cpp pulsar client implement this feature mainly because
    /// there are many minor problems with flushing existing messages and receiving new ones
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek(
        &mut self,
        consumer_ids: Option<Vec<String>>,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
        client: Pulsar<Exe>,
    ) -> Result<(), Error> {
        let inner_consumer: InnerConsumer<T, Exe> = match &mut self.inner {
            InnerConsumer::Single(c) => {
                c.seek(message_id, timestamp).await?;
                let topic = c.topic().to_string();
                let addr = client.lookup_topic(&topic).await?;
                let config = c.config().clone();
                InnerConsumer::Single(TopicConsumer::new(client, topic, addr, config).await?)
            }
            InnerConsumer::Multi(c) => {
                c.seek(consumer_ids, message_id, timestamp).await?;
                let topics = c.topics();
                let config = c.config().clone();

                //currently, pulsar only supports seek for non partitioned topics
                let addrs =
                    try_join_all(topics.into_iter().map(|topic| client.lookup_topic(topic)))
                        .await?;

                let topic_addr_pair = c.topics.iter().cloned().zip(addrs.iter().cloned());

                let consumers = try_join_all(topic_addr_pair.map(|(topic, addr)| {
                    TopicConsumer::new(client.clone(), topic, addr, config.clone())
                }))
                .await?;

                let consumers: BTreeMap<_, _> = consumers
                    .into_iter()
                    .map(|c| (c.topic(), Box::pin(c)))
                    .collect();
                let topics: VecDeque<String> = consumers.keys().cloned().collect();
                let existing_topics = topics.clone();
                let topic_refresh = Duration::from_secs(30);
                let refresh = Box::pin(client.executor.interval(topic_refresh).map(drop));
                let namespace = c.namespace.clone();
                let config = c.config().clone();
                let topic_regex = c.topic_regex.clone();
                InnerConsumer::Multi(MultiTopicConsumer {
                    namespace,
                    topic_regex,
                    pulsar: client,
                    consumers,
                    topics,
                    existing_topics,
                    new_consumers: None,
                    refresh,
                    config,
                    disc_last_message_received: None,
                    disc_messages_received: 0,
                })
            }
        };

        self.inner = inner_consumer;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.unsubscribe().await,
            InnerConsumer::Multi(c) => c.unsubscribe().await,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close(&mut self) -> Result<(), Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => c.close().await,
            InnerConsumer::Multi(c) => c.close().await,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(&mut self) -> Result<Vec<MessageIdData>, Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => Ok(vec![c.get_last_message_id().await?]),
            InnerConsumer::Multi(c) => c.get_last_message_id().await,
        }
    }

    /// returns the list of topics this consumer is subscribed on
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topics(&self) -> Vec<String> {
        match &self.inner {
            InnerConsumer::Single(c) => vec![c.topic()],
            InnerConsumer::Multi(c) => c.existing_topics(),
        }
    }

    /// returns a list of broker URLs this consumer is connnected to
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn connections(&mut self) -> Result<Vec<Url>, Error> {
        match &mut self.inner {
            InnerConsumer::Single(c) => Ok(vec![c.connection().await?.url().clone()]),
            InnerConsumer::Multi(c) => {
                let v = c
                    .consumers
                    .values_mut()
                    .map(|c| c.connection())
                    .collect::<Vec<_>>();

                let mut connections = try_join_all(v).await?;
                Ok(connections
                    .drain(..)
                    .map(|conn| conn.url().clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect())
            }
        }
    }

    /// returns the consumer's configuration options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn options(&self) -> &ConsumerOptions {
        match &self.inner {
            InnerConsumer::Single(c) => &c.config.options,
            InnerConsumer::Multi(c) => &c.config.options,
        }
    }

    /// returns the consumer's dead letter policy options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn dead_letter_policy(&self) -> Option<&DeadLetterPolicy> {
        match &self.inner {
            InnerConsumer::Single(c) => c.dead_letter_policy.as_ref(),
            InnerConsumer::Multi(c) => c.config.dead_letter_policy.as_ref(),
        }
    }

    /// returns the consumer's subscription name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn subscription(&self) -> &str {
        match &self.inner {
            InnerConsumer::Single(c) => &c.config.subscription,
            InnerConsumer::Multi(c) => &c.config.subscription,
        }
    }

    /// returns the consumer's subscription type
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn sub_type(&self) -> SubType {
        match &self.inner {
            InnerConsumer::Single(c) => c.config.sub_type,
            InnerConsumer::Multi(c) => c.config.sub_type,
        }
    }

    /// returns the consumer's batch size
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn batch_size(&self) -> Option<u32> {
        match &self.inner {
            InnerConsumer::Single(c) => c.config.batch_size,
            InnerConsumer::Multi(c) => c.config.batch_size,
        }
    }

    /// returns the consumer's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn consumer_name(&self) -> Option<&str> {
        match &self.inner {
            InnerConsumer::Single(c) => &c.config.consumer_name,
            InnerConsumer::Multi(c) => &c.config.consumer_name,
        }
        .as_ref()
        .map(|s| s.as_str())
    }

    /// returns the consumer's list of ids
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn consumer_id(&self) -> Vec<u64> {
        match &self.inner {
            InnerConsumer::Single(c) => vec![c.consumer_id],
            InnerConsumer::Multi(c) => c.consumers.values().map(|c| c.consumer_id).collect(),
        }
    }

    /// returns the consumer's redelivery delay
    ///
    /// if messages are not acknowledged before this delay, they will be sent
    /// again on the subscription
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn unacked_message_redelivery_delay(&self) -> Option<Duration> {
        match &self.inner {
            InnerConsumer::Single(c) => c.config.unacked_message_redelivery_delay,
            InnerConsumer::Multi(c) => c.config.unacked_message_redelivery_delay,
        }
    }

    /// returns the date of the last message reception
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        match &self.inner {
            InnerConsumer::Single(c) => c.last_message_received(),
            InnerConsumer::Multi(c) => c.last_message_received(),
        }
    }

    /// returns the current number of messages received
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_received(&self) -> u64 {
        match &self.inner {
            InnerConsumer::Single(c) => c.messages_received(),
            InnerConsumer::Multi(c) => c.messages_received(),
        }
    }
}

//TODO: why does T need to be 'static?
impl<T: DeserializeMessage + 'static, Exe: Executor> Stream for Consumer<T, Exe> {
    type Item = Result<Message<T>, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.inner {
            InnerConsumer::Single(c) => Pin::new(c).poll_next(cx),
            InnerConsumer::Multi(c) => Pin::new(c).poll_next(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        iter,
        time::{SystemTime, UNIX_EPOCH},
    };

    use futures::{
        future::{select, Either},
        StreamExt, TryStreamExt,
    };
    use log::LevelFilter;
    use regex::Regex;
    #[cfg(feature = "tokio-runtime")]
    use tokio::time::timeout;

    use super::*;
    #[cfg(feature = "tokio-runtime")]
    use crate::executor::TokioExecutor;
    use crate::{
        consumer::initial_position::InitialPosition, producer, tests::TEST_LOGGER, Payload, Pulsar,
        SerializeMessage,
    };

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
    pub struct TestData {
        topic: String,
        msg: u32,
    }

    impl<'a> SerializeMessage for &'a TestData {
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

    pub static MULTI_LOGGER: crate::tests::SimpleLogger = crate::tests::SimpleLogger {
        tag: "multi_consumer",
    };
    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn multi_consumer() {
        let _result = log::set_logger(&MULTI_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";

        let topic_n: u16 = rand::random();
        let topic1 = format!("multi_consumer_a_{topic_n}");
        let topic2 = format!("multi_consumer_b_{topic_n}");

        let data1 = TestData {
            topic: "a".to_owned(),
            msg: 1,
        };
        let data2 = TestData {
            topic: "a".to_owned(),
            msg: 2,
        };
        let data3 = TestData {
            topic: "b".to_owned(),
            msg: 3,
        };
        let data4 = TestData {
            topic: "b".to_owned(),
            msg: 4,
        };

        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        try_join_all(vec![
            client.send(&topic1, &data1),
            client.send(&topic1, &data2),
            client.send(&topic2, &data3),
            client.send(&topic2, &data4),
        ])
        .await
        .unwrap();

        let builder = client
            .consumer()
            .with_subscription_type(SubType::Shared)
            // get earliest messages
            .with_options(ConsumerOptions {
                initial_position: InitialPosition::Earliest,
                ..Default::default()
            });

        let consumer_1: Consumer<TestData, _> = builder
            .clone()
            .with_subscription("consumer_1")
            .with_topics([&topic1, &topic2])
            .build()
            .await
            .unwrap();

        let consumer_2: Consumer<TestData, _> = builder
            .with_subscription("consumer_2")
            .with_topic_regex(Regex::new(&format!("multi_consumer_[ab]_{topic_n}")).unwrap())
            .build()
            .await
            .unwrap();

        let expected: HashSet<_> = vec![data1, data2, data3, data4].into_iter().collect();
        for consumer in [consumer_1, consumer_2].iter_mut() {
            let connected_topics = consumer.topics();
            debug!(
                "connected topics for {}: {:?}",
                consumer.subscription(),
                &connected_topics
            );
            assert_eq!(connected_topics.len(), 2);
            assert!(connected_topics.iter().any(|t| t.ends_with(&topic1)));
            assert!(connected_topics.iter().any(|t| t.ends_with(&topic2)));

            let mut received = HashSet::new();
            while let Some(message) = timeout(Duration::from_secs(1), consumer.next())
                .await
                .unwrap()
            {
                received.insert(message.unwrap().deserialize().unwrap());
                if received.len() == 4 {
                    break;
                }
            }
            assert_eq!(expected, received);
            assert_eq!(consumer.messages_received(), 4);
            assert!(consumer.last_message_received().is_some());
        }
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn consumer_dropped_with_lingering_acks() {
        use rand::{distributions::Alphanumeric, Rng};
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";

        let topic = format!(
            "consumer_dropped_with_lingering_acks_{}",
            rand::random::<u16>()
        );

        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        let message = TestData {
            topic: iter::repeat(())
                .map(|()| rand::thread_rng().sample(Alphanumeric) as char)
                .take(8)
                .collect(),
            msg: 1,
        };

        client.send(&topic, &message).await.unwrap().await.unwrap();
        println!("producer sends done");

        {
            println!("creating consumer");
            let mut consumer: Consumer<TestData, _> = client
                .consumer()
                .with_topic(&topic)
                .with_subscription("dropped_ack")
                .with_subscription_type(SubType::Shared)
                // get earliest messages
                .with_options(ConsumerOptions {
                    initial_position: InitialPosition::Earliest,
                    ..Default::default()
                })
                .build()
                .await
                .unwrap();

            println!("created consumer");

            // consumer.next().await
            let msg: Message<TestData> = timeout(Duration::from_secs(1), consumer.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            println!("got message: {:?}", msg.payload);
            assert_eq!(
                message,
                msg.deserialize().unwrap(),
                "we probably receive a message from a previous run of the test"
            );
            consumer.ack(&msg).await.unwrap();
        }

        {
            println!("creating second consumer. The message should have been acked");
            let mut consumer: Consumer<TestData, _> = client
                .consumer()
                .with_topic(&topic)
                .with_subscription("dropped_ack")
                .with_subscription_type(SubType::Shared)
                .with_options(ConsumerOptions {
                    initial_position: InitialPosition::Earliest,
                    ..Default::default()
                })
                .build()
                .await
                .unwrap();

            println!("created second consumer");

            // the message has already been acked, so we should not receive anything
            let res: Result<_, tokio::time::error::Elapsed> =
                tokio::time::timeout(Duration::from_secs(1), consumer.next()).await;
            let is_err = res.is_err();
            if let Ok(val) = res {
                let msg = val.unwrap().unwrap();
                println!("got message: {:?}", msg.payload);
                // cleanup for the next test
                consumer.ack(&msg).await.unwrap();
                // we should not receive a different message anyway
                assert_eq!(message, msg.deserialize().unwrap());
            }

            assert!(
                is_err,
                "waiting for a message should have timed out, since we already acknowledged the \
                 only message in the queue"
            );
        }
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn dead_letter_queue() {
        let _result = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";

        let test_id: u16 = rand::random();
        let topic = format!("dead_letter_queue_test_{test_id}");
        let test_msg: u32 = rand::random();

        let message = TestData {
            topic: topic.clone(),
            msg: test_msg,
        };

        let dead_letter_topic = format!("{topic}_dlq");

        let dead_letter_policy = DeadLetterPolicy {
            max_redeliver_count: 1,
            dead_letter_topic: dead_letter_topic.clone(),
        };

        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        println!("creating consumer");
        let mut consumer: Consumer<TestData, _> = client
            .consumer()
            .with_topic(topic.clone())
            .with_subscription("nack")
            .with_subscription_type(SubType::Shared)
            .with_dead_letter_policy(dead_letter_policy)
            .build()
            .await
            .unwrap();

        println!("created consumer");

        println!("creating second consumer that consumes from the DLQ");
        let mut dlq_consumer: Consumer<TestData, _> = client
            .clone()
            .consumer()
            .with_topic(dead_letter_topic)
            .with_subscription("dead_letter_topic")
            .with_subscription_type(SubType::Shared)
            .build()
            .await
            .unwrap();

        println!("created second consumer");

        client.send(&topic, &message).await.unwrap().await.unwrap();
        println!("producer sends done");

        let msg = consumer.next().await.unwrap().unwrap();
        println!("got message: {:?}", msg.payload);
        assert_eq!(
            message,
            msg.deserialize().unwrap(),
            "we probably received a message from a previous run of the test"
        );
        // Nacking message to send it to DLQ
        consumer.nack(&msg).await.unwrap();

        let dlq_msg = dlq_consumer.next().await.unwrap().unwrap();
        println!("got message: {:?}", msg.payload);
        assert_eq!(
            message,
            dlq_msg.deserialize().unwrap(),
            "we probably received a message from a previous run of the test"
        );
        dlq_consumer.ack(&dlq_msg).await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn failover() {
        let _result = log::set_logger(&MULTI_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("failover_{}", rand::random::<u16>());
        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        let msg_count = 100_u32;
        try_join_all((0..msg_count).map(|i| client.send(&topic, i.to_string())))
            .await
            .unwrap();

        let builder = client
            .consumer()
            .with_subscription("failover")
            .with_topic(&topic)
            .with_subscription_type(SubType::Failover)
            // get earliest messages
            .with_options(ConsumerOptions {
                initial_position: InitialPosition::Earliest,
                ..Default::default()
            });

        let mut consumer_1: Consumer<String, _> = builder.clone().build().await.unwrap();

        let mut consumer_2: Consumer<String, _> = builder.build().await.unwrap();

        let mut consumed_1 = 0_u32;
        let mut consumed_2 = 0_u32;
        let mut pending_1 = Some(consumer_1.next());
        let mut pending_2 = Some(consumer_2.next());
        while consumed_1 + consumed_2 < msg_count {
            let next = select(pending_1.take().unwrap(), pending_2.take().unwrap());
            match timeout(Duration::from_secs(2), next).await.unwrap() {
                Either::Left((msg, pending)) => {
                    consumed_1 += 1;
                    drop(consumer_1.ack(&msg.unwrap().unwrap()));
                    pending_1 = Some(consumer_1.next());
                    pending_2 = Some(pending);
                }
                Either::Right((msg, pending)) => {
                    consumed_2 += 1;
                    drop(consumer_2.ack(&msg.unwrap().unwrap()));
                    pending_1 = Some(pending);
                    pending_2 = Some(consumer_2.next());
                }
            }
        }
        match (consumed_1, consumed_2) {
            (consumed_1, 0) => assert_eq!(consumed_1, msg_count),
            (0, consumed_2) => assert_eq!(consumed_2, msg_count),
            _ => panic!(
                "Expected one consumer to consume all messages. Message count: {msg_count}, consumer_1: {consumed_1} \
                 consumer_2: {consumed_2}"
            ),
        }
    }

    #[tokio::test]
    #[cfg(feature = "tokio-runtime")]
    async fn seek_single_consumer() {
        let _result = log::set_logger(&MULTI_LOGGER);
        log::set_max_level(LevelFilter::Debug);
        log::info!("starting seek test");
        let addr = "pulsar://127.0.0.1:6650";
        let topic = format!("seek_{}", rand::random::<u16>());
        let client: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await.unwrap();

        // send 100 messages and record the starting time
        let msg_count = 100_u32;

        let start_time: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        std::thread::sleep(Duration::from_secs(2));

        println!("this is the starting time: {start_time}");

        try_join_all((0..msg_count).map(|i| client.send(&topic, i.to_string())))
            .await
            .unwrap();
        log::info!("sent all messages");

        let mut consumer_1: Consumer<String, _> = client
            .consumer()
            .with_consumer_name("seek_single_test")
            .with_subscription("seek_single_test")
            .with_subscription_type(SubType::Shared)
            .with_topic(&topic)
            .build()
            .await
            .unwrap();

        log::info!("built the consumer");

        let mut consumed_1 = 0_u32;
        while let Some(msg) = consumer_1.try_next().await.unwrap() {
            consumer_1.ack(&msg).await.unwrap();
            let publish_time = msg.metadata().publish_time;
            let data = match msg.deserialize() {
                Ok(data) => data,
                Err(e) => {
                    log::error!("could not deserialize message: {:?}", e);
                    break;
                }
            };

            consumed_1 += 1;
            log::info!(
                "first loop, got {} messages, content: {}, publish time: {}",
                consumed_1,
                data,
                publish_time
            );

            // break after enough half of the messages were received
            if consumed_1 >= msg_count / 2 {
                log::info!("first loop, received {} messages, so break", consumed_1);
                break;
            }
        }

        // // call seek(timestamp), roll back the consumer to start_time
        log::info!("calling seek method");
        // Seek in a separate task, to verify that the seek future is `Send`. See #255.
        let mut consumer_1 = tokio::task::spawn(async move {
            consumer_1
                .seek(None, None, Some(start_time), client)
                .await
                .unwrap();
            consumer_1
        })
        .await
        .unwrap();
        // let mut consumer_2: Consumer<String, _> = client
        // .consumer()
        // .with_consumer_name("seek")
        // .with_subscription("seek")
        // .with_topic(&topic)
        // .build()
        // .await
        // .unwrap();

        // then read the messages again
        let mut consumed_2 = 0_u32;
        log::info!("reading messages again");
        while let Some(msg) = consumer_1.try_next().await.unwrap() {
            let publish_time = msg.metadata().publish_time;
            consumer_1.ack(&msg).await.unwrap();
            let data = match msg.deserialize() {
                Ok(data) => data,
                Err(e) => {
                    log::error!("could not deserialize message: {:?}", e);
                    break;
                }
            };
            consumed_2 += 1;
            log::info!(
                "second loop, got {} messages, content: {},  publish time: {}",
                consumed_2,
                data,
                publish_time,
            );

            if consumed_2 >= msg_count {
                log::info!("received {} messagses, so break", consumed_2);
                break;
            }
        }

        // then check if all messages were received
        assert_eq!(50, consumed_1);
        assert_eq!(100, consumed_2);
    }
}
