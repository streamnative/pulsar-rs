use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{future::try_join_all, StreamExt};
use rand::{distributions::Alphanumeric, Rng};
use regex::Regex;

use crate::{
    consumer::{
        config::ConsumerConfig, data::DeadLetterPolicy, multi::MultiTopicConsumer,
        negative_ack_backoff::NegativeAckBackoff, options::ConsumerOptions, topic::TopicConsumer,
        InnerConsumer,
    },
    message::proto::command_subscribe::SubType,
    reader::{Reader, State},
    BrokerAddress, Consumer, DeserializeMessage, Error, Executor, Pulsar,
};

/// Builder structure for consumers
///
/// This is the main way to create a [Consumer] or a [Reader]
#[derive(Clone)]
pub struct ConsumerBuilder<Exe: Executor> {
    pulsar: Pulsar<Exe>,
    topics: Option<Vec<String>>,
    topic_regex: Option<Regex>,
    subscription: Option<String>,
    subscription_type: Option<SubType>,
    consumer_id: Option<u64>,
    consumer_name: Option<String>,
    batch_size: Option<u32>,
    unacked_message_resend_delay: Option<Duration>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    consumer_options: Option<ConsumerOptions>,
    namespace: Option<String>,
    topic_refresh: Option<Duration>,
    nack_redelivery_delay: Option<Duration>,
    negative_ack_backoff: Option<Arc<dyn NegativeAckBackoff + Send + Sync>>,
}

fn check_nack_delay_duration(delay: Duration) -> Result<(), Error> {
    if delay.as_millis() >= u64::MAX as u128 {
        return Err(Error::Custom("delay duration is too large".to_string()));
    }

    if Instant::now().checked_add(delay).is_none() {
        return Err(Error::Custom("delay duration is too large".to_string()));
    }

    Ok(())
}

impl<Exe: Executor> ConsumerBuilder<Exe> {
    fn topic_consumer_configs_for_joined_topics(
        config: &ConsumerConfig,
        joined_topics: &[(String, BrokerAddress)],
    ) -> Vec<ConsumerConfig> {
        joined_topics
            .iter()
            .map(|_| config.clone_for_topic_consumer())
            .collect()
    }

    fn topic_consumer_config_for_reader(config: &ConsumerConfig) -> ConsumerConfig {
        config.clone_for_topic_consumer()
    }

    /// Creates a new [ConsumerBuilder] from an existing client instance
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(pulsar: &Pulsar<Exe>) -> Self {
        ConsumerBuilder {
            pulsar: pulsar.clone(),
            topics: None,
            topic_regex: None,
            subscription: None,
            subscription_type: None,
            consumer_id: None,
            consumer_name: None,
            batch_size: None,
            // TODO what should this default to? None seems incorrect..
            unacked_message_resend_delay: None,
            dead_letter_policy: None,
            consumer_options: None,
            namespace: None,
            topic_refresh: None,
            nack_redelivery_delay: None,
            negative_ack_backoff: None,
        }
    }

    /// sets the consumer's topic or add one to the list of topics
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topic<S: Into<String>>(mut self, topic: S) -> ConsumerBuilder<Exe> {
        match &mut self.topics {
            Some(topics) => topics.push(topic.into()),
            None => self.topics = Some(vec![topic.into()]),
        }
        self
    }

    /// adds a list of topics to the future consumer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topics<S: AsRef<str>, I: IntoIterator<Item = S>>(
        mut self,
        topics: I,
    ) -> ConsumerBuilder<Exe> {
        let new_topics = topics.into_iter().map(|t| t.as_ref().into());
        match &mut self.topics {
            Some(topics) => {
                topics.extend(new_topics);
            }
            None => self.topics = Some(new_topics.collect()),
        }
        self
    }

    /// sets up a consumer that will listen on all topics matching the regular
    /// expression
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topic_regex(mut self, regex: Regex) -> ConsumerBuilder<Exe> {
        self.topic_regex = Some(regex);
        self
    }

    /// sets the subscription's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_subscription<S: Into<String>>(mut self, subscription: S) -> Self {
        self.subscription = Some(subscription.into());
        self
    }

    /// sets the kind of subscription
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_subscription_type(mut self, subscription_type: SubType) -> Self {
        self.subscription_type = Some(subscription_type);
        self
    }

    /// Tenant/Namespace to be used when matching against a regex. For other
    /// consumers, specify namespace using the
    /// `<persistent|non-persistent://<tenant>/<namespace>/<topic>`
    /// topic format.
    /// Defaults to `public/default` if not specifid
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_lookup_namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Interval for refreshing the topics when using a topic regex or when errors occur with a
    /// MultiTopicConsumer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_topic_refresh(mut self, refresh_interval: Duration) -> Self {
        self.topic_refresh = Some(refresh_interval);
        self
    }

    /// sets the consumer id for this consumer
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_consumer_id(mut self, consumer_id: u64) -> Self {
        self.consumer_id = Some(consumer_id);
        self
    }

    /// sets the consumer's name
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_consumer_name<S: Into<String>>(mut self, consumer_name: S) -> Self {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    /// sets the batch size
    ///
    /// batch messages containing more than the configured batch size will
    /// not be sent by Pulsar
    ///
    /// default value: 1000
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// sets consumer options
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_options(mut self, options: ConsumerOptions) -> Self {
        self.consumer_options = Some(options);
        self
    }

    /// sets the dead letter policy
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_dead_letter_policy(mut self, dead_letter_policy: DeadLetterPolicy) -> Self {
        self.dead_letter_policy = Some(dead_letter_policy);
        self
    }

    /// The time after which a message is dropped without being acknowledged or nacked
    /// that the message is resent. If `None`, messages will only be resent when a
    /// consumer disconnects with pending unacknowledged messages.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_unacked_message_resend_delay(mut self, delay: Option<Duration>) -> Self {
        self.unacked_message_resend_delay = delay;
        self
    }

    /// Sets the fixed negative-ack redelivery delay.
    ///
    /// If this option is not set, negative acknowledgments use a 60 seconds default delay before
    /// broker redelivery. This is an intentional parity behavior change from the previous Rust
    /// client immediate-redelivery default.
    ///
    /// `Duration::ZERO` is valid and means zero delay immediate redelivery; use it to preserve the
    /// previous Rust-client behavior. Repeated calls are last-call-wins.
    ///
    /// Delayed negative acks are tracked by a best-effort in-memory timer. Pending timers are
    /// dropped on consumer close or process loss, and are preserved only best-effort across
    /// reconnect when the same consumer engine survives.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_nack_redelivery_delay(mut self, delay: Duration) -> Self {
        self.nack_redelivery_delay = Some(delay);
        self
    }

    /// Configures an optional negative-ack backoff policy.
    ///
    /// Message-based nacks use the message redelivery count supplied by the broker to compute the
    /// backoff delay. ID-only `nack_with_id` calls have no redelivery count, so the ID-only fixed
    /// fallback uses [`Self::with_nack_redelivery_delay`] or the 60 seconds default instead.
    /// The fixed fallback never uses the backoff policy.
    ///
    /// A backoff that returns `Duration::ZERO` uses zero delay immediate redelivery. Batched message
    /// ids use batch-entry normalization for scheduling, while ack cancellation remains
    /// partial-batch-safe; this does not expose public precision controls or partial-batch
    /// redelivery APIs.
    ///
    /// Negative-ack tracking is separate from unacked-message resend tracking and preserves the
    /// existing Rust DLQ behavior. This option adds no retry-letter/DLQ redesign, no
    /// delayed-reconsume behavior, and no acknowledgement-timeout backoff behavior.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_negative_ack_backoff<B>(mut self, backoff: B) -> Self
    where
        B: NegativeAckBackoff + 'static,
    {
        self.negative_ack_backoff =
            Some(Arc::new(backoff) as Arc<dyn NegativeAckBackoff + Send + Sync>);
        self
    }

    // Checks the builder for inconsistencies
    // returns a config and a list of topics with associated brokers
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn validate(self) -> Result<(ConsumerConfig, Vec<(String, BrokerAddress)>), Error> {
        let ConsumerBuilder {
            pulsar,
            topics,
            topic_regex,
            subscription,
            subscription_type,
            consumer_id,
            mut consumer_name,
            batch_size,
            unacked_message_resend_delay,
            consumer_options,
            dead_letter_policy,
            namespace: _,
            topic_refresh: _,
            nack_redelivery_delay,
            negative_ack_backoff,
        } = self;

        if consumer_name.is_none() {
            let s: String = (0..8)
                .map(|_| rand::thread_rng().sample(Alphanumeric))
                .map(|c| c as char)
                .collect();
            consumer_name = Some(format!("consumer_{s}"));
        }

        if topics.is_none() && topic_regex.is_none() {
            return Err(Error::Custom(
                "Cannot create consumer with no topics and no topic regex".into(),
            ));
        }
        if let Some(delay) = nack_redelivery_delay {
            check_nack_delay_duration(delay)?;
        }

        let topics: Vec<(String, BrokerAddress)> = try_join_all(
            topics
                .into_iter()
                .flatten()
                .map(|topic| pulsar.lookup_partitioned_topic(topic)),
        )
        .await?
        .into_iter()
        .flatten()
        .collect();

        if topics.is_empty() && topic_regex.is_none() {
            return Err(Error::Custom(
                "Unable to create consumer - topic not found".to_string(),
            ));
        }

        let consumer_id = match (consumer_id, topics.len()) {
            (Some(consumer_id), 1) => Some(consumer_id),
            (Some(_), _) => {
                warn!(
                    "Cannot specify consumer id for connecting to partitioned topics or multiple \
                     topics"
                );
                None
            }
            _ => None,
        };
        let subscription = subscription.unwrap_or_else(|| {
            let s: String = (0..8)
                .map(|_| rand::thread_rng().sample(Alphanumeric))
                .map(|c| c as char)
                .collect();
            let subscription = format!("sub_{s}");
            warn!(
                "Subscription not specified. Using new subscription `{}`.",
                subscription
            );
            subscription
        });
        let sub_type = subscription_type.unwrap_or_else(|| {
            warn!("Subscription Type not specified. Defaulting to `Shared`.");
            SubType::Shared
        });

        let config = ConsumerConfig {
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_redelivery_delay: unacked_message_resend_delay,
            options: consumer_options.unwrap_or_default(),
            dead_letter_policy,
            nack_redelivery_delay,
            negative_ack_backoff,
        };
        Ok((config, topics))
    }

    /// creates a [Consumer] from this builder
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn build<T: DeserializeMessage>(self) -> Result<Consumer<T, Exe>, Error> {
        // would this clone() consume too much memory?
        let (config, joined_topics) = self.clone().validate().await?;

        let topic_consumer_configs =
            Self::topic_consumer_configs_for_joined_topics(&config, &joined_topics);
        let consumers = try_join_all(joined_topics.into_iter().zip(topic_consumer_configs).map(
            |((topic, addr), topic_consumer_config)| {
                TopicConsumer::new(self.pulsar.clone(), topic, addr, topic_consumer_config)
            },
        ))
        .await?;

        let consumer = if consumers.len() == 1 {
            let consumer = consumers.into_iter().next().unwrap();
            InnerConsumer::Single(consumer)
        } else {
            let consumers: BTreeMap<_, _> = consumers
                .into_iter()
                .map(|c| (c.topic(), Box::pin(c)))
                .collect();
            let topics: VecDeque<String> = consumers.keys().cloned().collect();
            let existing_topics = topics.clone();
            let topic_refresh = self
                .topic_refresh
                .unwrap_or_else(|| Duration::from_secs(30));
            let refresh = Box::pin(self.pulsar.executor.interval(topic_refresh).map(drop));
            let mut consumer = MultiTopicConsumer {
                namespace: self
                    .namespace
                    .unwrap_or_else(|| "public/default".to_string()),
                topic_regex: self.topic_regex,
                pulsar: self.pulsar,
                consumers,
                topics,
                existing_topics,
                new_consumers: None,
                refresh,
                config,
                disc_last_message_received: None,
                disc_messages_received: 0,
            };
            if consumer.topic_regex.is_some() {
                consumer.update_topics();
                let initial_consumers = consumer.new_consumers.take().unwrap().await?;
                consumer.add_consumers(initial_consumers);
            }
            InnerConsumer::Multi(consumer)
        };
        Ok(Consumer { inner: consumer })
    }

    /// creates a [Reader] from this builder
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn into_reader<T: DeserializeMessage>(self) -> Result<Reader<T, Exe>, Error> {
        if self.topic_regex.is_some() {
            return Err(Error::Custom(
                "Unable to create a reader - topic regex is not supported".to_string(),
            ));
        }

        // would this clone() consume too much memory?
        let (mut config, mut joined_topics) = self.clone().validate().await?;

        // Internally, the reader interface is implemented as a consumer using an exclusive,
        // non-durable subscription
        config.options.durable = Some(false);

        // the validate() function defaults sub_type to SubType::Shared,
        // but a reader's subscription is exclusive
        warn!("Subscription Type for a reader is `Exclusive`. Resetting.");
        config.sub_type = SubType::Exclusive;

        if joined_topics.len() != 1 {
            return Err(Error::Custom(
                "Unable to create a reader - exactly one topic is required".to_string(),
            ));
        }

        let (topic, addr) = joined_topics.pop().expect("len checked above");
        let topic_consumer_config = Self::topic_consumer_config_for_reader(&config);
        let consumer =
            TopicConsumer::new(self.pulsar.clone(), topic, addr, topic_consumer_config).await?;

        Ok(Reader {
            consumer,
            state: Some(State::PollingConsumer),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::TokioExecutor;

    #[test]
    fn test_nack_delay_validation_oversized() {
        let result = check_nack_delay_duration(Duration::MAX);

        assert!(result.is_err());
    }

    #[test]
    fn test_nack_delay_validation_rejects_unrepresentable_instant_delay() {
        let result = check_nack_delay_duration(Duration::from_millis(u64::MAX));

        assert!(result.is_err());
    }

    #[test]
    fn test_nack_delay_validation_zero_is_valid() {
        let result = check_nack_delay_duration(Duration::ZERO);

        assert!(result.is_ok());
    }

    #[test]
    fn test_consumer_config_nack_delay_default_is_none() {
        let config = ConsumerConfig::default();

        assert!(config.nack_redelivery_delay.is_none());
        assert!(config.negative_ack_backoff.is_none());
    }

    #[test]
    fn test_consumer_config_stores_nack_delay() {
        let config = ConsumerConfig {
            nack_redelivery_delay: Some(Duration::from_secs(5)),
            ..ConsumerConfig::default()
        };

        assert_eq!(config.nack_redelivery_delay, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_consumer_config_stores_both_nack_configs_independently() {
        let config = ConsumerConfig {
            nack_redelivery_delay: Some(Duration::from_secs(5)),
            negative_ack_backoff: Some(Arc::new(
                crate::consumer::negative_ack_backoff::MultiplierRedeliveryBackoff::default(),
            )),
            ..ConsumerConfig::default()
        };

        assert!(config.nack_redelivery_delay.is_some());
        assert!(config.negative_ack_backoff.is_some());
    }

    #[test]
    fn test_consumer_config_backoff_only_stores_none_for_delay() {
        let config = ConsumerConfig {
            negative_ack_backoff: Some(Arc::new(
                crate::consumer::negative_ack_backoff::MultiplierRedeliveryBackoff::default(),
            )),
            ..ConsumerConfig::default()
        };

        assert!(config.nack_redelivery_delay.is_none());
        assert!(config.negative_ack_backoff.is_some());
    }

    #[test]
    fn negative_ack_runtime_docs_cover_phase4_semantics() {
        let source = include_str!("builder.rs");
        let docs_source = source
            .split("#[cfg(test)]")
            .next()
            .expect("builder source contains test module marker");

        for expected in [
            "60 seconds",
            "zero delay",
            "immediate redelivery",
            "message redelivery count",
            "ID-only",
            "fixed fallback",
            "batch-entry normalization",
            "best-effort in-memory timer",
            "no retry-letter/DLQ redesign",
        ] {
            assert!(
                docs_source.contains(expected),
                "missing negative-ack docs phrase: {expected}"
            );
        }
    }

    fn nack_config_with_delay_and_backoff() -> ConsumerConfig {
        ConsumerConfig {
            nack_redelivery_delay: Some(Duration::from_secs(30)),
            negative_ack_backoff: Some(Arc::new(
                crate::consumer::negative_ack_backoff::MultiplierRedeliveryBackoff::default(),
            )),
            ..ConsumerConfig::default()
        }
    }

    fn broker_addr(port: u16) -> BrokerAddress {
        BrokerAddress {
            url: format!("pulsar://127.0.0.1:{port}").parse().unwrap(),
            broker_url: format!("127.0.0.1:{port}"),
            proxy: false,
        }
    }

    fn joined_topics(count: usize) -> Vec<(String, BrokerAddress)> {
        (0..count)
            .map(|idx| {
                (
                    format!("persistent://public/default/topic-{idx}"),
                    broker_addr(6650 + idx as u16),
                )
            })
            .collect()
    }

    fn assert_topic_consumer_receives_negative_ack_config(
        passed_to_topic_consumer: &ConsumerConfig,
    ) {
        assert_eq!(
            passed_to_topic_consumer.nack_redelivery_delay,
            Some(Duration::from_secs(30))
        );
        assert!(passed_to_topic_consumer.negative_ack_backoff.is_some());
    }

    #[test]
    fn single_topic_consumer_config_carries_nack_delay_to_engine() {
        let config = nack_config_with_delay_and_backoff();
        let topic_consumer_configs =
            ConsumerBuilder::<TokioExecutor>::topic_consumer_configs_for_joined_topics(
                &config,
                &joined_topics(1),
            );

        assert_eq!(topic_consumer_configs.len(), 1);
        assert_topic_consumer_receives_negative_ack_config(&topic_consumer_configs[0]);
    }

    #[test]
    fn partitioned_consumer_config_carries_nack_delay_to_each_engine() {
        let config = nack_config_with_delay_and_backoff();
        let topic_consumer_configs =
            ConsumerBuilder::<TokioExecutor>::topic_consumer_configs_for_joined_topics(
                &config,
                &joined_topics(3),
            );

        assert_eq!(topic_consumer_configs.len(), 3);
        for topic_consumer_config in &topic_consumer_configs {
            assert_topic_consumer_receives_negative_ack_config(topic_consumer_config);
        }
    }

    #[test]
    fn multi_topic_consumer_config_carries_nack_delay_to_each_engine() {
        let config = nack_config_with_delay_and_backoff();
        let topic_consumer_configs =
            ConsumerBuilder::<TokioExecutor>::topic_consumer_configs_for_joined_topics(
                &config,
                &joined_topics(2),
            );

        assert_eq!(topic_consumer_configs.len(), 2);
        for topic_consumer_config in &topic_consumer_configs {
            assert_topic_consumer_receives_negative_ack_config(topic_consumer_config);
        }
    }

    #[test]
    fn reader_consumer_config_carries_nack_delay_to_engine() {
        let config = nack_config_with_delay_and_backoff();
        let mut reader_config = config;
        reader_config.options.durable = Some(false);
        reader_config.sub_type = SubType::Exclusive;

        let topic_consumer_config =
            ConsumerBuilder::<TokioExecutor>::topic_consumer_config_for_reader(&reader_config);

        assert_topic_consumer_receives_negative_ack_config(&topic_consumer_config);
        assert_eq!(topic_consumer_config.options.durable, Some(false));
        assert_eq!(topic_consumer_config.sub_type, SubType::Exclusive);
    }

    #[test]
    fn build_path_config_helpers_clone_for_each_topic_consumer() {
        let mut config = nack_config_with_delay_and_backoff();
        config.consumer_id = Some(7);
        let topic_consumer_configs =
            ConsumerBuilder::<TokioExecutor>::topic_consumer_configs_for_joined_topics(
                &config,
                &joined_topics(2),
            );

        assert_eq!(topic_consumer_configs.len(), 2);
        assert!(topic_consumer_configs
            .iter()
            .all(|topic_consumer_config| topic_consumer_config.consumer_id == Some(7)));
    }
}
