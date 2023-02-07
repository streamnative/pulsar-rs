use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

use futures::{future::try_join_all, StreamExt};
use rand::{distributions::Alphanumeric, Rng};
use regex::Regex;

use crate::{
    consumer::{
        config::ConsumerConfig, data::DeadLetterPolicy, multi::MultiTopicConsumer,
        options::ConsumerOptions, topic::TopicConsumer, InnerConsumer,
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
}

impl<Exe: Executor> ConsumerBuilder<Exe> {
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

    /// Interval for refreshing the topics when using a topic regex. Unused otherwise.
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

    // Checks the builder for inconsistencies
    // returns a config and a list of topics with associated brokers
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn validate<T: DeserializeMessage>(
        self,
    ) -> Result<(ConsumerConfig, Vec<(String, BrokerAddress)>), Error> {
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
        };
        Ok((config, topics))
    }

    /// creates a [Consumer] from this builder
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn build<T: DeserializeMessage>(self) -> Result<Consumer<T, Exe>, Error> {
        // would this clone() consume too much memory?
        let (config, joined_topics) = self.clone().validate::<T>().await?;

        let consumers = try_join_all(joined_topics.into_iter().map(|(topic, addr)| {
            TopicConsumer::new(self.pulsar.clone(), topic, addr, config.clone())
        }))
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
        // would this clone() consume too much memory?
        let (mut config, mut joined_topics) = self.clone().validate::<T>().await?;

        // the validate() function defaults sub_type to SubType::Shared,
        // but a reader's subscription is exclusive
        warn!("Subscription Type for a reader is `Exclusive`. Resetting.");
        config.sub_type = SubType::Exclusive;

        if self.topics.unwrap().len() > 1 {
            return Err(Error::Custom(
                "Unable to create a reader - one topic max".to_string(),
            ));
        }

        let (topic, addr) = joined_topics.pop().unwrap();
        let consumer = TopicConsumer::new(self.pulsar.clone(), topic, addr, config.clone()).await?;

        Ok(Reader {
            consumer,
            state: Some(State::PollingConsumer),
        })
    }
}
