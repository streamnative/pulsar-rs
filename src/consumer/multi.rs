use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    future::Future,
    iter,
    pin::Pin,
    task::{Context, Poll},
};

use async_std::prelude::Stream;
use chrono::{DateTime, Utc};
use futures::future::try_join_all;
use regex::Regex;

use crate::{
    consumer::{config::ConsumerConfig, message::Message, topic::TopicConsumer},
    error::{ConnectionError, ConsumerError},
    message::proto::MessageIdData,
    proto,
    proto::CommandConsumerStatsResponse,
    DeserializeMessage, Error, Executor, Pulsar,
};

/// A consumer that can subscribe on multiple topics, from a regex matching
/// topic names
pub struct MultiTopicConsumer<T: DeserializeMessage, Exe: Executor> {
    pub(super) namespace: String,
    pub(super) topic_regex: Option<Regex>,
    pub(super) pulsar: Pulsar<Exe>,
    pub(super) consumers: BTreeMap<String, Pin<Box<TopicConsumer<T, Exe>>>>,
    pub(super) topics: VecDeque<String>,
    pub(super) existing_topics: VecDeque<String>,
    #[allow(clippy::type_complexity)]
    pub(super) new_consumers:
        Option<Pin<Box<dyn Future<Output = Result<Vec<TopicConsumer<T, Exe>>, Error>> + Send>>>,
    pub(super) refresh: Pin<Box<dyn Stream<Item = ()> + Send>>,
    pub(super) config: ConsumerConfig,
    // Stats on disconnected consumers to keep metrics correct
    pub(super) disc_messages_received: u64,
    pub(super) disc_last_message_received: Option<DateTime<Utc>>,
}

impl<T: DeserializeMessage, Exe: Executor> MultiTopicConsumer<T, Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topics(&self) -> Vec<String> {
        self.topics.iter().map(|s| s.to_string()).collect()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn existing_topics(&self) -> Vec<String> {
        self.existing_topics.iter().map(|s| s.to_string()).collect()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_stats(&mut self) -> Result<Vec<CommandConsumerStatsResponse>, Error> {
        let resposnes = try_join_all(self.consumers.values_mut().map(|c| c.get_stats())).await?;
        Ok(resposnes)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.consumers
            .values()
            .filter_map(|c| c.last_message_received)
            .chain(self.disc_last_message_received)
            .max()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_received(&self) -> u64 {
        self.consumers
            .values()
            .map(|c| c.messages_received)
            .chain(iter::once(self.disc_messages_received))
            .sum()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connections(&mut self) -> Result<(), Error> {
        self.pulsar
            .manager
            .get_base_connection()
            .await?
            .sender()
            .send_ping()
            .await?;

        for consumer in self.consumers.values_mut() {
            consumer.connection().await?.sender().send_ping().await?;
        }

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(&mut self) -> Result<Vec<MessageIdData>, Error> {
        let responses =
            try_join_all(self.consumers.values_mut().map(|c| c.get_last_message_id())).await?;
        Ok(responses)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        for consumer in self.consumers.values_mut() {
            consumer.unsubscribe().await?;
        }

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn close(&mut self) -> Result<(), Error> {
        for consumer in self.consumers.values_mut() {
            consumer.close().await?;
        }

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn add_consumers<I: IntoIterator<Item = TopicConsumer<T, Exe>>>(&mut self, consumers: I) {
        for consumer in consumers {
            let topic = consumer.topic().to_owned();
            self.consumers.insert(topic.clone(), Box::pin(consumer));
            self.existing_topics.push_back(topic);
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn remove_consumers(&mut self, topics: &[String]) {
        self.existing_topics.retain(|t| !topics.contains(t));
        for topic in topics {
            if let Some(consumer) = self.consumers.remove(topic) {
                self.disc_messages_received += consumer.messages_received;
                self.disc_last_message_received = self
                    .disc_last_message_received
                    .into_iter()
                    .chain(consumer.last_message_received)
                    .max();
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn update_topics(&mut self) {
        let existing_topics = self.existing_topics.clone();
        let consumer_config = self.config.clone();
        let pulsar = self.pulsar.clone();
        let topic_regex = self.topic_regex.clone();
        let namespace = self.namespace.clone();

        // 1. get topics
        // 1.1 original topics from builder
        let mut topics = self.topics.clone();

        self.new_consumers = Some(Box::pin(async move {
            // 1.2 append topics which match `topic_regex`
            if let Some(regex) = topic_regex {
                let all_topics = pulsar
                    .get_topics_of_namespace(
                        namespace.clone(),
                        proto::command_get_topics_of_namespace::Mode::All,
                    )
                    .await?;
                trace!("fetched topics {:?}", topics);

                let mut matched_topics = all_topics
                    .into_iter()
                    .filter(|t| regex.is_match(t))
                    .collect();

                trace!("matched topics {:?} (regex: {})", matched_topics, &regex);

                topics.append(&mut matched_topics);
            }

            // 2. lookup partitioned topic
            let topics = try_join_all(
                topics
                    .into_iter()
                    .map(|topic| pulsar.lookup_partitioned_topic(topic)),
            )
            .await?
            .into_iter()
            .flatten();

            // 3. create consumers
            let consumers = try_join_all(
                topics
                    .into_iter()
                    .filter(|(t, _)| !existing_topics.contains(t))
                    .map(|(topic, addr)| {
                        TopicConsumer::new(pulsar.clone(), topic, addr, consumer_config.clone())
                    }),
            )
            .await?;
            trace!("created {} consumers", consumers.len());
            Ok(consumers)
        }));
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.ack(msg).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.cumulative_ack(msg).await
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        if let Some(c) = self.consumers.get_mut(&msg.topic) {
            c.nack(msg).await?;
            Ok(())
        } else {
            Err(ConnectionError::Unexpected(format!("no consumer for topic {}", msg.topic)).into())
        }
    }

    /// Assume that this seek method will call seek for the topics given in the consumer_ids
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek(
        &mut self,
        consumer_ids: Option<Vec<String>>,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        // 0. null or empty vector
        match consumer_ids {
            Some(consumer_ids) => {
                // 1, select consumers
                let mut actions = Vec::default();
                for (consumer_id, consumer) in self.consumers.iter_mut() {
                    if consumer_ids.contains(consumer_id) {
                        actions.push(consumer.seek(message_id.clone(), timestamp));
                    }
                }
                // 2 join all the futures
                let mut v = futures::future::join_all(actions).await;

                for res in v.drain(..) {
                    res?;
                }

                Ok(())
            }
            None => Err(ConnectionError::Unexpected(format!(
                "no consumer for consumer ids {:?}",
                consumer_ids
            ))
            .into()),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }
}

impl<T: DeserializeMessage, Exe: Executor> Debug for MultiTopicConsumer<T, Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MultiTopicConsumer({:?}, {:?})",
            &self.namespace, &self.topic_regex
        )
    }
}

impl<T: 'static + DeserializeMessage, Exe: Executor> Stream for MultiTopicConsumer<T, Exe> {
    type Item = Result<Message<T>, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(mut new_consumers) = self.new_consumers.take() {
            match new_consumers.as_mut().poll(cx) {
                Poll::Ready(Ok(new_consumers)) => {
                    self.add_consumers(new_consumers);
                }
                Poll::Pending => {
                    self.new_consumers = Some(new_consumers);
                }
                Poll::Ready(Err(e)) => {
                    error!("Error creating pulsar consumers: {}", e);
                    // don't return error here; could be intermittent connection
                    // failure and we want to retry
                }
            }
        }

        if let Poll::Ready(Some(_)) = self.refresh.as_mut().poll_next(cx) {
            self.update_topics();
            return self.poll_next(cx);
        }

        let mut topics_to_remove = Vec::new();
        let mut result = None;
        for _ in 0..self.existing_topics.len() {
            if result.is_some() {
                break;
            }
            let topic = self.existing_topics.pop_front().unwrap();
            if let Some(item) = self
                .consumers
                .get_mut(&topic)
                .map(|c| c.as_mut().poll_next(cx))
            {
                match item {
                    Poll::Pending => {}
                    Poll::Ready(Some(Ok(msg))) => result = Some(msg),
                    Poll::Ready(None) => {
                        error!("Unexpected end of stream for pulsar topic {}", &topic);
                        topics_to_remove.push(topic.clone());
                    }
                    Poll::Ready(Some(Err(e))) => {
                        error!(
                            "Unexpected error consuming from pulsar topic {}: {}",
                            &topic, e
                        );
                        topics_to_remove.push(topic.clone());
                    }
                }
            } else {
                eprintln!("BUG: Missing consumer for topic {}", &topic);
            }
            self.existing_topics.push_back(topic);
        }
        self.remove_consumers(&topics_to_remove);
        if let Some(result) = result {
            return Poll::Ready(Some(Ok(result)));
        }

        Poll::Pending
    }
}
