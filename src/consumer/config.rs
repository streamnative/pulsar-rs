use std::{sync::Arc, time::Duration};

use crate::{
    consumer::{
        data::DeadLetterPolicy, negative_ack_backoff::NegativeAckBackoff, options::ConsumerOptions,
    },
    message::proto::command_subscribe::SubType,
};

/// the complete configuration of a consumer
#[derive(Debug, Clone, Default)]
pub struct ConsumerConfig {
    /// subscription name
    pub(crate) subscription: String,
    /// subscription type
    ///
    /// default: Shared
    pub(crate) sub_type: SubType,
    /// maximum size for batched messages
    ///
    /// default: 1000
    pub(crate) batch_size: Option<u32>,
    /// name of the consumer
    pub(crate) consumer_name: Option<String>,
    /// numerical id of the consumer
    pub(crate) consumer_id: Option<u64>,
    /// time after which unacked messages will be sent again
    pub(crate) unacked_message_redelivery_delay: Option<Duration>,
    /// consumer options
    pub(crate) options: ConsumerOptions,
    /// dead letter policy
    pub(crate) dead_letter_policy: Option<DeadLetterPolicy>,
    /// fixed negative-ack redelivery delay
    pub(crate) nack_redelivery_delay: Option<Duration>,
    /// negative-ack redelivery backoff policy
    pub(crate) negative_ack_backoff: Option<Arc<dyn NegativeAckBackoff + Send + Sync>>,
}

impl ConsumerConfig {
    /// Returns the exact configuration handed to a per-topic consumer engine.
    ///
    /// Keeping this as a named seam makes cross-flavor tests assert the same value that
    /// `TopicConsumer::new` receives from single-topic, partitioned, multi-topic, regex-refresh, and
    /// reader construction paths instead of only asserting arbitrary `ConsumerConfig` clones.
    pub(crate) fn clone_for_topic_consumer(&self) -> Self {
        self.clone()
    }
}
