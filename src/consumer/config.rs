use std::{fmt, sync::Arc, time::Duration};

use crate::{
    consumer::{
        data::DeadLetterPolicy, negative_ack_backoff::NegativeAckBackoff, options::ConsumerOptions,
    },
    message::proto::command_subscribe::SubType,
};

/// the complete configuration of a consumer
#[derive(Clone, Default)]
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
    pub(crate) negative_ack_backoff: Option<Arc<dyn NegativeAckBackoff>>,
}

impl fmt::Debug for ConsumerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerConfig")
            .field("subscription", &self.subscription)
            .field("sub_type", &self.sub_type)
            .field("batch_size", &self.batch_size)
            .field("consumer_name", &self.consumer_name)
            .field("consumer_id", &self.consumer_id)
            .field(
                "unacked_message_redelivery_delay",
                &self.unacked_message_redelivery_delay,
            )
            .field("options", &self.options)
            .field("dead_letter_policy", &self.dead_letter_policy)
            .field("nack_redelivery_delay", &self.nack_redelivery_delay)
            .field(
                "negative_ack_backoff",
                &self.negative_ack_backoff.as_ref().map(|_| "configured"),
            )
            .finish()
    }
}
