use std::marker::PhantomData;

use crate::{
    consumer::data::MessageData,
    message::proto::{MessageIdData, MessageMetadata},
    DeserializeMessage, Payload,
};

/// a message received by a consumer
///
/// it is generic over the type it can be deserialized to
#[derive(Debug)]
pub struct Message<T> {
    /// origin topic of the message
    pub topic: String,
    /// contains the message's data and other metadata
    pub payload: Payload,
    /// contains the message's id and batch size data
    pub message_id: MessageData,
    redelivery_count: u32,
    pub(super) _phantom: PhantomData<T>,
}

impl<T> Message<T> {
    pub fn new(topic: &str, message_id: MessageData, payload: Payload) -> Self {
        Self::new_with_redelivery_count(topic, message_id, payload, 0)
    }

    pub(crate) fn new_with_redelivery_count(
        topic: &str,
        message_id: MessageData,
        payload: Payload,
        redelivery_count: u32,
    ) -> Self {
        Message {
            topic: topic.to_string(),
            message_id,
            payload,
            redelivery_count,
            _phantom: PhantomData,
        }
    }

    /// Pulsar metadata for the message
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn metadata(&self) -> &MessageMetadata {
        &self.payload.metadata
    }

    /// Get Pulsar message id for the message
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn message_id(&self) -> &MessageIdData {
        &self.message_id.id
    }

    /// Returns the broker-provided redelivery count for this message. Returns `0` when the broker
    /// did not supply a redelivery count (first delivery or broker omitted the field), or when the
    /// message was constructed directly with [`Message::new`].
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn redelivery_count(&self) -> u32 {
        self.redelivery_count
    }

    /// Get message key (partition key)
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn key(&self) -> Option<String> {
        self.payload.metadata.partition_key.clone()
    }
}

impl<T: DeserializeMessage> Message<T> {
    /// directly deserialize a message
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn deserialize(&self) -> T::Output {
        T::deserialize_message(&self.payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::proto;

    fn message_id() -> proto::MessageIdData {
        proto::MessageIdData {
            ledger_id: 1,
            entry_id: 1,
            partition: None,
            batch_index: None,
            ack_set: vec![],
            batch_size: None,
            first_chunk_message_id: None,
        }
    }

    fn payload() -> Payload {
        Payload {
            metadata: proto::MessageMetadata::default(),
            data: vec![],
        }
    }

    #[test]
    fn test_redelivery_count_returns_broker_value() {
        let message_id = MessageData {
            id: message_id(),
            batch_size: None,
        };
        let msg = Message::<()>::new_with_redelivery_count("test-topic", message_id, payload(), 7);

        assert_eq!(msg.redelivery_count(), 7);
    }

    #[test]
    fn test_redelivery_count_defaults_to_zero() {
        let message_id = MessageData {
            id: message_id(),
            batch_size: None,
        };
        let msg = Message::<()>::new("test-topic", message_id, payload());

        assert_eq!(msg.redelivery_count(), 0);
    }
}
