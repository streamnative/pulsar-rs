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
    pub(super) _phantom: PhantomData<T>,
}

impl<T> Message<T> {
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
