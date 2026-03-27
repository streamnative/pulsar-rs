use crate::{
    consumer::data::MessageData,
    message::proto::{MessageIdData, MessageMetadata},
    DeserializeMessage, Payload,
};

/// a message received by a consumer
///
/// it is generic over the type it can be deserialized to
pub struct Message<T> {
    /// origin topic of the message
    pub topic: String,
    /// contains the message's data and other metadata
    pub payload: Payload,
    /// contains the message's id and batch size data
    pub message_id: MessageData,
    /// Pre-decoded value from PulsarSchema. None when using DeserializeMessage path
    /// or when schema decode failed (check [`decode_error`](Self::decode_error)).
    pub(super) decoded: Option<T>,
    /// If schema decode failed, contains the error description.
    /// `None` when no schema is attached or when decoding succeeded.
    pub(super) decode_error: Option<String>,
}

// Manual Debug impl — avoids requiring T: Debug
impl<T> std::fmt::Debug for Message<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("topic", &self.topic)
            .field("payload", &self.payload)
            .field("message_id", &self.message_id)
            .field("has_decoded", &self.decoded.is_some())
            .field("decode_error", &self.decode_error)
            .finish()
    }
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

    /// Returns the pre-decoded value if this message was decoded via PulsarSchema.
    ///
    /// Returns `None` when:
    /// - Using the `DeserializeMessage` path (no schema attached), or
    /// - Schema decode failed — check [`decode_error()`](Self::decode_error) for details.
    pub fn value(&self) -> Option<&T> {
        self.decoded.as_ref()
    }

    /// If schema decode failed for this message, returns the error description.
    ///
    /// Returns `None` when no schema is attached or when decoding succeeded.
    pub fn decode_error(&self) -> Option<&str> {
        self.decode_error.as_deref()
    }
}

impl<T: DeserializeMessage> Message<T> {
    /// directly deserialize a message
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn deserialize(&self) -> T::Output {
        T::deserialize_message(&self.payload)
    }
}
