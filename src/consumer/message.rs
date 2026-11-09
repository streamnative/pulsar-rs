use std::marker::PhantomData;

use base64::Engine;

use crate::{
    consumer::data::MessageData,
    message::proto::{MessageIdData, MessageMetadata},
    DeserializeMessage, Error, Payload,
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
    pub fn new(topic: &str, message_id: MessageData, payload: Payload) -> Self {
        Message {
            topic: topic.to_string(),
            message_id,
            payload,
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

    /// Get message key (partition key string as stored in metadata)
    ///
    /// When [`has_base64_encoded_key`](Self::has_base64_encoded_key) is true, this is the
    /// Base64 encoding of the raw key bytes.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn key(&self) -> Option<String> {
        self.payload.metadata.partition_key.clone()
    }

    /// Whether the partition key is a Base64 encoding of raw key bytes.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn has_base64_encoded_key(&self) -> bool {
        self.payload.metadata.partition_key_b64_encoded()
    }

    /// Get the raw partition key bytes.
    ///
    /// If the key is Base64-encoded, it is decoded; otherwise the UTF-8 bytes of
    /// the key string are returned. Returns `Ok(None)` when there is no key.
    ///
    /// Returns an error when the key is marked Base64-encoded but decoding fails.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn key_bytes(&self) -> Result<Option<Vec<u8>>, Error> {
        match self.payload.metadata.partition_key.as_ref() {
            None => Ok(None),
            Some(key) if !self.has_base64_encoded_key() => Ok(Some(key.as_bytes().to_vec())),
            Some(key) => match base64::engine::general_purpose::STANDARD.decode(key) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(e) => Err(Error::Custom(format!(
                    "failed to decode Base64 partition key on topic {} message_id={:?}: {e}",
                    self.topic,
                    self.message_id()
                ))),
            },
        }
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
    use crate::consumer::data::MessageData;
    use crate::message::proto::MessageIdData;

    fn message_with_key(partition_key: Option<String>, b64: Option<bool>) -> Message<()> {
        Message::new(
            "persistent://public/default/topic",
            MessageData {
                id: MessageIdData {
                    ledger_id: 1,
                    entry_id: 2,
                    ..Default::default()
                },
                batch_size: None,
            },
            Payload {
                metadata: MessageMetadata {
                    producer_name: "test".into(),
                    sequence_id: 0,
                    publish_time: 0,
                    partition_key,
                    partition_key_b64_encoded: b64,
                    ..Default::default()
                },
                data: vec![],
            },
        )
    }

    #[test]
    fn key_bytes_decodes_when_flag_set() {
        let raw = b"\x00\x01binary";
        let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
        let msg = message_with_key(Some(encoded), Some(true));
        assert!(msg.has_base64_encoded_key());
        assert_eq!(msg.key_bytes().unwrap().as_deref(), Some(raw.as_slice()));
    }

    #[test]
    fn key_bytes_uses_utf8_when_flag_unset() {
        let msg = message_with_key(Some("plain-key".into()), Some(false));
        assert!(!msg.has_base64_encoded_key());
        assert_eq!(
            msg.key_bytes().unwrap().as_deref(),
            Some(b"plain-key".as_slice())
        );
    }

    #[test]
    fn key_bytes_none_without_key() {
        let msg = message_with_key(None, None);
        assert!(!msg.has_base64_encoded_key());
        assert!(msg.key_bytes().unwrap().is_none());
    }

    #[test]
    fn key_bytes_errors_on_invalid_base64() {
        let msg = message_with_key(Some("not!!valid".into()), Some(true));
        assert!(msg.has_base64_encoded_key());
        let err = msg.key_bytes().unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("persistent://public/default/topic"),
            "error should include topic: {err_msg}"
        );
        assert!(
            err_msg.contains("message_id="),
            "error should include message id: {err_msg}"
        );
    }
}
