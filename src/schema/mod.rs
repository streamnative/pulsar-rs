pub mod default;
pub mod key_value;
pub mod schema_id_util;

use async_trait::async_trait;

use crate::{message::proto, Error, Payload};

/// Data returned from encoding a message with a PulsarSchema.
#[derive(Debug, Clone)]
pub struct EncodeData {
    /// The serialized message payload.
    pub payload: Vec<u8>,
    /// Schema identifier from an external registry.
    /// Present only for external schemas. Written to MessageMetadata.schema_id
    /// with magic-byte framing.
    pub schema_id: Option<Vec<u8>>,
}

/// Unified schema interface for Pulsar messages.
///
/// Both built-in Pulsar schemas and external schemas (e.g., Confluent Schema
/// Registry) implement this trait. Named `PulsarSchema` to avoid collision
/// with the protobuf `proto::Schema` type.
#[async_trait]
pub trait PulsarSchema<T: Send>: Send + Sync + 'static {
    /// Schema metadata sent to the broker during producer/consumer creation.
    fn schema_info(&self) -> proto::Schema;

    /// Encode a message for the given topic.
    ///
    /// Built-in schemas return `EncodeData` with `schema_id = None`.
    /// External schemas return `EncodeData` with a pre-framed `schema_id`
    /// (magic byte included) ready to write to `MessageMetadata.schema_id`.
    async fn encode(&self, topic: &str, message: T) -> Result<EncodeData, Error>;

    /// Decode a message.
    ///
    /// For built-in schemas, `schema_id` is `None`.
    /// For external schemas, `schema_id` identifies the schema in the registry.
    async fn decode(
        &self,
        topic: &str,
        payload: &Payload,
        schema_id: Option<&[u8]>,
    ) -> Result<T, Error>;

    /// Release resources (connections, caches) when producer/consumer closes.
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

// Re-exports
pub use default::DefaultPulsarSchema;
pub use key_value::KeyValueSchema;
pub use schema_id_util::SchemaIdInfo;
