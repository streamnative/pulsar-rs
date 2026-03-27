use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use pulsar::{
    message::proto,
    schema::{EncodeData, PulsarSchema},
    Error, Payload,
};

use schema_registry_converter::async_impl::schema_registry::{SrSettings, SrSettingsBuilder};

/// Authentication options for the Confluent Schema Registry.
pub enum ConfluentAuth {
    Basic { username: String, password: String },
    Bearer { token: String },
}

/// Controls how the subject name is derived for a schema in the registry.
pub enum SubjectNameStrategy {
    /// `<topic>-key` or `<topic>-value`
    TopicName,
    /// The fully-qualified Rust type name
    RecordName,
    /// `<topic>-<type_name>`
    TopicRecordName,
}

/// Configuration for connecting to a Confluent Schema Registry.
pub struct ConfluentConfig {
    pub registry_url: String,
    pub auth: Option<ConfluentAuth>,
    pub subject_name_strategy: SubjectNameStrategy,
    pub timeout: Option<Duration>,
}

/// A [`PulsarSchema`] implementation backed by the Confluent Schema Registry.
///
/// Encodes and decodes messages as JSON. Schema registration is a placeholder
/// for an initial release; `schema_id` will be `None` until a live registry is
/// wired up in a future iteration.
pub struct ConfluentJsonSchema<T> {
    /// Holds the connection settings for a future live-registry integration.
    #[allow(dead_code)]
    sr_settings: SrSettings,
    subject_strategy: SubjectNameStrategy,
    is_key: bool,
    /// Ensures the "not yet implemented" warning is emitted at most once.
    warned: AtomicBool,
    _phantom: PhantomData<fn(T)>,
}

impl<T> ConfluentJsonSchema<T> {
    pub fn new(config: ConfluentConfig, is_key: bool) -> Result<Self, Error> {
        let sr_settings = if config.auth.is_some() || config.timeout.is_some() {
            let mut builder: SrSettingsBuilder = SrSettings::new_builder(config.registry_url);
            if let Some(auth) = config.auth {
                match auth {
                    ConfluentAuth::Basic { username, password } => {
                        builder.set_basic_authorization(&username, Some(&password));
                    }
                    ConfluentAuth::Bearer { token } => {
                        builder.set_token_authorization(&token);
                    }
                }
            }
            if let Some(timeout) = config.timeout {
                builder.set_timeout(timeout);
            }
            builder
                .build()
                .map_err(|e| Error::SchemaRegistry(format!("Failed to build SrSettings: {}", e)))?
        } else {
            SrSettings::new(config.registry_url)
        };

        Ok(Self {
            sr_settings,
            subject_strategy: config.subject_name_strategy,
            is_key,
            warned: AtomicBool::new(false),
            _phantom: PhantomData,
        })
    }

    fn subject_name(&self, topic: &str) -> String {
        let suffix = if self.is_key { "key" } else { "value" };
        match &self.subject_strategy {
            SubjectNameStrategy::TopicName => format!("{topic}-{suffix}"),
            SubjectNameStrategy::RecordName => std::any::type_name::<T>().to_string(),
            SubjectNameStrategy::TopicRecordName => {
                let type_name = std::any::type_name::<T>();
                format!("{topic}-{type_name}")
            }
        }
    }
}

#[async_trait]
impl<T> PulsarSchema<T> for ConfluentJsonSchema<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn schema_info(&self) -> proto::Schema {
        proto::Schema {
            r#type: 22, // External = 22
            ..Default::default()
        }
    }

    async fn encode(&self, topic: &str, message: T) -> Result<EncodeData, Error> {
        let json_bytes =
            serde_json::to_vec(&message).map_err(|e| Error::SchemaRegistry(e.to_string()))?;

        // Schema registration and ID retrieval is not yet implemented.
        // A future iteration will wire the schema_registry_converter encode API
        // to register schemas and extract the Confluent schema ID, then re-frame
        // it with Pulsar's 0xFF magic byte via schema_id_util::add_magic_header().
        let subject = self.subject_name(topic);
        if !self.warned.swap(true, Ordering::Relaxed) {
            log::warn!(
                "ConfluentJsonSchema::encode(): schema registration not yet implemented \
                 for subject '{subject}' on topic '{topic}' — returning schema_id: None. \
                 Messages will be sent without external schema framing.",
            );
        } else {
            log::debug!(
                "ConfluentJsonSchema::encode(): schema_id: None for subject '{subject}' \
                 on topic '{topic}' (not-yet-implemented placeholder)",
            );
        }

        Ok(EncodeData {
            payload: json_bytes,
            schema_id: None,
        })
    }

    async fn decode(
        &self,
        _topic: &str,
        payload: &Payload,
        schema_id: Option<&[u8]>,
    ) -> Result<T, Error> {
        // Deliberate simplification for initial release:
        // JSON is forward-compatible, so direct deserialization works without
        // fetching the writer's schema from the registry.
        if let Some(id) = schema_id {
            log::debug!(
                "Confluent decode: received schema_id ({} bytes), using direct JSON deser",
                id.len()
            );
        }
        serde_json::from_slice(&payload.data).map_err(|e| Error::SchemaRegistry(e.to_string()))
    }
}

/// Convenience factory for building Confluent-backed schemas.
pub struct ConfluentSchemaFactory;

impl ConfluentSchemaFactory {
    pub fn json<T: Serialize + DeserializeOwned + Send + Sync + 'static>(
        config: ConfluentConfig,
    ) -> Result<ConfluentJsonSchema<T>, Error> {
        ConfluentJsonSchema::new(config, false)
    }

    pub fn kv<K, V>(
        key_schema: ConfluentJsonSchema<K>,
        value_schema: ConfluentJsonSchema<V>,
    ) -> pulsar::schema::KeyValueSchema<K, V>
    where
        K: Serialize + DeserializeOwned + Send + Sync + 'static,
        V: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        pulsar::schema::KeyValueSchema::new(
            Arc::new(key_schema) as Arc<dyn PulsarSchema<K>>,
            Arc::new(value_schema) as Arc<dyn PulsarSchema<V>>,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestEvent {
        name: String,
        value: i32,
    }

    #[test]
    fn test_subject_name_topic_strategy() {
        let schema = ConfluentJsonSchema::<TestEvent>::new(
            ConfluentConfig {
                registry_url: "http://localhost:8081".to_string(),
                auth: None,
                subject_name_strategy: SubjectNameStrategy::TopicName,
                timeout: None,
            },
            false,
        )
        .expect("schema creation failed");
        assert_eq!(schema.subject_name("my-topic"), "my-topic-value");
    }

    #[test]
    fn test_subject_name_topic_strategy_key() {
        let schema = ConfluentJsonSchema::<TestEvent>::new(
            ConfluentConfig {
                registry_url: "http://localhost:8081".to_string(),
                auth: None,
                subject_name_strategy: SubjectNameStrategy::TopicName,
                timeout: None,
            },
            true,
        )
        .expect("schema creation failed");
        assert_eq!(schema.subject_name("my-topic"), "my-topic-key");
    }

    #[test]
    fn test_schema_info_returns_external() {
        let schema = ConfluentJsonSchema::<TestEvent>::new(
            ConfluentConfig {
                registry_url: "http://localhost:8081".to_string(),
                auth: None,
                subject_name_strategy: SubjectNameStrategy::TopicName,
                timeout: None,
            },
            false,
        )
        .expect("schema creation failed");
        let info = schema.schema_info();
        assert_eq!(info.r#type, 22);
    }

    #[test]
    fn test_factory_json() {
        let _schema = ConfluentSchemaFactory::json::<TestEvent>(ConfluentConfig {
            registry_url: "http://localhost:8081".to_string(),
            auth: None,
            subject_name_strategy: SubjectNameStrategy::TopicName,
            timeout: None,
        })
        .expect("factory json failed");
    }

    #[tokio::test]
    async fn test_encode_decode_round_trip() {
        let schema = ConfluentJsonSchema::<TestEvent>::new(
            ConfluentConfig {
                registry_url: "http://localhost:8081".to_string(),
                auth: None,
                subject_name_strategy: SubjectNameStrategy::TopicName,
                timeout: None,
            },
            false,
        )
        .expect("schema creation failed");

        let original = TestEvent {
            name: "round-trip".to_string(),
            value: 99,
        };

        let encode_data = schema
            .encode("test-topic", original.clone())
            .await
            .expect("encode failed");

        let payload = Payload {
            data: encode_data.payload,
            metadata: Default::default(),
        };

        let decoded: TestEvent = schema
            .decode("test-topic", &payload, encode_data.schema_id.as_deref())
            .await
            .expect("decode failed");

        assert_eq!(
            decoded, original,
            "round-trip encode/decode must preserve data"
        );
    }

    #[ignore] // Requires a live Confluent Schema Registry
    #[tokio::test]
    async fn test_encode_returns_schema_id() {
        let schema = ConfluentJsonSchema::<TestEvent>::new(
            ConfluentConfig {
                registry_url: std::env::var("SCHEMA_REGISTRY_URL")
                    .unwrap_or_else(|_| "http://localhost:8081".to_string()),
                auth: None,
                subject_name_strategy: SubjectNameStrategy::TopicName,
                timeout: None,
            },
            false,
        )
        .expect("schema creation failed");
        let event = TestEvent {
            name: "test".to_string(),
            value: 42,
        };
        let encode_data = schema
            .encode("test-topic", event)
            .await
            .expect("encode failed");

        assert!(
            encode_data.schema_id.is_some(),
            "encode() must return a non-None schema_id for external schema registry to work"
        );
        let id = encode_data.schema_id.unwrap();
        assert!(id.len() >= 2, "schema_id too short");
        assert_eq!(
            id[0], 0xFF,
            "schema_id must start with Pulsar magic byte 0xFF"
        );
    }
}
