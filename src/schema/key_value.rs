use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    message::proto,
    schema::{schema_id_util, EncodeData, PulsarSchema},
    Error, Payload,
};

use super::schema_id_util::SchemaIdInfo;

/// Composes two inner schemas for key-value message types.
pub struct KeyValueSchema<K, V> {
    key_schema: Arc<dyn PulsarSchema<K>>,
    value_schema: Arc<dyn PulsarSchema<V>>,
}

impl<K, V> KeyValueSchema<K, V> {
    pub fn new(
        key_schema: Arc<dyn PulsarSchema<K>>,
        value_schema: Arc<dyn PulsarSchema<V>>,
    ) -> Self {
        Self {
            key_schema,
            value_schema,
        }
    }
}

/// Combine key and value payloads into a single byte vector.
/// Format: [4-byte key_len BE] [key_bytes] [value_bytes]
fn combine_kv_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let key_len = key.len() as u32;
    let mut result = Vec::with_capacity(4 + key.len() + value.len());
    result.extend_from_slice(&key_len.to_be_bytes());
    result.extend_from_slice(key);
    result.extend_from_slice(value);
    result
}

/// Split a combined key-value payload into key and value Payloads.
fn split_kv_payload(payload: &Payload) -> Result<(Payload, Payload), Error> {
    if payload.data.len() < 4 {
        return Err(Error::Custom(
            "KeyValue payload too short to contain key length".to_string(),
        ));
    }
    let key_len = u32::from_be_bytes([
        payload.data[0],
        payload.data[1],
        payload.data[2],
        payload.data[3],
    ]) as usize;
    if payload.data.len() < 4 + key_len {
        return Err(Error::Custom(
            "KeyValue payload too short for declared key length".to_string(),
        ));
    }
    let key_data = payload.data[4..4 + key_len].to_vec();
    let val_data = payload.data[4 + key_len..].to_vec();
    Ok((
        Payload {
            metadata: payload.metadata.clone(),
            data: key_data,
        },
        Payload {
            metadata: payload.metadata.clone(),
            data: val_data,
        },
    ))
}

#[async_trait]
impl<K, V> PulsarSchema<(K, V)> for KeyValueSchema<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    fn schema_info(&self) -> proto::Schema {
        proto::Schema {
            r#type: proto::schema::Type::KeyValue as i32,
            ..Default::default()
        }
    }

    async fn encode(&self, topic: &str, message: (K, V)) -> Result<EncodeData, Error> {
        let (key, value) = message;
        let key_data = self.key_schema.encode(topic, key).await?;
        let value_data = self.value_schema.encode(topic, value).await?;

        // Inner schemas return schema IDs framed with a 0xFF magic prefix.
        // Strip that prefix before KV-framing to avoid double-framing — the KV
        // envelope (0xFE + length-delimited) is the only framing on the wire.
        // On decode, strip_magic_header extracts raw inner IDs which are passed
        // directly to inner schemas' decode(), completing the round-trip.
        let schema_id = schema_id_util::generate_kv_schema_id(
            key_data
                .schema_id
                .as_deref()
                .map(schema_id_util::strip_single_magic_prefix),
            value_data
                .schema_id
                .as_deref()
                .map(schema_id_util::strip_single_magic_prefix),
        );
        Ok(EncodeData {
            payload: combine_kv_payload(&key_data.payload, &value_data.payload),
            schema_id,
        })
    }

    async fn decode(
        &self,
        topic: &str,
        payload: &Payload,
        schema_id: Option<&[u8]>,
    ) -> Result<(K, V), Error> {
        let (key_id, value_id) = match schema_id {
            Some(data) => match schema_id_util::strip_magic_header(data)? {
                Some(SchemaIdInfo::KeyValue { key_id, value_id }) => (Some(key_id), Some(value_id)),
                Some(SchemaIdInfo::Single(_)) => {
                    return Err(Error::Custom(format!(
                        "KV decode received Single (0xFF) schema_id framing on topic \
                         {topic} — expected KeyValue (0xFE) framing. This indicates a \
                         protocol or producer configuration mismatch."
                    )));
                }
                None => (None, None),
            },
            None => (None, None),
        };
        let (key_payload, value_payload) = split_kv_payload(payload)?;
        let key = self
            .key_schema
            .decode(topic, &key_payload, key_id.as_deref())
            .await?;
        let value = self
            .value_schema
            .decode(topic, &value_payload, value_id.as_deref())
            .await?;
        Ok((key, value))
    }

    async fn close(&self) -> Result<(), Error> {
        self.key_schema.close().await?;
        self.value_schema.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::schema_id_util;

    /// Simple mock schema for testing.
    struct MockSchema {
        schema_id: Option<Vec<u8>>,
    }

    #[async_trait]
    impl PulsarSchema<String> for MockSchema {
        fn schema_info(&self) -> proto::Schema {
            proto::Schema::default()
        }

        async fn encode(&self, _topic: &str, message: String) -> Result<EncodeData, Error> {
            Ok(EncodeData {
                payload: message.into_bytes(),
                schema_id: self.schema_id.clone(),
            })
        }

        async fn decode(
            &self,
            _topic: &str,
            payload: &Payload,
            _schema_id: Option<&[u8]>,
        ) -> Result<String, Error> {
            String::from_utf8(payload.data.clone()).map_err(|e| Error::Custom(e.to_string()))
        }
    }

    #[tokio::test]
    async fn test_kv_encode_decode_roundtrip() {
        let kv_schema = KeyValueSchema::new(
            Arc::new(MockSchema { schema_id: None }),
            Arc::new(MockSchema { schema_id: None }),
        );

        let encoded = kv_schema
            .encode("topic", ("key".to_string(), "value".to_string()))
            .await
            .unwrap();

        assert!(encoded.schema_id.is_none());

        let payload = Payload {
            metadata: proto::MessageMetadata::default(),
            data: encoded.payload,
        };
        let (k, v) = kv_schema.decode("topic", &payload, None).await.unwrap();
        assert_eq!(k, "key");
        assert_eq!(v, "value");
    }

    #[tokio::test]
    async fn test_kv_encode_with_schema_ids() {
        let kv_schema = KeyValueSchema::new(
            Arc::new(MockSchema {
                schema_id: Some(schema_id_util::add_magic_header(&[0x01])),
            }),
            Arc::new(MockSchema {
                schema_id: Some(schema_id_util::add_magic_header(&[0x02])),
            }),
        );

        let encoded = kv_schema
            .encode("topic", ("key".to_string(), "value".to_string()))
            .await
            .unwrap();

        let framed = encoded.schema_id.unwrap();
        assert_eq!(framed[0], schema_id_util::MAGIC_BYTE_KEY_VALUE);

        // Verify inner IDs are stored WITHOUT the 0xFF magic prefix (no double-framing).
        // The KV frame should contain raw inner IDs: [0xFE, key_len(4), 0x01, 0x02]
        let info = schema_id_util::strip_magic_header(&framed)
            .unwrap()
            .unwrap();
        match info {
            schema_id_util::SchemaIdInfo::KeyValue { key_id, value_id } => {
                // Raw inner IDs — no 0xFF prefix
                assert_eq!(
                    key_id,
                    vec![0x01],
                    "key_id should be raw, without 0xFF prefix"
                );
                assert_eq!(
                    value_id,
                    vec![0x02],
                    "value_id should be raw, without 0xFF prefix"
                );
            }
            other => panic!("expected KeyValue, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_kv_encode_decode_roundtrip_with_schema_ids() {
        // Verifies that inner schema IDs survive the encode→decode round-trip
        // without double-framing. The recording mock captures the schema_id
        // received by decode() so we can assert it matches the raw (unframed) ID.
        use std::sync::Mutex;

        struct RecordingSchema {
            encode_id: Option<Vec<u8>>,
            decoded_ids: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
        }

        #[async_trait]
        impl PulsarSchema<String> for RecordingSchema {
            fn schema_info(&self) -> proto::Schema {
                proto::Schema::default()
            }
            async fn encode(&self, _topic: &str, msg: String) -> Result<EncodeData, Error> {
                Ok(EncodeData {
                    payload: msg.into_bytes(),
                    schema_id: self.encode_id.clone(),
                })
            }
            async fn decode(
                &self,
                _topic: &str,
                payload: &Payload,
                schema_id: Option<&[u8]>,
            ) -> Result<String, Error> {
                self.decoded_ids
                    .lock()
                    .unwrap()
                    .push(schema_id.map(|s| s.to_vec()));
                String::from_utf8(payload.data.clone()).map_err(|e| Error::Custom(e.to_string()))
            }
        }

        let key_decoded = Arc::new(Mutex::new(Vec::new()));
        let val_decoded = Arc::new(Mutex::new(Vec::new()));

        let kv_schema = KeyValueSchema::new(
            Arc::new(RecordingSchema {
                encode_id: Some(schema_id_util::add_magic_header(&[0x00, 0x01])),
                decoded_ids: key_decoded.clone(),
            }),
            Arc::new(RecordingSchema {
                encode_id: Some(schema_id_util::add_magic_header(&[0x00, 0x02])),
                decoded_ids: val_decoded.clone(),
            }),
        );

        let encoded = kv_schema
            .encode("t", ("k".to_string(), "v".to_string()))
            .await
            .unwrap();

        let payload = Payload {
            metadata: proto::MessageMetadata::default(),
            data: encoded.payload,
        };
        let (k, v) = kv_schema
            .decode("t", &payload, encoded.schema_id.as_deref())
            .await
            .unwrap();
        assert_eq!(k, "k");
        assert_eq!(v, "v");

        // Inner decode() should receive raw IDs (without 0xFF prefix)
        let key_ids = key_decoded.lock().unwrap();
        assert_eq!(key_ids.len(), 1);
        assert_eq!(
            key_ids[0].as_deref(),
            Some([0x00, 0x01].as_slice()),
            "inner key decode should receive raw ID without 0xFF prefix"
        );

        let val_ids = val_decoded.lock().unwrap();
        assert_eq!(val_ids.len(), 1);
        assert_eq!(
            val_ids[0].as_deref(),
            Some([0x00, 0x02].as_slice()),
            "inner value decode should receive raw ID without 0xFF prefix"
        );
    }

    #[tokio::test]
    async fn test_kv_schema_info() {
        let kv_schema = KeyValueSchema::new(
            Arc::new(MockSchema { schema_id: None }) as Arc<dyn PulsarSchema<String>>,
            Arc::new(MockSchema { schema_id: None }) as Arc<dyn PulsarSchema<String>>,
        );

        let info = kv_schema.schema_info();
        assert_eq!(info.r#type, proto::schema::Type::KeyValue as i32);
    }

    #[tokio::test]
    async fn test_kv_decode_rejects_single_framed_schema_id() {
        let kv_schema = KeyValueSchema::new(
            Arc::new(MockSchema { schema_id: None }),
            Arc::new(MockSchema { schema_id: None }),
        );

        // Build a valid KV payload
        let payload = Payload {
            metadata: proto::MessageMetadata::default(),
            data: combine_kv_payload(b"key", b"value"),
        };

        // Pass a Single-framed schema_id (0xFF prefix) — should be an error,
        // not silently degraded to (None, None).
        let single_framed = schema_id_util::add_magic_header(&[0x00, 0x01]);
        let result = kv_schema
            .decode("topic", &payload, Some(&single_framed))
            .await;
        assert!(
            result.is_err(),
            "KV decode with Single framing should error"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Single (0xFF)"),
            "error should mention Single framing, got: {err_msg}"
        );
    }
}
