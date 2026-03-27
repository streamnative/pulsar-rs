use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{
    client::{DeserializeMessage, SerializeMessage},
    message::proto,
    schema::{EncodeData, PulsarSchema},
    Error, Payload,
};

/// Wraps existing `SerializeMessage`/`DeserializeMessage` implementations
/// into the `PulsarSchema<T>` trait for backward compatibility.
pub struct DefaultPulsarSchema<T> {
    schema_info: proto::Schema,
    _phantom: PhantomData<fn(T)>,
}

impl<T> DefaultPulsarSchema<T> {
    pub fn new(schema_info: proto::Schema) -> Self {
        Self {
            schema_info,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T, E> PulsarSchema<T> for DefaultPulsarSchema<T>
where
    T: SerializeMessage + DeserializeMessage<Output = Result<T, E>> + Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    fn schema_info(&self) -> proto::Schema {
        self.schema_info.clone()
    }

    async fn encode(&self, _topic: &str, message: T) -> Result<EncodeData, Error> {
        let msg = T::serialize_message(message)?;
        Ok(EncodeData {
            payload: msg.payload,
            schema_id: None,
        })
    }

    async fn decode(
        &self,
        _topic: &str,
        payload: &Payload,
        _schema_id: Option<&[u8]>,
    ) -> Result<T, Error> {
        let result: Result<T, E> = T::deserialize_message(payload);
        result.map_err(|e| Error::Custom(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer;

    // Test type that implements both SerializeMessage and DeserializeMessage
    #[derive(Debug, Clone, PartialEq)]
    struct TestMsg {
        data: String,
    }

    impl SerializeMessage for TestMsg {
        fn serialize_message(input: Self) -> Result<producer::Message, Error> {
            Ok(producer::Message {
                payload: input.data.into_bytes(),
                ..Default::default()
            })
        }
    }

    impl DeserializeMessage for TestMsg {
        type Output = Result<TestMsg, Error>;

        fn deserialize_message(payload: &Payload) -> Self::Output {
            let data = String::from_utf8(payload.data.clone())
                .map_err(|e| Error::Custom(e.to_string()))?;
            Ok(TestMsg { data })
        }
    }

    #[tokio::test]
    async fn test_default_schema_encode() {
        let schema = DefaultPulsarSchema::<TestMsg>::new(proto::Schema {
            r#type: proto::schema::Type::String as i32,
            ..Default::default()
        });

        let msg = TestMsg {
            data: "hello".to_string(),
        };
        let encoded = schema.encode("test-topic", msg).await.unwrap();
        assert_eq!(encoded.payload, b"hello");
        assert!(encoded.schema_id.is_none());
    }

    #[tokio::test]
    async fn test_default_schema_decode() {
        let schema = DefaultPulsarSchema::<TestMsg>::new(proto::Schema::default());

        let payload = Payload {
            metadata: proto::MessageMetadata::default(),
            data: b"world".to_vec(),
        };
        let decoded = schema.decode("test-topic", &payload, None).await.unwrap();
        assert_eq!(decoded.data, "world");
    }

    #[tokio::test]
    async fn test_default_schema_roundtrip() {
        let schema = DefaultPulsarSchema::<TestMsg>::new(proto::Schema::default());

        let original = TestMsg {
            data: "roundtrip".to_string(),
        };
        let encoded = schema.encode("test-topic", original.clone()).await.unwrap();

        let payload = Payload {
            metadata: proto::MessageMetadata::default(),
            data: encoded.payload,
        };
        let decoded = schema.decode("test-topic", &payload, None).await.unwrap();
        assert_eq!(decoded, original);
    }

    #[tokio::test]
    async fn test_default_schema_info() {
        let info = proto::Schema {
            r#type: proto::schema::Type::String as i32,
            name: "test".to_string(),
            ..Default::default()
        };
        let schema = DefaultPulsarSchema::<TestMsg>::new(info.clone());
        assert_eq!(schema.schema_info().r#type, info.r#type);
        assert_eq!(schema.schema_info().name, info.name);
    }
}
