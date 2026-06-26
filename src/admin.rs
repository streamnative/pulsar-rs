//! Pulsar Admin REST API client.
//!
//! Enabled by the `admin-api` feature flag. Requires a tokio runtime.

use std::{collections::HashMap, fmt::Write as _, sync::Arc, time::Duration};

use futures::lock::Mutex;

use crate::{
    authentication::Authentication,
    connection_manager::TlsOptions,
    error::{AdminError, Error},
    message::proto::{self, Schema},
};

/// Parses a Pulsar topic URL into (scheme, tenant, namespace, topic_name).
/// Accepts `persistent://` and `non-persistent://` prefixes, or a bare
/// `tenant/namespace/topic` string which defaults to `persistent://`.
fn parse_topic(topic: &str) -> Result<(&str, &str, &str, &str), Error> {
    let invalid = || {
        Error::Admin(AdminError::InvalidTopic(format!(
            "expected tenant/namespace/topic or a fully-qualified topic URL, got: {topic}"
        )))
    };

    let (scheme, rest) = if let Some(rest) = topic.strip_prefix("persistent://") {
        ("persistent", rest)
    } else if let Some(rest) = topic.strip_prefix("non-persistent://") {
        ("non-persistent", rest)
    } else {
        ("persistent", topic)
    };

    let mut parts = rest.splitn(3, '/');
    let tenant = parts.next().filter(|s| !s.is_empty()).ok_or_else(invalid)?;
    let namespace = parts.next().filter(|s| !s.is_empty()).ok_or_else(invalid)?;
    let name = parts.next().filter(|s| !s.is_empty()).ok_or_else(invalid)?;

    Ok((scheme, tenant, namespace, name))
}

fn encode_topic_local_name(name: &str) -> String {
    let mut encoded = String::with_capacity(name.len());
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char)
            }
            _ => write!(&mut encoded, "%{byte:02X}").expect("writing to String cannot fail"),
        }
    }
    encoded
}

#[derive(serde::Deserialize)]
struct AdminSchemaResponse {
    #[serde(rename = "type")]
    schema_type: String,
    #[serde(default)]
    data: String,
    #[serde(default)]
    properties: HashMap<String, String>,
}

fn schema_type_from_admin(schema_type: &str) -> Result<proto::schema::Type, Error> {
    match schema_type.to_ascii_uppercase().as_str() {
        "NONE" => Ok(proto::schema::Type::None),
        "STRING" => Ok(proto::schema::Type::String),
        "JSON" => Ok(proto::schema::Type::Json),
        "PROTOBUF" => Ok(proto::schema::Type::Protobuf),
        "AVRO" => Ok(proto::schema::Type::Avro),
        "BOOL" | "BOOLEAN" => Ok(proto::schema::Type::Bool),
        "INT8" => Ok(proto::schema::Type::Int8),
        "INT16" => Ok(proto::schema::Type::Int16),
        "INT32" => Ok(proto::schema::Type::Int32),
        "INT64" => Ok(proto::schema::Type::Int64),
        "FLOAT" => Ok(proto::schema::Type::Float),
        "DOUBLE" => Ok(proto::schema::Type::Double),
        "DATE" => Ok(proto::schema::Type::Date),
        "TIME" => Ok(proto::schema::Type::Time),
        "TIMESTAMP" => Ok(proto::schema::Type::Timestamp),
        "KEYVALUE" | "KEY_VALUE" => Ok(proto::schema::Type::KeyValue),
        "INSTANT" => Ok(proto::schema::Type::Instant),
        "LOCALDATE" | "LOCAL_DATE" => Ok(proto::schema::Type::LocalDate),
        "LOCALTIME" | "LOCAL_TIME" => Ok(proto::schema::Type::LocalTime),
        "LOCALDATETIME" | "LOCAL_DATE_TIME" => Ok(proto::schema::Type::LocalDateTime),
        "PROTOBUFNATIVE" | "PROTOBUF_NATIVE" => Ok(proto::schema::Type::ProtobufNative),
        _ => Err(AdminError::InvalidSchemaType(schema_type.to_string()).into()),
    }
}

fn key_value_schema_part_bytes(value: &serde_json::Value) -> Result<Vec<u8>, Error> {
    if value.as_str() == Some("") {
        return Ok(Vec::new());
    }
    serde_json::to_vec(value).map_err(|e| AdminError::SchemaDecode(e.to_string()).into())
}

fn append_key_value_schema_part(schema_data: &mut Vec<u8>, part: &[u8]) -> Result<(), Error> {
    let len = u32::try_from(part.len()).map_err(|_| {
        AdminError::SchemaDecode("KEY_VALUE schema part is too large to encode".to_string())
    })?;
    schema_data.extend_from_slice(&len.to_be_bytes());
    schema_data.extend_from_slice(part);
    Ok(())
}

fn key_value_schema_data_from_admin(data: &str) -> Result<Vec<u8>, Error> {
    let value: serde_json::Value =
        serde_json::from_str(data).map_err(|e| AdminError::SchemaDecode(e.to_string()))?;
    let object = value.as_object().ok_or_else(|| {
        AdminError::SchemaDecode("KEY_VALUE schema data must be a JSON object".to_string())
    })?;
    let key = object
        .get("key")
        .ok_or_else(|| AdminError::SchemaDecode("KEY_VALUE schema data missing key".to_string()))?;
    let value = object.get("value").ok_or_else(|| {
        AdminError::SchemaDecode("KEY_VALUE schema data missing value".to_string())
    })?;
    let key = key_value_schema_part_bytes(key)?;
    let value = key_value_schema_part_bytes(value)?;
    let mut schema_data = Vec::with_capacity(8 + key.len() + value.len());
    append_key_value_schema_part(&mut schema_data, &key)?;
    append_key_value_schema_part(&mut schema_data, &value)?;
    Ok(schema_data)
}

fn parse_schema_response(body: &str) -> Result<Schema, Error> {
    let response: AdminSchemaResponse =
        serde_json::from_str(body).map_err(|e| AdminError::SchemaDecode(e.to_string()))?;
    let schema_type = schema_type_from_admin(&response.schema_type)?;
    let schema_data = if schema_type == proto::schema::Type::KeyValue {
        key_value_schema_data_from_admin(&response.data)?
    } else {
        response.data.into_bytes()
    };
    Ok(Schema {
        r#type: schema_type as i32,
        schema_data,
        properties: response
            .properties
            .into_iter()
            .map(|(key, value)| proto::KeyValue { key, value })
            .collect(),
        ..Default::default()
    })
}

/// Client for the Pulsar Admin REST API.
///
/// Obtain an instance via [`Pulsar::admin()`][crate::Pulsar::admin].
///
/// # Example
///
/// ```rust,no_run
/// # async fn run(pulsar: pulsar::Pulsar<pulsar::TokioExecutor>) -> Result<(), pulsar::Error> {
/// let admin = pulsar.admin("http://localhost:8080")?;
/// admin
///     .set_max_unacked_messages_per_consumer(
///         "persistent://public/default/my-topic",
///         500,
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct AdminClient {
    client: reqwest::Client,
    admin_url: String,
    auth: Option<Arc<Mutex<Box<dyn Authentication>>>>,
}

impl AdminClient {
    /// Creates a new `AdminClient`.
    ///
    /// Reuses the TLS and authentication configuration already present on the
    /// [`Pulsar`][crate::Pulsar] client. Called internally by
    /// [`Pulsar::admin()`][crate::Pulsar::admin].
    pub(crate) fn new(
        admin_url: String,
        tls_options: &TlsOptions,
        auth: Option<Arc<Mutex<Box<dyn Authentication>>>>,
    ) -> Result<Self, Error> {
        let mut builder = reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(tls_options.allow_insecure_connection);

        builder = builder.danger_accept_invalid_hostnames(
            tls_options.allow_insecure_connection || !tls_options.tls_hostname_verification_enabled,
        );

        if let Some(pem_bytes) = &tls_options.certificate_chain {
            let certs = pem::parse_many(pem_bytes).map_err(|e| {
                Error::Admin(AdminError::TlsConfig(format!(
                    "failed to parse certificate chain: {e}"
                )))
            })?;
            for cert in certs.iter().rev() {
                let reqwest_cert = reqwest::Certificate::from_der(cert.contents())
                    .map_err(|e| Error::Admin(AdminError::Request(e)))?;
                builder = builder.add_root_certificate(reqwest_cert);
            }
        }

        Ok(AdminClient {
            client: builder
                .build()
                .map_err(|e| Error::Admin(AdminError::Request(e)))?,
            admin_url: admin_url.trim_end_matches('/').to_string(),
            auth,
        })
    }

    async fn apply_auth(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, Error> {
        let Some(auth) = &self.auth else {
            return Ok(req);
        };
        let mut auth = auth.lock().await;
        let method = auth.auth_method_name();
        let data = auth.auth_data().await.map_err(Error::Authentication)?;
        let data_str = String::from_utf8(data)
            .map_err(|e| Error::Custom(format!("auth data is not valid UTF-8: {e}")))?;
        Ok(match method.as_str() {
            "token" => req.bearer_auth(data_str),
            "basic" => match data_str.split_once(':') {
                Some((user, pass)) => req.basic_auth(user, Some(pass)),
                None => req.basic_auth(&data_str, None::<&str>),
            },
            _ => req,
        })
    }

    fn topic_policy_url(&self, topic: &str, policy: &str) -> Result<String, Error> {
        let (scheme, tenant, namespace, name) = parse_topic(topic)?;
        Ok(format!(
            "{}/admin/v2/{}/{}/{}/{}/{policy}",
            self.admin_url, scheme, tenant, namespace, name
        ))
    }

    fn schema_url(&self, topic: &str, version: Option<u64>) -> Result<String, Error> {
        let (_, tenant, namespace, name) = parse_topic(topic)?;
        let name = encode_topic_local_name(name);
        let url = format!(
            "{}/admin/v2/schemas/{}/{}/{}/schema",
            self.admin_url, tenant, namespace, name
        );
        Ok(match version {
            Some(version) => format!("{url}/{version}"),
            None => url,
        })
    }

    async fn check_response(&self, resp: reqwest::Response) -> Result<(), Error> {
        if resp.status().is_success() {
            return Ok(());
        }
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(Error::Admin(AdminError::Http { status, body }))
    }

    /// Sets the maximum number of unacknowledged messages allowed per consumer
    /// on a topic.
    ///
    /// This is a persistent broker-side topic policy. The topic must already
    /// exist when this is called (subscribe a consumer first, then call this).
    /// Requires `topicLevelPoliciesEnabled=true` in the broker configuration.
    pub async fn set_max_unacked_messages_per_consumer(
        &self,
        topic: &str,
        max_unacked: u32,
    ) -> Result<(), Error> {
        let url = self.topic_policy_url(topic, "maxUnackedMessagesOnConsumer")?;
        let req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(max_unacked.to_string());
        let req = self.apply_auth(req).await?;
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?;
        self.check_response(resp).await
    }

    /// Removes the per-topic max unacked messages override, reverting to the
    /// broker or namespace default.
    ///
    /// To disable the limit without removing the topic-level override, call
    /// [`set_max_unacked_messages_per_consumer`][Self::set_max_unacked_messages_per_consumer]
    /// with a value of `0` (unlimited).
    pub async fn remove_max_unacked_messages_per_consumer(&self, topic: &str) -> Result<(), Error> {
        let url = self.topic_policy_url(topic, "maxUnackedMessagesOnConsumer")?;
        let req = self.client.delete(&url);
        let req = self.apply_auth(req).await?;
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?;
        self.check_response(resp).await
    }

    async fn get_schema_with_version(
        &self,
        topic: &str,
        version: Option<u64>,
    ) -> Result<Option<Schema>, Error> {
        let url = self.schema_url(topic, version)?;
        let req = self.client.get(&url);
        let req = self.apply_auth(req).await?;
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Admin(AdminError::Http { status, body }));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Admin(AdminError::Request(e)))?;
        parse_schema_response(&body).map(Some)
    }

    /// Gets the latest schema registered for a topic through the Pulsar Admin
    /// HTTP API.
    pub async fn get_schema(&self, topic: &str) -> Result<Option<Schema>, Error> {
        self.get_schema_with_version(topic, None).await
    }

    /// Gets a specific schema version registered for a topic through the Pulsar
    /// Admin HTTP API.
    pub async fn get_schema_at_version(
        &self,
        topic: &str,
        version: u64,
    ) -> Result<Option<Schema>, Error> {
        self.get_schema_with_version(topic, Some(version)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_topic_persistent() {
        let (scheme, tenant, ns, name) =
            parse_topic("persistent://my-tenant/my-namespace/my-topic").unwrap();
        assert_eq!(scheme, "persistent");
        assert_eq!(tenant, "my-tenant");
        assert_eq!(ns, "my-namespace");
        assert_eq!(name, "my-topic");
    }

    #[test]
    fn test_parse_topic_non_persistent() {
        let (scheme, tenant, ns, name) = parse_topic("non-persistent://tenant/ns/topic").unwrap();
        assert_eq!(scheme, "non-persistent");
        assert_eq!(tenant, "tenant");
        assert_eq!(ns, "ns");
        assert_eq!(name, "topic");
    }

    #[test]
    fn test_parse_topic_bare() {
        // No prefix defaults to persistent://
        let (scheme, tenant, ns, name) = parse_topic("tenant/ns/topic").unwrap();
        assert_eq!(scheme, "persistent");
        assert_eq!(tenant, "tenant");
        assert_eq!(ns, "ns");
        assert_eq!(name, "topic");
    }

    #[test]
    fn test_parse_topic_missing_parts() {
        assert!(parse_topic("").is_err());
        assert!(parse_topic("tenant").is_err());
        assert!(parse_topic("tenant/ns").is_err());
        // trailing slash = empty topic name
        assert!(parse_topic("tenant/ns/").is_err());
        assert!(parse_topic("persistent://").is_err());
        assert!(parse_topic("persistent://tenant").is_err());
        assert!(parse_topic("persistent://tenant/ns").is_err());
        assert!(parse_topic("persistent://tenant/ns/").is_err());
    }

    #[test]
    fn test_topic_policy_url() {
        let client = AdminClient {
            client: reqwest::Client::new(),
            admin_url: "http://localhost:8080".to_string(),
            auth: None,
        };
        assert_eq!(
            client
                .topic_policy_url(
                    "persistent://public/default/my-topic",
                    "maxUnackedMessagesOnConsumer"
                )
                .unwrap(),
            "http://localhost:8080/admin/v2/persistent/public/default/my-topic/maxUnackedMessagesOnConsumer"
        );
    }

    #[test]
    fn test_schema_url_latest() {
        let client = AdminClient {
            client: reqwest::Client::new(),
            admin_url: "http://localhost:8080".to_string(),
            auth: None,
        };
        assert_eq!(
            client
                .schema_url("persistent://public/default/my-topic", None)
                .unwrap(),
            "http://localhost:8080/admin/v2/schemas/public/default/my-topic/schema"
        );
    }

    #[test]
    fn test_schema_url_version() {
        let client = AdminClient {
            client: reqwest::Client::new(),
            admin_url: "http://localhost:8080".to_string(),
            auth: None,
        };
        assert_eq!(
            client
                .schema_url("public/default/my-topic", Some(7))
                .unwrap(),
            "http://localhost:8080/admin/v2/schemas/public/default/my-topic/schema/7"
        );
    }

    #[test]
    fn test_schema_url_encodes_local_name() {
        let client = AdminClient {
            client: reqwest::Client::new(),
            admin_url: "http://localhost:8080".to_string(),
            auth: None,
        };
        assert_eq!(
            client
                .schema_url("persistent://public/default/topic?key#frag ment", None)
                .unwrap(),
            "http://localhost:8080/admin/v2/schemas/public/default/topic%3Fkey%23frag%20ment/schema"
        );
    }

    #[test]
    fn test_parse_schema_response() {
        let body = r#"{
            "version": 3,
            "type": "JSON",
            "timestamp": 1234,
            "data": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[]}",
            "properties": {"k": "v"}
        }"#;

        let schema = parse_schema_response(body).unwrap();

        assert_eq!(
            schema.r#type,
            crate::message::proto::schema::Type::Json as i32
        );
        assert_eq!(
            schema.schema_data,
            b"{\"type\":\"record\",\"name\":\"User\",\"fields\":[]}"
        );
        assert_eq!(schema.properties.len(), 1);
        assert_eq!(schema.properties[0].key, "k");
        assert_eq!(schema.properties[0].value, "v");
    }

    #[test]
    fn test_parse_key_value_schema_response_converts_data() {
        let body = r#"{
            "version": 1,
            "type": "KEY_VALUE",
            "timestamp": 1234,
            "data": "{\"key\":{\"type\":\"record\",\"name\":\"Key\",\"fields\":[]},\"value\":\"\"}",
            "properties": {"kv.encoding.type": "SEPARATED"}
        }"#;

        let schema = parse_schema_response(body).unwrap();

        assert_eq!(
            schema.r#type,
            crate::message::proto::schema::Type::KeyValue as i32
        );
        let key_schema = br#"{"fields":[],"name":"Key","type":"record"}"#;
        let mut expected = Vec::new();
        expected.extend_from_slice(&(key_schema.len() as u32).to_be_bytes());
        expected.extend_from_slice(key_schema);
        expected.extend_from_slice(&0u32.to_be_bytes());
        assert_eq!(schema.schema_data, expected);
        assert_eq!(schema.properties[0].key, "kv.encoding.type");
        assert_eq!(schema.properties[0].value, "SEPARATED");
    }

    #[test]
    fn test_admin_url_trailing_slash_stripped() {
        // Trailing slash on admin_url should be normalized away
        let tls = TlsOptions::default();
        let client = AdminClient::new("http://localhost:8080/".to_string(), &tls, None).unwrap();
        assert_eq!(client.admin_url, "http://localhost:8080");
    }
}
