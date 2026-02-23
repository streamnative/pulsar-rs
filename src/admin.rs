//! Pulsar Admin REST API client.
//!
//! Enabled by the `admin-api` feature flag. Requires a tokio runtime.

use std::sync::Arc;

use futures::lock::Mutex;

use crate::{
    authentication::Authentication,
    connection_manager::TlsOptions,
    error::{AdminError, Error},
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
            .danger_accept_invalid_certs(tls_options.allow_insecure_connection);

        builder = builder.danger_accept_invalid_hostnames(
            tls_options.allow_insecure_connection || !tls_options.tls_hostname_verification_enabled,
        );

        if let Some(pem_bytes) = &tls_options.certificate_chain {
            let certs = pem::parse_many(pem_bytes)
                .map_err(|e| Error::Custom(format!("failed to parse certificate chain: {e}")))?;
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
    /// exist when this is called (e.g., after a consumer has subscribed).
    /// Requires `topicLevelPoliciesEnabled=true` in the broker configuration.
    /// This will be called automatically if using `BrokerConfigOptions` via the builder.
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
    /// This must be called manually via `Pulsar::admin`; the consumer builder has no equivalent.
    /// To disable the limit without removing the topic-level override, call
    /// `set_max_unacked_messages_per_consumer` with a value of `0` (unlimited), or
    /// use `BrokerConfigOptions` to set this value via the builder.
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
    fn test_admin_url_trailing_slash_stripped() {
        // Trailing slash on admin_url should be normalized away
        let tls = TlsOptions::default();
        let client = AdminClient::new("http://localhost:8080/".to_string(), &tls, None).unwrap();
        assert_eq!(client.admin_url, "http://localhost:8080");
    }
}
