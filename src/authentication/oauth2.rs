//! OAuth2 Authenticator

use std::{
    fmt::{Display, Formatter},
    fs,
    time::Instant,
};

use async_trait::async_trait;
use data_url::DataUrl;
use nom::lib::std::ops::Add;
use oauth2::{
    basic::{BasicClient, BasicTokenResponse},
    reqwest,
    AuthType::RequestBody,
    AuthUrl, ClientId, ClientSecret, RequestTokenError, Scope, TokenResponse, TokenUrl,
};
use openidconnect::{core::CoreProviderMetadata, IssuerUrl};
use serde::Deserialize;
use url::Url;

use crate::{authentication::Authentication, error::AuthenticationError};

#[derive(Deserialize, Debug, Clone)]
struct OAuth2PrivateParams {
    client_id: String,
    client_secret: String,
    #[allow(dead_code)]
    client_email: Option<String>,
    issuer_url: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct OAuth2Params {
    pub issuer_url: String,
    pub credentials_url: String,
    pub audience: Option<String>,
    pub scope: Option<String>,
}

impl Display for OAuth2Params {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OAuth2Params({}, {}, {:?}, {:?})",
            self.issuer_url, self.credentials_url, self.audience, self.scope
        )
    }
}

pub struct CachedToken {
    token_secret: Vec<u8>,
    expiring_at: Option<Instant>,
    expired_at: Option<Instant>,
}

impl From<BasicTokenResponse> for CachedToken {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(resp: BasicTokenResponse) -> Self {
        let now = Instant::now();
        CachedToken {
            expiring_at: resp.expires_in().map(|d| now.add(d.mul_f32(0.9))),
            expired_at: resp.expires_in().map(|d| now.add(d)),
            token_secret: resp.access_token().secret().clone().into_bytes(),
        }
    }
}

impl CachedToken {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn is_expiring(&self) -> bool {
        match &self.expiring_at {
            Some(expiring_at) => Instant::now().ge(expiring_at),
            None => false,
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn is_expired(&self) -> bool {
        match &self.expired_at {
            Some(expired_at) => Instant::now().ge(expired_at),
            None => false,
        }
    }
}

pub struct OAuth2Authentication {
    params: OAuth2Params,
    private_params: Option<OAuth2PrivateParams>,
    token_url: Option<TokenUrl>,
    token: Option<CachedToken>,
}

impl OAuth2Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn client_credentials(params: OAuth2Params) -> Box<dyn Authentication> {
        Box::new(OAuth2Authentication {
            params,
            private_params: None,
            token_url: None,
            token: None,
        })
    }

    fn credentials_hint(&self) -> String {
        match Url::parse(self.params.credentials_url.as_str()) {
            Ok(url) => match url.scheme() {
                "file" => format!("file:{}", url.path()),
                scheme => format!("{scheme}:<redacted>"),
            },
            Err(_) => String::from("<invalid credentials_url>"),
        }
    }
}

impl OAuth2Params {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn read_private_params(&self) -> Result<OAuth2PrivateParams, Box<dyn std::error::Error>> {
        let credentials_url = Url::parse(self.credentials_url.as_str())?;
        match credentials_url.scheme() {
            "file" => {
                let path = credentials_url.path();
                Ok(serde_json::from_str(fs::read_to_string(path)?.as_str())?)
            }
            "data" => {
                let data_url = match DataUrl::process(self.credentials_url.as_str()) {
                    Ok(data_url) => data_url,
                    Err(err) => {
                        return Err(Box::from(format!(
                            "invalid data url [{}]: {:?}",
                            self.credentials_url.as_str(),
                            err
                        )));
                    }
                };
                let body = match data_url.decode_to_vec() {
                    Ok((body, _)) => body,
                    Err(err) => {
                        return Err(Box::from(format!(
                            "invalid data url [{}]: {:?}",
                            self.credentials_url.as_str(),
                            err
                        )));
                    }
                };

                Ok(serde_json::from_slice(&body)?)
            }
            _ => Err(Box::from(format!(
                "invalid credential url [{}]",
                self.credentials_url.as_str()
            ))),
        }
    }
}

#[async_trait]
impl Authentication for OAuth2Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn auth_method_name(&self) -> String {
        String::from("token")
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn initialize(&mut self) -> Result<(), AuthenticationError> {
        match self.params.read_private_params() {
            Ok(private_params) => self.private_params = Some(private_params),
            Err(e) => {
                return Err(AuthenticationError::Custom(format!(
                    "failed to read OAuth2 credentials from {}: {e}",
                    self.credentials_hint()
                )));
            }
        }
        if let Err(e) = self.token_url().await {
            return Err(AuthenticationError::Custom(format!(
                "failed to discover OAuth2 token endpoint for issuer [{}]: {e}",
                self.params.issuer_url
            )));
        }
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
        if self.private_params.is_none() {
            return Err(AuthenticationError::Custom("not initialized".to_string()));
        }
        let mut need_token = false;
        let mut none_or_expired = true;
        if let Some(token) = self.token.as_ref() {
            none_or_expired = token.is_expired();
            if none_or_expired || token.is_expiring() {
                need_token = true;
            }
        } else {
            need_token = true;
        }
        if need_token {
            match self.fetch_token().await {
                Ok(token) => {
                    self.token = Some(token.into());
                }
                Err(e) => {
                    if none_or_expired {
                        // invalidate the expired token
                        self.token = None;
                        if let Some(auth_err) = e.downcast_ref::<AuthenticationError>() {
                            return Err(auth_err.clone());
                        }
                        return Err(AuthenticationError::Custom(format!(
                            "failed to fetch OAuth2 access token for issuer [{}] (audience={:?}, scope={:?}): {e}",
                            self.params.issuer_url,
                            self.params.audience,
                            self.params.scope
                        )));
                    } else {
                        warn!(
                            "failed to refresh OAuth2 token for issuer [{}] (audience={:?}, scope={:?}): {e}. Using existing non-expired token",
                            self.params.issuer_url,
                            self.params.audience,
                            self.params.scope
                        );
                    }
                }
            }
        }
        Ok(self.token.as_ref().unwrap().token_secret.clone())
    }
}

impl OAuth2Authentication {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn token_url(&mut self) -> Result<TokenUrl, Box<dyn std::error::Error>> {
        match &self.token_url {
            Some(url) => Ok(url.clone()),
            None => {
                let client = reqwest::Client::new();
                let metadata = CoreProviderMetadata::discover_async(
                    IssuerUrl::from_url(Url::parse(self.params.issuer_url.as_str())?),
                    &client,
                )
                .await?;
                if let Some(token_endpoint) = metadata.token_endpoint() {
                    self.token_url = Some(token_endpoint.clone());
                    return Ok(token_endpoint.clone());
                } else {
                    return Err(Box::from("token url not exists"));
                }
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn fetch_token(&mut self) -> Result<BasicTokenResponse, Box<dyn std::error::Error>> {
        let token_url = self.token_url().await.map_err(|e| {
            AuthenticationError::Retriable(format!(
                "failed to discover OAuth2 token endpoint for issuer [{}]: {e}",
                self.params.issuer_url
            ))
        })?;

        let private_params = self
            .private_params
            .as_ref()
            .expect("oauth2 provider is uninitialized");

        let issuer_url = if let Some(url) = private_params.issuer_url.as_ref() {
            url.as_str()
        } else {
            self.params.issuer_url.as_str()
        };

        let client = BasicClient::new(ClientId::new(private_params.client_id.clone()))
            .set_client_secret(ClientSecret::new(private_params.client_secret.clone()))
            .set_auth_uri(AuthUrl::from_url(Url::parse(issuer_url)?))
            .set_token_uri(token_url)
            .set_auth_type(RequestBody);

        let mut request = client.exchange_client_credentials();

        if let Some(audience) = &self.params.audience {
            request = request.add_extra_param("audience", audience.clone());
        }

        if let Some(scope) = &self.params.scope {
            request = request.add_scope(Scope::new(scope.clone()));
        }

        let client = reqwest::Client::new();
        let token = request.request_async(&client).await.map_err(|e| {
            let error_message = format!(
                "token endpoint request failed for issuer [{}] (audience={:?}, scope={:?}): {e:?}",
                self.params.issuer_url, self.params.audience, self.params.scope
            );
            if let RequestTokenError::ServerResponse(_) = e {
                AuthenticationError::Custom(error_message)
            } else {
                AuthenticationError::Retriable(error_message)
            }
        })?;
        debug!("Got a new oauth2 token for [{}]", self.params);
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use oauth2::TokenUrl;

    use crate::{
        authentication::{
            oauth2::{OAuth2Authentication, OAuth2Params, OAuth2PrivateParams},
            Authentication,
        },
        error::{AuthenticationError, ConnectionError},
    };

    #[test]
    fn parse_data_url() {
        let params = OAuth2Params {
            issuer_url: "".to_string(),
            credentials_url: "data:application/json;base64,eyJjbGllbnRfaWQiOiJjbGllbnQtaWQiLCJjbGllbnRfc2VjcmV0IjoiY2xpZW50LXNlY3JldCJ9Cg==".to_string(),
            audience: None,
            scope: None,
        };
        let private_params = params.read_private_params().unwrap();
        assert_eq!(private_params.client_id, "client-id");
        assert_eq!(private_params.client_secret, "client-secret");
        assert_eq!(private_params.client_email, None);
        assert_eq!(private_params.issuer_url, None);
    }

    #[tokio::test]
    async fn auth_data_returns_retriable_error() {
        let params = OAuth2Params {
            issuer_url: "http://issuer.example".to_string(),
            credentials_url: "data:application/json;base64,eyJjbGllbnRfaWQiOiJpZCIsImNsaWVudF9zZWNyZXQiOiJzZWNyZXQifQ==".to_string(),
            audience: None,
            scope: None,
        };

        let private_params = OAuth2PrivateParams {
            client_id: "id".to_string(),
            client_secret: "secret".to_string(),
            client_email: None,
            issuer_url: None,
        };

        let assert_retriable_error = |err| match err {
            AuthenticationError::Retriable(_) => {
                let err: ConnectionError = err.into();
                assert!(err.establish_retryable());
            }
            other => panic!("expected retriable error, got {other:?}"),
        };

        let mut auth = OAuth2Authentication {
            params: params.clone(),
            private_params: Some(private_params.clone()),
            token_url: None, // token_url is not set to trigger discovery
            token: None,
        };
        assert_retriable_error(auth.auth_data().await.unwrap_err());

        let token_url = TokenUrl::new("http://127.0.0.1:1/token".to_string()).unwrap();

        let mut auth = OAuth2Authentication {
            params,
            private_params: Some(private_params),
            token_url: Some(token_url),
            token: None,
        };
        assert_retriable_error(auth.auth_data().await.unwrap_err());
    }
}
