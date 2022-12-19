use async_trait::async_trait;

use crate::error::AuthenticationError;

#[async_trait]
pub trait Authentication: Send + Sync + 'static {
    fn auth_method_name(&self) -> String;

    async fn initialize(&mut self) -> Result<(), AuthenticationError>;

    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError>;
}

pub mod token {
    use std::rc::Rc;

    use async_trait::async_trait;

    use crate::{authentication::Authentication, error::AuthenticationError};

    pub struct TokenAuthentication {
        token: Vec<u8>,
    }

    impl TokenAuthentication {
        #[allow(clippy::new_ret_no_self)]
        #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
        pub fn new(token: String) -> Rc<dyn Authentication> {
            Rc::new(TokenAuthentication {
                token: token.into_bytes(),
            })
        }
    }

    #[async_trait]
    impl Authentication for TokenAuthentication {
        #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
        fn auth_method_name(&self) -> String {
            String::from("token")
        }

        #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
        async fn initialize(&mut self) -> Result<(), AuthenticationError> {
            Ok(())
        }

        #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
        async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
            Ok(self.token.clone())
        }
    }
}

#[cfg(feature = "auth-oauth2")]
pub mod oauth2 {
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
        reqwest::async_http_client,
        AuthType::RequestBody,
        AuthUrl, ClientId, ClientSecret, Scope, TokenResponse, TokenUrl,
    };
    use openidconnect::{core::CoreProviderMetadata, IssuerUrl};
    use serde::Deserialize;
    use url::Url;

    use crate::{authentication::Authentication, error::AuthenticationError};

    #[derive(Deserialize, Debug)]
    struct OAuth2PrivateParams {
        client_id: String,
        client_secret: String,
        #[allow(dead_code)]
        client_email: Option<String>,
        issuer_url: Option<String>,
    }

    #[derive(Deserialize, Debug)]
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
                Err(e) => return Err(AuthenticationError::Custom(e.to_string())),
            }
            if let Err(e) = self.token_url().await {
                return Err(AuthenticationError::Custom(e.to_string()));
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
                            return Err(AuthenticationError::Custom(e.to_string()));
                        } else {
                            warn!(
                                "failed to get a new token for [{}], use the existing one for now",
                                self.params
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
        async fn token_url(&mut self) -> Result<Option<TokenUrl>, Box<dyn std::error::Error>> {
            match &self.token_url {
                Some(url) => Ok(Some(url.clone())),
                None => {
                    let metadata = CoreProviderMetadata::discover_async(
                        IssuerUrl::from_url(Url::parse(self.params.issuer_url.as_str())?),
                        async_http_client,
                    )
                    .await?;
                    if let Some(token_endpoint) = metadata.token_endpoint() {
                        self.token_url = Some(token_endpoint.clone());
                    } else {
                        return Err(Box::from("token url not exists"));
                    }

                    match metadata.token_endpoint() {
                        Some(endpoint) => Ok(Some(endpoint.clone())),
                        None => Err(Box::from("token endpoint is unavailable")),
                    }
                }
            }
        }

        #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
        async fn fetch_token(&mut self) -> Result<BasicTokenResponse, Box<dyn std::error::Error>> {
            let private_params = self
                .private_params
                .as_ref()
                .expect("oauth2 provider is uninitialized");

            let issuer_url = if let Some(url) = private_params.issuer_url.as_ref() {
                url.as_str()
            } else {
                self.params.issuer_url.as_str()
            };

            let client = BasicClient::new(
                ClientId::new(private_params.client_id.clone()),
                Some(ClientSecret::new(private_params.client_secret.clone())),
                AuthUrl::from_url(Url::parse(issuer_url)?),
                self.token_url().await?,
            )
            .set_auth_type(RequestBody);

            let mut request = client.exchange_client_credentials();

            if let Some(audience) = &self.params.audience {
                request = request.add_extra_param("audience", audience.clone());
            }

            if let Some(scope) = &self.params.scope {
                request = request.add_scope(Scope::new(scope.clone()));
            }

            let token = request.request_async(async_http_client).await?;
            debug!("Got a new oauth2 token for [{}]", self.params);
            Ok(token)
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::authentication::oauth2::OAuth2Params;

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
    }
}
