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

    use crate::authentication::Authentication;
    use crate::error::AuthenticationError;

    pub struct TokenAuthentication {
        token: Vec<u8>,
    }

    impl TokenAuthentication {
        pub fn new(token: String) -> Rc<dyn Authentication> {
            Rc::new(TokenAuthentication {
                token: token.into_bytes()
            })
        }
    }

    #[async_trait]
    impl Authentication for TokenAuthentication {
        fn auth_method_name(&self) -> String {
            String::from("token")
        }

        async fn initialize(&mut self) -> Result<(), AuthenticationError> {
            Ok(())
        }

        async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError> {
            Ok(self.token.clone())
        }
    }
}

#[cfg(feature = "auth-oauth2")]
pub mod oauth2 {
    use std::fs;

    use async_trait::async_trait;
    use oauth2::{AuthUrl, ClientId, ClientSecret, Scope, TokenResponse, TokenUrl};
    use oauth2::AuthType::RequestBody;
    use oauth2::basic::{BasicClient, BasicTokenResponse};
    use oauth2::reqwest::async_http_client;
    use openidconnect::core::CoreProviderMetadata;
    use openidconnect::IssuerUrl;
    use serde::Deserialize;
    use url::Url;

    use crate::authentication::Authentication;
    use crate::error::AuthenticationError;
    use std::time::Instant;
    use nom::lib::std::ops::Add;
    use std::fmt::{Display, Formatter};

    #[derive(Deserialize, Debug)]
    struct OAuth2PrivateParams {
        client_id: String,
        client_secret: String,
        client_email: Option<String>,
        issuer_url: Option<String>,
    }

    #[derive(Deserialize, Debug)]
    pub struct OAuth2Params {
        issuer_url: Url,
        credentials_url: Url,
        audience: String,
        scope: Option<String>,
    }

    impl Display for OAuth2Params {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "OAuth2Params({}, {}, {}, {:?})", self.issuer_url, self.credentials_url, self.audience, self.scope)
        }
    }

    pub struct CachedToken {
        token_secret: Vec<u8>,
        expiring_at: Option<Instant>,
        expired_at: Option<Instant>,
    }

    impl From<BasicTokenResponse> for CachedToken {
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
        fn is_expiring(&self) -> bool {
            match &self.expiring_at {
                Some(expiring_at) => Instant::now().ge(expiring_at),
                None => false,
            }
        }

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
        fn read_private_params(&self) -> Result<OAuth2PrivateParams, Box<dyn std::error::Error>> {
            if self.credentials_url.scheme() != "file" {
                return Err(Box::from(format!("invalid credential url [{}]", self.credentials_url.as_str())));
            }
            let path = self.credentials_url.path();
            Ok(serde_json::from_str(fs::read_to_string(path)?.as_str())?)
        }
    }

    #[async_trait]
    impl Authentication for OAuth2Authentication {
        fn auth_method_name(&self) -> String {
            String::from("token")
        }

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
                            return Err(AuthenticationError::Custom(e.to_string()));
                        } else {
                            warn!("failed to get a new token for [{}], use the existing one for now", self.params);
                        }
                    }
                }
            }
            Ok(self.token.as_ref().unwrap().token_secret.clone())
        }
    }

    impl OAuth2Authentication {
        async fn token_url(&mut self) -> Result<Option<TokenUrl>, Box<dyn std::error::Error>> {
            match &self.token_url {
                Some(url) => Ok(Some(url.clone())),
                None => {
                    let metadata = CoreProviderMetadata::discover_async(
                        IssuerUrl::from_url(self.params.issuer_url.clone()), async_http_client).await?;
                    if let Some(token_endpoint) = metadata.token_endpoint() {
                        self.token_url = Some(token_endpoint.clone());
                    } else {
                        return Err(Box::from("token url not exists"));
                    }

                    match metadata.token_endpoint() {
                        Some(endpoint) => {
                            Ok(Some(endpoint.clone()))
                        }
                        None => Err(Box::from("token endpoint is unavailable"))
                    }
                }
            }
        }

        async fn fetch_token(&mut self) -> Result<BasicTokenResponse, Box<dyn std::error::Error>> {
            let private_params = self.private_params.as_ref()
                .expect("oauth2 provider is uninitialized");

            let client = BasicClient::new(
                ClientId::new(private_params.client_id.clone()),
                Some(ClientSecret::new(private_params.client_secret.clone())),
                AuthUrl::from_url(self.params.issuer_url.clone()),
                self.token_url().await?)
                .set_auth_type(RequestBody);

            let mut request = client
                .exchange_client_credentials()
                .add_extra_param("audience", self.params.audience.clone());

            if let Some(scope) = &self.params.scope {
                request = request.add_scope(Scope::new(scope.clone()));
            }

            let token = request
                .request_async(async_http_client).await?;
            debug!("Got a new oauth2 token for [{}]", self.params);
            Ok(token)
        }
    }
}
