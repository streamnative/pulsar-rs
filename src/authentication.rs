use async_trait::async_trait;

use crate::Error;

#[async_trait]
pub trait Authentication: Send + Sync + 'static {
    fn auth_method_name(&self) -> String;

    async fn initialize(&mut self) -> Result<(), Error>;

    async fn auth_data(&mut self) -> Result<Vec<u8>, Error>;
}

pub mod token {
    use std::rc::Rc;

    use async_trait::async_trait;

    use crate::authentication::Authentication;
    use crate::Error;

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

        async fn initialize(&mut self) -> Result<(), Error> {
            Ok(())
        }

        async fn auth_data(&mut self) -> Result<Vec<u8>, Error> {
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
    use crate::Error;
    use crate::error::AuthenticationError;

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

    pub struct OAuth2Authentication {
        params: OAuth2Params,
        private_params: Option<OAuth2PrivateParams>,
        token_url: Option<TokenUrl>,
        token: Option<BasicTokenResponse>,
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
        fn read_private_params(&self) -> Result<OAuth2PrivateParams, Error> {
            if self.credentials_url.scheme() != "file" {
                return Err(Error::Authentication(AuthenticationError::Custom(format!("invalid credential url [{}]", self.credentials_url.as_str()))));
            }
            let path = self.credentials_url.path();
            Ok(serde_json::from_str(fs::read_to_string(path).unwrap().as_str()).unwrap())
        }
    }

    #[async_trait]
    impl Authentication for OAuth2Authentication {
        fn auth_method_name(&self) -> String {
            String::from("token")
        }

        async fn initialize(&mut self) -> Result<(), Error> {
            self.private_params = Some(self.params.read_private_params()?);
            if let Err(e) = self.token_url().await {
                return Err(Error::Authentication(AuthenticationError::Custom(e.to_string())));
            }
            Ok(())
        }

        async fn auth_data(&mut self) -> Result<Vec<u8>, Error> {
            if self.private_params.is_none() {
                return Err(Error::Authentication(AuthenticationError::Custom("not initialized".to_string())));
            }
            match self.token.as_ref() {
                Some(token) => Ok(token.access_token().secret().clone().into_bytes()),
                None => {
                    match self.fetch_token().await {
                        Ok(token) => {
                            self.token = Some(token);
                            Ok(self.token.as_ref().unwrap().access_token().secret().clone().into_bytes())
                        }
                        Err(e) => {
                            Err(Error::Authentication(AuthenticationError::Custom(e.to_string())))
                        }
                    }
                }
            }
        }
    }

    impl OAuth2Authentication {
        async fn token_url(&mut self) -> Result<Option<TokenUrl>, Box<dyn std::error::Error>> {
            match &self.token_url {
                Some(url) => Ok(Some(url.clone())),
                None => {
                    let metadata = CoreProviderMetadata::discover_async(
                        IssuerUrl::from_url(self.params.issuer_url.clone()), async_http_client).await?;
                    self.token_url = Some(metadata.token_endpoint().unwrap().clone());
                    match metadata.token_endpoint() {
                        Some(endpoint) => {
                            Ok(Some(endpoint.clone()))
                        }
                        None => Err(Box::new(Error::Authentication(AuthenticationError::Custom("token endpoint is unavailable".to_string()))))
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
            info!("Got a new token: {:?}\n{}", token, token.access_token().secret());
            Ok(token)
        }
    }
}
