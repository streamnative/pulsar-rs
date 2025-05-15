//! Token Authenticator

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
