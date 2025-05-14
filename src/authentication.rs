use async_trait::async_trait;

use crate::error::AuthenticationError;

#[async_trait]
pub trait Authentication: Send + Sync + 'static {
    fn auth_method_name(&self) -> String;

    async fn initialize(&mut self) -> Result<(), AuthenticationError>;

    async fn auth_data(&mut self) -> Result<Vec<u8>, AuthenticationError>;
}

pub mod basic;
#[cfg(feature = "auth-oauth2")]
pub mod oauth2;
pub mod token;
