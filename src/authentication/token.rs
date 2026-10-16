//! Token Authenticator

use std::{future::Future, path::PathBuf, pin::Pin};

use async_trait::async_trait;

use crate::{authentication::Authentication, error::AuthenticationError};

/// How a [`TokenAuthentication`] obtains the token bytes for a connection.
enum TokenSource {
    /// A fixed token, captured once.
    Static(Vec<u8>),
    /// A file re-read every time the broker asks for credentials.
    File(PathBuf),
    /// A user-supplied async closure, called every time the broker asks for
    /// credentials.
    Supplier(Box<dyn Fn() -> BoxTokenFuture + Send + Sync>),
}

type BoxTokenFuture =
    Pin<Box<dyn Future<Output = Result<Vec<u8>, AuthenticationError>> + Send + 'static>>;

/// JWT token authentication.
///
/// The broker asks for credentials again whenever the ones it holds expire (a
/// `CommandAuthChallenge`), and on every reconnection. A [`Static`] token cannot answer
/// those, so a client using one stops working the moment its token expires — use
/// [`from_file`] or [`from_supplier`] for credentials that rotate.
///
/// [`Static`]: TokenAuthentication::new
/// [`from_file`]: TokenAuthentication::from_file
/// [`from_supplier`]: TokenAuthentication::from_supplier
pub struct TokenAuthentication {
    source: TokenSource,
}

impl TokenAuthentication {
    /// A fixed token that never changes.
    ///
    /// Only appropriate for tokens that outlive the client. For an expiring token, use
    /// [`TokenAuthentication::from_file`] or [`TokenAuthentication::from_supplier`].
    #[allow(clippy::new_ret_no_self)]
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(token: String) -> Box<dyn Authentication> {
        Box::new(TokenAuthentication {
            source: TokenSource::Static(token.into_bytes()),
        })
    }

    /// A token read from `path`, re-read every time the broker asks for credentials.
    ///
    /// This is the provider to use for a Kubernetes projected ServiceAccount token, or
    /// any other token a sidecar rotates in place: the file is read on each auth
    /// challenge and each reconnection, so a rotated token is picked up without
    /// restarting the client. Trailing whitespace is trimmed.
    ///
    /// Read errors are reported as [`AuthenticationError::Retriable`], so a token file
    /// that is briefly absent mid-rotation causes a connection retry rather than a
    /// fatal error.
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), pulsar::Error> {
    /// use pulsar::{authentication::token::TokenAuthentication, Pulsar, TokioExecutor};
    ///
    /// let client: Pulsar<_> = Pulsar::builder("pulsar://localhost:6650", TokioExecutor)
    ///     .with_auth_provider(TokenAuthentication::from_file(
    ///         "/var/run/secrets/pulsar/token",
    ///     ))
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn from_file(path: impl Into<PathBuf>) -> Box<dyn Authentication> {
        Box::new(TokenAuthentication {
            source: TokenSource::File(path.into()),
        })
    }

    /// A token produced by `supplier`, called every time the broker asks for credentials.
    ///
    /// Use this when the token comes from somewhere other than a file — a secrets
    /// manager, an in-memory cache refreshed elsewhere, a token exchange.
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), pulsar::Error> {
    /// use pulsar::{authentication::token::TokenAuthentication, Pulsar, TokioExecutor};
    ///
    /// let auth = TokenAuthentication::from_supplier(|| async {
    ///     Ok(fetch_token_from_somewhere().await.into_bytes())
    /// });
    /// # async fn fetch_token_from_somewhere() -> String { unimplemented!() }
    /// # let _ = auth;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn from_supplier<F, Fut>(supplier: F) -> Box<dyn Authentication>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, AuthenticationError>> + Send + 'static,
    {
        Box::new(TokenAuthentication {
            source: TokenSource::Supplier(Box::new(move || Box::pin(supplier()))),
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
        match &self.source {
            TokenSource::Static(token) => Ok(token.clone()),
            TokenSource::File(path) => std::fs::read_to_string(path)
                .map(|token| token.trim().as_bytes().to_vec())
                .map_err(|e| {
                    AuthenticationError::Retriable(format!(
                        "failed to read token file {}: {e}",
                        path.display()
                    ))
                }),
            TokenSource::Supplier(supplier) => supplier().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn from_file_picks_up_a_rotated_token() {
        let dir = std::env::temp_dir().join(format!("pulsar-rs-token-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");

        // Trailing newline is what a k8s projected token volume looks like.
        std::fs::write(&path, "first-token\n").unwrap();
        let mut auth = TokenAuthentication::from_file(&path);
        assert_eq!(auth.auth_data().await.unwrap(), b"first-token".to_vec());

        // Rotated in place, as kubelet does before the old token expires.
        std::fs::write(&path, "second-token\n").unwrap();
        assert_eq!(auth.auth_data().await.unwrap(), b"second-token".to_vec());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn from_file_reports_a_missing_file_as_retriable() {
        let mut auth = TokenAuthentication::from_file("/nonexistent/pulsar-rs/token");
        match auth.auth_data().await {
            Err(AuthenticationError::Retriable(_)) => {}
            other => panic!("expected a retriable error, got {other:?}"),
        }
    }

    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn from_supplier_is_called_for_every_request() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let mut auth = {
            let calls = calls.clone();
            TokenAuthentication::from_supplier(move || {
                let calls = calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    Ok(format!("token-{n}").into_bytes())
                }
            })
        };

        assert_eq!(auth.auth_data().await.unwrap(), b"token-0".to_vec());
        assert_eq!(auth.auth_data().await.unwrap(), b"token-1".to_vec());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    async fn static_token_is_stable() {
        let mut auth = TokenAuthentication::new("fixed".to_string());
        assert_eq!(auth.auth_method_name(), "token");
        assert_eq!(auth.auth_data().await.unwrap(), b"fixed".to_vec());
        assert_eq!(auth.auth_data().await.unwrap(), b"fixed".to_vec());
    }
}
