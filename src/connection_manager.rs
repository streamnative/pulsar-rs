use crate::connection::{Authentication, Connection};
use crate::error::ConnectionError;
use crate::executor::Executor;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::oneshot;
use native_tls::Certificate;
use rand::Rng;
use url::Url;

/// holds connection information for a broker
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BrokerAddress {
    /// URL we're using for connection (can be the proxy's URL)
    pub url: Url,
    /// pulsar URL for the broker we're actually contacting
    /// this must follow the IP:port format
    pub broker_url: String,
    /// true if we're connecting through a proxy
    pub proxy: bool,
}

/// configuration for reconnection exponential back off
#[derive(Debug, Clone)]
pub struct ConnectionRetryOptions {
    /// minimum time between connection retries
    pub min_backoff: Duration,
    /// maximum time between rconnection etries
    pub max_backoff: Duration,
    /// maximum number of connection retries
    pub max_retries: u32,
    /// time limit to establish a connection
    pub connection_timeout: Duration,
    /// time limit to receive an answer to a Pulsar operation
    pub operation_timeout: Duration,
}

impl std::default::Default for ConnectionRetryOptions {
    fn default() -> Self {
        ConnectionRetryOptions {
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(30),
            max_retries: 12u32,
            connection_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
        }
    }
}

/// configuration for TLS connections
#[derive(Debug, Clone, Default)]
pub struct TlsOptions {
    pub certificate_chain: Option<Vec<u8>>,
}

enum ConnectionStatus<Exe: Executor> {
    Connected(Arc<Connection<Exe>>),
    Connecting(Vec<oneshot::Sender<Result<Arc<Connection<Exe>>, ConnectionError>>>),
}

/// Look up broker addresses for topics and partitioned topics
///
/// The ConnectionManager object provides a single interface to start
/// interacting with a cluster. It will automatically follow redirects
/// or use a proxy, and aggregate broker connections
#[derive(Clone)]
pub struct ConnectionManager<Exe: Executor> {
    pub url: Url,
    auth: Option<Authentication>,
    pub(crate) executor: Arc<Exe>,
    connections: Arc<Mutex<HashMap<BrokerAddress, ConnectionStatus<Exe>>>>,
    connection_retry_options: ConnectionRetryOptions,
    tls_options: TlsOptions,
    certificate_chain: Vec<native_tls::Certificate>,
}

impl<Exe: Executor> ConnectionManager<Exe> {
    pub async fn new(
        url: String,
        auth: Option<Authentication>,
        connection_retry: Option<ConnectionRetryOptions>,
        tls: Option<TlsOptions>,
        executor: Arc<Exe>,
    ) -> Result<Self, ConnectionError> {
        let connection_retry_options = connection_retry.unwrap_or_default();
        let tls_options = tls.unwrap_or_default();
        let url = Url::parse(&url)
            .map_err(|e| {
                error!("error parsing URL: {:?}", e);
                ConnectionError::NotFound
            })
            .and_then(|url| {
                url.host_str().ok_or_else(|| {
                    error!("missing host for URL: {:?}", url);
                    ConnectionError::NotFound
                })?;
                Ok(url)
            })?;

        let certificate_chain = match tls_options.certificate_chain.as_ref() {
            None => vec![],
            Some(certificate_chain) => {
                let mut v = vec![];
                for cert in pem::parse_many(&certificate_chain).iter().rev() {
                    v.push(
                        Certificate::from_der(&cert.contents[..])
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
                    );
                }
                v
            }
        };

        let manager = ConnectionManager {
            url: url.clone(),
            auth,
            executor,
            connections: Arc::new(Mutex::new(HashMap::new())),
            connection_retry_options,
            tls_options,
            certificate_chain,
        };
        let broker_address = BrokerAddress {
            url: url.clone(),
            broker_url: format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(6650)),
            proxy: false,
        };
        manager.connect(broker_address).await?;
        Ok(manager)
    }

    pub fn get_base_address(&self) -> BrokerAddress {
        BrokerAddress {
            url: self.url.clone(),
            broker_url: format!(
                "{}:{}",
                self.url.host_str().unwrap(),
                self.url.port().unwrap_or(6650)
            ),
            proxy: false,
        }
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    pub async fn get_base_connection(&self) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        let broker_address = BrokerAddress {
            url: self.url.clone(),
            broker_url: format!(
                "{}:{}",
                self.url.host_str().unwrap(),
                self.url.port().unwrap_or(6650)
            ),
            proxy: false,
        };

        self.get_connection(&broker_address).await
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    pub async fn get_connection(
        &self,
        broker: &BrokerAddress,
    ) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        let rx = {
            match self.connections.lock().unwrap().get_mut(broker) {
                None => None,
                Some(ConnectionStatus::Connected(c)) => {
                    if c.is_valid() {
                        return Ok(c.clone());
                    } else {
                        None
                    }
                }
                Some(ConnectionStatus::Connecting(ref mut v)) => {
                    let (tx, rx) = oneshot::channel();
                    v.push(tx);
                    Some(rx)
                }
            }
        };

        match rx {
            None => self.connect(broker.clone()).await,
            Some(rx) => match rx.await {
                Ok(res) => res,
                Err(_) => Err(ConnectionError::Canceled),
            },
        }
    }

    async fn connect(&self, broker: BrokerAddress) -> Result<Arc<Connection<Exe>>, ConnectionError> {
        debug!("ConnectionManager::connect({:?})", broker);

        let rx = {
            match self
                .connections
                .lock()
                .unwrap()
                .entry(broker.clone())
                .or_insert_with(|| ConnectionStatus::Connecting(Vec::new()))
            {
                ConnectionStatus::Connecting(ref mut v) => {
                    if v.is_empty() {
                        None
                    } else {
                        let (tx, rx) = oneshot::channel();
                        v.push(tx);
                        Some(rx)
                    }
                }
                ConnectionStatus::Connected(_) => None,
            }
        };
        if let Some(rx) = rx {
            return match rx.await {
                Ok(res) => res,
                Err(_) => Err(ConnectionError::Canceled),
            };
        }

        let proxy_url = if broker.proxy {
            Some(broker.broker_url.clone())
        } else {
            None
        };

        let mut current_backoff;
        let mut current_retries = 0u32;

        let start = std::time::Instant::now();
        let conn = loop {
            match Connection::new(
                broker.url.clone(),
                self.auth.clone(),
                proxy_url.clone(),
                &self.certificate_chain,
                self.connection_retry_options.connection_timeout,
                self.connection_retry_options.operation_timeout,
                self.executor.clone(),
            )
            .await
            {
                Ok(c) => break c,
                Err(ConnectionError::Io(e)) => {
                    if e.kind() != std::io::ErrorKind::ConnectionRefused ||
                      e.kind() != std::io::ErrorKind::TimedOut {
                        return Err(ConnectionError::Io(e));
                    }

                    if current_retries == self.connection_retry_options.max_retries {
                        return Err(ConnectionError::Io(e));
                    }

                    let jitter = rand::thread_rng().gen_range(0..10);
                    current_backoff = std::cmp::min(
                        self.connection_retry_options.min_backoff * 2u32.saturating_pow(current_retries),
                        self.connection_retry_options.max_backoff,
                    ) + self.connection_retry_options.min_backoff * jitter;
                    current_retries += 1;

                    trace!(
                        "current retries: {}, current_backoff(pow = {}): {}ms",
                        current_retries,
                        2u32.pow(current_retries - 1),
                        current_backoff.as_millis()
                    );
                    error!(
                        "connection error, retrying connection to {} after {}ms",
                        broker.url,
                        current_backoff.as_millis()
                    );
                    self.executor.delay(current_backoff).await;
                }
                Err(e) => return Err(e),
            }
        };
        info!(
            "Connected to {} in {}ms",
            broker.url,
            (std::time::Instant::now() - start).as_millis()
        );
        let c = Arc::new(conn);
        let old = self
            .connections
            .lock()
            .unwrap()
            .insert(broker, ConnectionStatus::Connected(c.clone()));
        match old {
            Some(ConnectionStatus::Connecting(mut v)) => {
                //info!("was in connecting state({} waiting)", v.len());
                for tx in v.drain(..) {
                    let _ = tx.send(Ok(c.clone()));
                }
            }
            Some(ConnectionStatus::Connected(_)) => {
                //info!("removing old connection");
            }
            None => {
                //info!("setting up new connection");
            }
        };

        Ok(c)
    }
}
