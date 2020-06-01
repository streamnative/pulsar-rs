use crate::connection::{Authentication, Connection};
use crate::error::ConnectionError;
use crate::executor::Executor;
use futures::channel::oneshot;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use url::Url;
use rand::Rng;

/// holds connection information for a broker
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BrokerAddress {
    /// URL we're using for connection (can be the proxy's URL)
    pub url: Url,
    /// pulsar URL for the broker we're actually contacting
    pub broker_url: Url,
    /// true if we're connecting through a proxy
    pub proxy: bool,
}

/// configuration for reconnection exponential back off
#[derive(Debug, Clone)]
pub struct BackOffOptions {
    pub min_backoff: Duration,
    pub max_backoff: Duration,
    pub max_retries: u32,
}

impl std::default::Default for BackOffOptions {
    fn default() -> Self {
        BackOffOptions {
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(30),
            max_retries: 12u32,
        }
    }
}

enum ConnectionStatus {
    Connected(Arc<Connection>),
    Connecting(Vec<oneshot::Sender<Result<Arc<Connection>, ConnectionError>>>),
}

/// Look up broker addresses for topics and partitioned topics
///
/// The ConnectionManager object provides a single interface to start
/// interacting with a cluster. It will automatically follow redirects
/// or use a proxy, and aggregate broker connections
#[derive(Clone)]
pub struct ConnectionManager<Exe: Executor + ?Sized> {
    pub url: Url,
    auth: Option<Authentication>,
    executor: PhantomData<Exe>,
    connections: Arc<Mutex<HashMap<BrokerAddress, ConnectionStatus>>>,
    back_off_options: BackOffOptions,

}

impl<Exe: Executor> ConnectionManager<Exe> {
    pub async fn new(
        url: String,
        auth: Option<Authentication>,
        backoff: Option<BackOffOptions>) -> Result<Self, ConnectionError> {
        let back_off_options = backoff.unwrap_or_default();
        let url = Url::parse(&url).map_err(|e| {
            error!("error parsing URL: {:?}", e);
            ConnectionError::NotFound
        })?;

        let manager = ConnectionManager {
            url: url.clone(),
            auth,
            executor: PhantomData,
            connections: Arc::new(Mutex::new(HashMap::new())),
            back_off_options,
        };
        let broker_address = BrokerAddress {
            url: url.clone(),
            broker_url: url,
            proxy: false,
        };
        manager.connect(broker_address).await?;
        Ok(manager)
    }

    pub fn get_base_address(&self) -> BrokerAddress {
        BrokerAddress {
            url: self.url.clone(),
            broker_url: self.url.clone(),
            proxy: false,
        }
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    pub async fn get_base_connection(&self) -> Result<Arc<Connection>, ConnectionError> {
        let broker_address = BrokerAddress {
            url: self.url.clone(),
            broker_url: self.url.clone(),
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
    ) -> Result<Arc<Connection>, ConnectionError> {
        let rx = {
            match self.connections.lock().unwrap().get_mut(broker) {
                None => None,
                Some(ConnectionStatus::Connected(c)) => {
                    if c.is_valid() {
                        return Ok(c.clone());
                    } else {
                        None
                    }
                },
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

    async fn connect(&self, broker: BrokerAddress) -> Result<Arc<Connection>, ConnectionError> {
        debug!("ConnectionManager::connect({:?})", broker);

        let rx = {
            match self
                .connections
                .lock()
                .unwrap()
                .entry(broker.clone())
                .or_insert(ConnectionStatus::Connecting(Vec::new()))
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
                ConnectionStatus::Connected(_) => {
                    None
                }
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
        //let conn = Connection::new::<Exe>(broker.url.clone(), self.auth.clone(), proxy_url).await?;
        let conn = loop {
            match Connection::new::<Exe>(broker.url.clone(), self.auth.clone(), proxy_url.clone()).await {
                Ok(c) => break c,
                Err(ConnectionError::Io(e)) => {
                    if e.kind() != std::io::ErrorKind::ConnectionRefused {
                        return Err(ConnectionError::Io(e).into());
                    }

                    if current_retries == self.back_off_options.max_retries {
                        return Err(ConnectionError::Io(e).into());
                    }

                    let jitter = rand::thread_rng().gen_range(0, 10);
                    current_backoff = std::cmp::min(
                        self.back_off_options.min_backoff * 2u32.pow(current_retries),
                        self.back_off_options.max_backoff)
                        + self.back_off_options.min_backoff * jitter;
                    current_retries += 1;

                    trace!("current retries: {}, current_backoff(pow = {}): {}ms",
                      current_retries, 2u32.pow(current_retries - 1), current_backoff.as_millis());
                    Exe::delay(current_backoff).await;
                },
                Err(e) => return Err(e.into()),
            }
        };
        debug!("got the new connection in {}ms", (std::time::Instant::now() - start).as_millis());
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
            Some(ConnectionStatus::Connected(c)) => {
                //info!("removing old connection");
            }
            None => {
                //info!("setting up new connection");
            }
        };

        Ok(c)
    }
}
