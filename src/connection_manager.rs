use crate::connection::{Authentication, Connection};
use crate::error::ConnectionError;
use crate::executor::Executor;
use futures::channel::oneshot;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use url::Url;

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
}

impl<Exe: Executor> ConnectionManager<Exe> {
    pub async fn new(url: String, auth: Option<Authentication>) -> Result<Self, ConnectionError> {
        let url = Url::parse(&url).map_err(|e| {
            error!("error parsing URL: {:?}", e);
            ConnectionError::NotFound
        })?;

        let manager = ConnectionManager {
            url: url.clone(),
            auth,
            executor: PhantomData,
            connections: Arc::new(Mutex::new(HashMap::new())),
        };
        let broker_address = BrokerAddress {
            url: url.clone(),
            broker_url: url,
            proxy: false,
        };
        manager.connect(broker_address).await?;
        Ok(manager)
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
                Some(ConnectionStatus::Connected(c)) => return Ok(c.clone()),
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
        info!("ConnectionManager::connect({:?})", broker);

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
                ConnectionStatus::Connected(c) => {
                    info!("already connected");
                    return Ok(c.clone());
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

        let conn = Connection::new::<Exe>(broker.url.clone(), self.auth.clone(), proxy_url).await?;
        let c = Arc::new(conn);
        let old = self
            .connections
            .lock()
            .unwrap()
            .insert(broker, ConnectionStatus::Connected(c.clone()));
        match old {
            Some(ConnectionStatus::Connected(c)) => {
                //info!("removing old connection");
            }
            Some(ConnectionStatus::Connecting(mut v)) => {
                //info!("was in connecting state({} waiting)", v.len());
                for tx in v.drain(..) {
                    let _ = tx.send(Ok(c.clone()));
                }
            }
            None => {
                //info!("setting up new connection");
            }
        };

        Ok(c)
    }
}
