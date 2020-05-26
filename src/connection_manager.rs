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

    pub async fn get_connection_from_url(
        &self,
        broker: Option<Url>,
    ) -> Option<(bool, Arc<Connection>)> {
        let res = match broker {
            None => {
                debug!("using the base connection for lookup, not through a proxy");
                let broker_address = BrokerAddress {
                    url: self.url.clone(),
                    broker_url: self.url.clone(),
                    proxy: false,
                };

                let rx = {
                    match self.connections.lock().unwrap().get_mut(&broker_address) {
                        None => return None,
                        Some(ConnectionStatus::Connecting(ref mut v)) => {
                            let (tx, rx) = oneshot::channel();
                            v.push(tx);
                            rx
                        }
                        Some(ConnectionStatus::Connected(c)) => return Some((false, c.clone())),
                    }
                };
                match rx.await {
                    Ok(Ok(c)) => Some((false, c)),
                    _ => None,
                }
                /*if let Some(c) = res
                 * };
                {
                    if c.is_valid() {
                        info!("base connection is still valid");
                        Some((false, c.clone()))
                    } else {
                        error!("invalid base connection({:?}), reconnecting", c.error());
                        let conn = loop {
                            let conn = Connection::new::<Exe>(self.url.clone(), self.auth.clone(), None).await.ok();
                            if conn.is_some() {
                                break conn.unwrap();
                            }
                            tokio::time::delay_for(std::time::Duration::from_millis(500)).await;
                            error!("trying again...");
                        };

                        let c = Arc::new(conn);
                        self.connections.lock().unwrap().insert(broker_address, ConnectionStatus::Connected(c.clone()));
                        Some((false, c))
                    }
                } else {
                    None
                }*/
            }
            Some(ref s) => {
                let (b, rx) = {
                    match self
                        .connections
                        .lock()
                        .unwrap()
                        .iter_mut()
                        .find(|(k, _)| &k.broker_url == s)
                    {
                        None => return None,
                        Some((b, ConnectionStatus::Connecting(ref mut v))) => {
                            let (tx, rx) = oneshot::channel();
                            v.push(tx);
                            (b.proxy, rx)
                        }
                        Some((b, ConnectionStatus::Connected(c))) => {
                            return Some((b.proxy, c.clone()))
                        }
                    }
                };

                match rx.await {
                    Ok(Ok(c)) => Some((b, c)),
                    _ => None,
                }
            }
        };
        res
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
