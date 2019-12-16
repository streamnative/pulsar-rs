use crate::connection::{Authentication, Connection};
use crate::error::ConnectionError;
use futures::{future::{self, Either}, sync::{mpsc, oneshot}, Future, Stream, Async};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::runtime::TaskExecutor;
use tokio::prelude::*;
use trust_dns_resolver::AsyncResolver;
use trust_dns_resolver::config::{ResolverConfig, ResolverOpts};
use url::Url;

/// holds connection information for a broker
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BrokerAddress {
    /// IP and port (using the proxy's if applicable)
    pub address: SocketAddr,
    /// pulsar URL for the broker
    pub broker_url: String,
    /// true if we're connecting through a proxy
    pub proxy: bool,
}

/// Look up broker addresses for topics and partitioned topics
///
/// The ConnectionManager object provides a single interface to start
/// interacting with a cluster. It will automatically follow redirects
/// or use a proxy, and aggregate broker connections
#[derive(Clone)]
pub struct ConnectionManager {
    tx: mpsc::UnboundedSender<Query>,
    pub address: String,
}

impl ConnectionManager {
    pub fn new(
        addr: String,
        auth: Option<Authentication>,
        executor: TaskExecutor,
    ) -> ConnectionManager {
        let tx = ConnectionManagerEngine::run(addr.clone(), auth, executor);
        ConnectionManager { tx, address: addr }
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    pub fn get_base_connection(
        &self,
    ) -> impl Future<Item = Arc<Connection>, Error = ConnectionError> {
        if self.tx.is_closed() {
            return Either::A(future::err(ConnectionError::Shutdown));
        }

        let (tx, rx) = oneshot::channel();
        if self.tx.unbounded_send(Query::Base(tx)).is_err() {
            return Either::A(future::err(ConnectionError::Shutdown));
        }

        Either::B(rx.map_err(|_| ConnectionError::Canceled).flatten())
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    pub fn get_connection(
        &self,
        broker: &BrokerAddress,
    ) -> impl Future<Item = Arc<Connection>, Error = ConnectionError> {
        if self.tx.is_closed() {
            return Either::A(future::err(ConnectionError::Shutdown));
        }

        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .unbounded_send(Query::Connect(broker.clone(), tx))
            .is_err()
        {
            return Either::A(future::err(ConnectionError::Shutdown));
        }

        Either::B(rx.map_err(|_| ConnectionError::Canceled).flatten())
    }

    pub fn get_connection_from_url(
        &self,
        broker: Option<String>,
    ) -> impl Future<Item = Option<(bool, Arc<Connection>)>, Error = ConnectionError> {
        if self.tx.is_closed() {
            return Either::A(future::err(ConnectionError::Shutdown));
        }

        let (tx, rx) = oneshot::channel();
        if self.tx.unbounded_send(Query::Get(broker, tx)).is_err() {
            return Either::A(future::err(ConnectionError::Shutdown));
        }

        Either::B(rx.map_err(|_| ConnectionError::Canceled).flatten())
    }

    pub fn resolve_dns(&self, url: String) -> impl Future<Item=SocketAddr, Error=ConnectionError> {
        if self.tx.is_closed() {
            return Either::A(future::err(ConnectionError::Shutdown));
        }

        let (tx, rx) = oneshot::channel();
        if self.tx.unbounded_send(Query::Dns(url, tx)).is_err() {
            return Either::A(future::err(ConnectionError::Shutdown));
        }

        Either::B(rx.map_err(|_| ConnectionError::Canceled).flatten())
    }
}

/// enum holding the service discovery query sent to the engine function
enum Query {
    Base(oneshot::Sender<Result<Arc<Connection>, ConnectionError>>),
    /// broker URL
    Get(
        Option<String>,
        oneshot::Sender<Result<Option<(bool, Arc<Connection>)>, ConnectionError>>,
    ),
    Connect(
        BrokerAddress,
        /// channel to send back the response
        oneshot::Sender<Result<Arc<Connection>, ConnectionError>>,
    ),
    Dns(
        /// URL
        String,
        oneshot::Sender<Result<SocketAddr, ConnectionError>>,
    )
}

struct ConnectionManagerEngine {
    addr: String,
    auth: Option<Authentication>,
    executor: TaskExecutor,
    receiver: mpsc::UnboundedReceiver<Query>,
    broker_connections: HashMap<BrokerAddress, Arc<Connection>>,
    pending_broker_connections: HashMap<BrokerAddress, Box<dyn Future<Item=Arc<Connection>, Error=ConnectionError> + Send>>,
    base_connection: Option<Arc<Connection>>,
    pending_base_connection: Option<Box<dyn Future<Item=Arc<Connection>, Error=ConnectionError> + Send>>,
    dns_resolver: AsyncResolver,
}

impl ConnectionManagerEngine {
    pub fn run(
        addr: String,
        auth: Option<Authentication>,
        executor: TaskExecutor
    ) -> mpsc::UnboundedSender<Query> {
        let (sender, receiver) = mpsc::unbounded();
        let (dns_resolver, resolver_future) =
            AsyncResolver::new(ResolverConfig::default(), ResolverOpts::default());
        executor.spawn(resolver_future);
        executor.spawn(ConnectionManagerEngine {
            addr,
            auth,
            executor: executor.clone(),
            receiver,
            broker_connections: HashMap::new(),
            pending_broker_connections: HashMap::new(),
            base_connection: None,
            pending_base_connection: None,
            dns_resolver
        });
        sender
    }

    fn connect_base(&self) -> Box<dyn Future<Item=Arc<Connection>, Error=ConnectionError> + Send> {
        let auth = self.auth.clone();
        let executor = self.executor.clone();
        Box::new(self.resolve_dns(self.addr.clone())
            .and_then(move |socket_addr: SocketAddr| {
                Connection::new(socket_addr, auth, None, executor)
                    .map(|c| Arc::new(c))
            }))
    }

    fn resolve_base_connection(&mut self, resolver: oneshot::Sender<Result<Arc<Connection>, ConnectionError>>) {
        if let Some(base_connection) = self.base_connection.clone() {
            let _ = resolver.send(Ok(base_connection));
        } else {
            let pending_base_connection = self.pending_base_connection.take().unwrap_or_else(|| self.connect_base());
            let pending_base_connection = Box::new(pending_base_connection.then(|r| {
                let _ = resolver.send(r.clone());
                r
            }));
            self.pending_base_connection = Some(pending_base_connection);
        }
    }

    fn connect_broker(&mut self, broker: BrokerAddress) -> Box<dyn Future<Item=Arc<Connection>, Error=ConnectionError> + Send> {
        let proxy_url = if broker.proxy {
            Some(broker.broker_url.clone())
        } else {
            None
        };

        Box::new(Connection::new(broker.address, self.auth.clone(), proxy_url, self.executor.clone())
             .map(|c| Arc::new(c)))
    }

    fn resolve_broker_connection(
        &mut self,
        broker: BrokerAddress,
        resolver: oneshot::Sender<Result<Arc<Connection>, ConnectionError>>
    ) {
        if let Some(conn) = self.broker_connections.get(&broker) {
            let _ = resolver.send(Ok(conn.clone()));
        } else {
            let conn = self.pending_broker_connections.remove(&broker).unwrap_or_else(|| self.connect_broker(broker.clone()));
            self.pending_broker_connections.insert(
                broker,
                Box::new(conn.then(|r| {
                    let _ = resolver.send(r.clone());
                    r
                }))
            );
        }
    }

    fn resolve_url_connection(
        &mut self,
        broker_url: &String,
        resolver: oneshot::Sender<Result<Option<(bool, Arc<Connection>)>, ConnectionError>>
    ) {
        if let Some((broker, conn)) = self.broker_connections
            .iter()
            .find(|(k, c)| &k.broker_url == broker_url && c.is_valid())
        {
            let _ = resolver.send(Ok(Some((broker.proxy, conn.clone()))));
        } else {
            if let Some(broker) = self.pending_broker_connections
                .keys()
                .find(|b| &b.broker_url == broker_url)
                .or_else(|| self.broker_connections.keys().find(|b| &b.broker_url == broker_url))
                .cloned()
            {
                debug!(
                    "using another connection for lookup, proxying to {:?}",
                    &broker.address
                );
                let conn = self.pending_broker_connections.remove(&broker)
                    .unwrap_or_else(|| self.connect_broker(broker.clone()));
                let proxy = broker.proxy;
                self.pending_broker_connections.insert(
                    broker,
                    Box::new(conn.then(move |r| {
                        let _ = resolver.send(r.clone().map(|c| Some((proxy, c))));
                        r
                    }))
                );
            } else {
                let _ = resolver.send(Ok(None));
            }
        }
    }

    fn resolve_dns(&self, url: String) -> impl Future<Item = SocketAddr, Error = ConnectionError> {
        let url = match Url::parse(&url).map_err(|e| ConnectionError::Url(e.to_string())) {
            Ok(url) => url,
            Err(e) => return Either::A(future::failed(e)),
        };
        let port = match url.port_or_known_default().ok_or_else(|| ConnectionError::Url(format!("Unable to parse port from {}", url))) {
            Ok(port) => port,
            Err(e) => return Either::A(future::failed(e)),
        };
        Either::B(self.dns_resolver
            .lookup_ip(url.as_str())
            .map_err(move |e| {
                error!("DNS lookup error: {:?}", e);
                ConnectionError::Dns(e.to_string())
            })
            .map(move |results| {
                SocketAddr::new(results.iter().next().unwrap(), port)
            }))
    }
}

impl Future for ConnectionManagerEngine {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        if let Some(mut new_base_connection) = self.pending_base_connection.take() {
            match new_base_connection.poll() {
                Ok(Async::Ready(connection)) => {
                    self.base_connection = Some(connection);
                }
                Ok(Async::NotReady) => {
                    self.pending_base_connection = Some(new_base_connection);
                }
                Err(_) => {
                    self.pending_base_connection = Some(self.connect_base());
                }
            }
        }
        let mut resolved_brokers = Vec::new();
        for (broker, pending_connection) in self.pending_broker_connections.iter_mut() {
            match pending_connection.poll() {
                Ok(Async::Ready(conn)) => {
                    self.broker_connections.insert(broker.clone(), conn);
                    resolved_brokers.push(broker.clone());
                }
                Err(_) => {
                    resolved_brokers.push(broker.clone());
                }
                Ok(Async::NotReady) => {}
            }
        }
        for broker in resolved_brokers {
            self.pending_broker_connections.remove(&broker);
        }
        loop {
            match try_ready!(self.receiver.poll()) {
                Some(Query::Base(tx)) => {
                    self.resolve_base_connection(tx);
                }
                Some(Query::Get(None, tx)) => {
                    debug!("using the base connection for lookup, not through a proxy");
                    let (sender, receiver) = oneshot::channel();
                    self.resolve_base_connection(sender);
                    tokio::spawn(
                        receiver
                            .map_err(drop)
                            .map(move |r| {
                                let _ = tx.send(r.map(|c| Some((false, c))));
                            })
                    );
                }
                Some(Query::Get(Some(url), tx)) => {
                    self.resolve_url_connection(&url, tx);
                }
                Some(Query::Connect(broker, tx)) => {
                    self.resolve_broker_connection(broker, tx);
                }
                Some(Query::Dns(url, tx)) => {
                    tokio::spawn(self.resolve_dns(url).then(|r| {
                        let _ = tx.send(r);
                        Ok(())
                    }));
                }
                None => return Err(()),
            }
        }
    }
}
