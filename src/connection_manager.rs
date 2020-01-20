use crate::connection::{Authentication, Connection};
use crate::error::ConnectionError;
use crate::executor::TaskExecutor;
use futures::{
    future::{self, Either},
    sync::{mpsc, oneshot},
    Future, Stream,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

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
    pub address: SocketAddr,
}

impl ConnectionManager {
    pub fn new(
        addr: SocketAddr,
        auth: Option<Authentication>,
        executor: TaskExecutor,
    ) -> impl Future<Item = Self, Error = ConnectionError> {
        Connection::new(addr.to_string(), auth.clone(), None, executor.clone())
            .map_err(|e| e.into())
            .and_then(move |conn| ConnectionManager::from_connection(conn, auth, addr, executor))
    }

    pub fn from_connection(
        connection: Connection,
        auth: Option<Authentication>,
        address: SocketAddr,
        executor: TaskExecutor,
    ) -> Result<ConnectionManager, ConnectionError> {
        let tx = engine(Arc::new(connection), auth, executor);
        Ok(ConnectionManager { tx, address })
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
    Connected(
        BrokerAddress,
        Connection,
        /// channel to send back the response
        oneshot::Sender<Result<Arc<Connection>, ConnectionError>>,
    ),
}

/// core of the service discovery
///
/// this function loops over the query channel and launches lookups.
/// It can send a message to itself for further queries if necessary.
fn engine(
    connection: Arc<Connection>,
    auth: Option<Authentication>,
    executor: TaskExecutor,
) -> mpsc::UnboundedSender<Query> {
    let (tx, rx) = mpsc::unbounded();
    let mut connections: HashMap<BrokerAddress, Arc<Connection>> = HashMap::new();
    let executor2 = executor.clone();
    let tx2 = tx.clone();

    let f = move || {
        rx.for_each(move |query: Query| {
            let exe = executor2.clone();
            let self_tx = tx2.clone();

            match query {
                Query::Connect(broker, tx) => Either::A(match connections.get(&broker) {
                    Some(conn) => {
                        let _ = tx.send(Ok(conn.clone()));
                        Either::A(future::ok(()))
                    }
                    None => Either::B(connect(broker, auth.clone(), tx, self_tx, exe)),
                }),
                Query::Base(tx) => {
                    let _ = tx.send(Ok(connection.clone()));
                    Either::B(future::ok(()))
                }
                Query::Connected(broker, conn, tx) => {
                    let c = Arc::new(conn);
                    connections.insert(broker, c.clone());
                    let _ = tx.send(Ok(c));
                    Either::B(future::ok(()))
                }
                Query::Get(url_opt, tx) => {
                    let res = match url_opt {
                        None => {
                            debug!("using the base connection for lookup, not through a proxy");
                            Some((false, connection.clone()))
                        }
                        Some(ref s) => {
                            if let Some((b, c)) =
                                connections.iter().find(|(k, _)| &k.broker_url == s)
                            {
                                debug!(
                                    "using another connection for lookup, proxying to {:?}",
                                    b.proxy
                                );
                                Some((b.proxy, c.clone()))
                            } else {
                                None
                            }
                        }
                    };
                    let _ = tx.send(Ok(res));
                    Either::B(future::ok(()))
                }
            }
        })
        .map_err(|_| {
            error!("service discovery engine stopped");
            ()
        })
    };

    executor.spawn(f());

    tx
}

fn connect(
    broker: BrokerAddress,
    auth: Option<Authentication>,
    tx: oneshot::Sender<Result<Arc<Connection>, ConnectionError>>,
    self_tx: mpsc::UnboundedSender<Query>,
    exe: TaskExecutor,
) -> impl Future<Item = (), Error = ()> {
    let proxy_url = if broker.proxy {
        Some(broker.broker_url.clone())
    } else {
        None
    };

    Connection::new(broker.address.to_string(), auth, proxy_url, exe).then(move |res| {
        match res {
            Ok(conn) => match self_tx.unbounded_send(Query::Connected(broker, conn, tx)) {
                Err(e) => match e.into_inner() {
                    Query::Connected(_, _, tx) => {
                        let _ = tx.send(Err(ConnectionError::Shutdown));
                    }
                    _ => {}
                },
                Ok(_) => {}
            },
            Err(e) => {
                let _ = tx.send(Err(e));
            }
        };
        future::ok(())
    })
}
