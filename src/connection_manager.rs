use crate::connection::{Authentication, Connection};
use crate::error::ConnectionError;
use crate::executor::{Executor, TaskExecutor};
use futures::{
    channel::{mpsc, oneshot},
    StreamExt,
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
    pub async fn new<E: Executor + 'static>(
        addr: SocketAddr,
        auth: Option<Authentication>,
        executor: E,
    ) -> Result<Self, ConnectionError> {
        let executor = TaskExecutor::new(executor);
        let conn = Connection::new(addr.to_string(), auth.clone(), None, executor.clone()).await?;
        ConnectionManager::from_connection(conn, auth, addr, executor)
    }

    pub fn from_connection<E: Executor + 'static>(
        connection: Connection,
        auth: Option<Authentication>,
        address: SocketAddr,
        executor: E,
    ) -> Result<ConnectionManager, ConnectionError> {
        let executor = TaskExecutor::new(executor);
        let tx = engine(Arc::new(connection), auth, executor);
        Ok(ConnectionManager { tx, address })
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    pub async fn get_base_connection(&self) -> Result<Arc<Connection>, ConnectionError> {
        if self.tx.is_closed() {
            return Err(ConnectionError::Shutdown);
        }

        let (tx, rx) = oneshot::channel();
        if self.tx.unbounded_send(Query::Base(tx)).is_err() {
            return Err(ConnectionError::Shutdown);
        }

        rx.await.map_err(|_| ConnectionError::Canceled)?
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    pub async fn get_connection(
        &self,
        broker: &BrokerAddress,
    ) -> Result<Arc<Connection>, ConnectionError> {
        if self.tx.is_closed() {
            return Err(ConnectionError::Shutdown);
        }

        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .unbounded_send(Query::Connect(broker.clone(), tx))
            .is_err()
        {
            return Err(ConnectionError::Shutdown);
        }

        rx.await.map_err(|_| ConnectionError::Canceled)?
    }

    pub async fn get_connection_from_url(
        &self,
        broker: Option<String>,
    ) -> Result<Option<(bool, Arc<Connection>)>, ConnectionError> {
        if self.tx.is_closed() {
            return Err(ConnectionError::Shutdown);
        }

        let (tx, rx) = oneshot::channel();
        if self.tx.unbounded_send(Query::Get(broker, tx)).is_err() {
            return Err(ConnectionError::Shutdown);
        }

        rx.await.map_err(|_| ConnectionError::Canceled)?
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

/// core of the connection manager
///
/// this function loops over the query channel and creates connections on demand
/// It can send a message to itself for further queries if necessary.
fn engine(
    connection: Arc<Connection>,
    auth: Option<Authentication>,
    executor: TaskExecutor,
) -> mpsc::UnboundedSender<Query> {
    let (tx, mut rx) = mpsc::unbounded();
    let mut connections: HashMap<BrokerAddress, Arc<Connection>> = HashMap::new();
    let executor2 = executor.clone();
    let tx2 = tx.clone();

    let f = async move {
        while let Some(query) = rx.next().await {
            let exe = executor2.clone();
            let self_tx = tx2.clone();

            match query {
                Query::Connect(broker, tx) => match connections.get(&broker) {
                    Some(conn) => {
                        let _ = tx.send(Ok(conn.clone()));
                    }
                    None => {
                        connect(broker, auth.clone(), tx, self_tx, exe).await;
                    }
                },
                Query::Base(tx) => {
                    let _ = tx.send(Ok(connection.clone()));
                }
                Query::Connected(broker, conn, tx) => {
                    let c = Arc::new(conn);
                    connections.insert(broker, c.clone());
                    let _ = tx.send(Ok(c));
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
                }
            }
        }
    };

    if let Err(_) = executor.spawn(Box::pin(f)) {
        error!("the executor could not spawn the Connection Manager engine future");
    }

    tx
}

async fn connect(
    broker: BrokerAddress,
    auth: Option<Authentication>,
    tx: oneshot::Sender<Result<Arc<Connection>, ConnectionError>>,
    self_tx: mpsc::UnboundedSender<Query>,
    exe: TaskExecutor,
) {
    let proxy_url = if broker.proxy {
        Some(broker.broker_url.clone())
    } else {
        None
    };

    match Connection::new(broker.address.to_string(), auth, proxy_url, exe).await {
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
    }
}
