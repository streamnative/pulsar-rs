use crate::connection::{Authentication, Connection, ConnectionOptions, ConnectionHandle};
use crate::error::{ConnectionError, Error};
use futures::{
    future::{self, Either},
    sync::{mpsc, oneshot},
    Future, Stream,
};
use std::collections::{HashMap, BTreeMap};
use std::time::Duration;
use futures::channel::mpsc;
use crate::util::SerialId;
use futures::task::{Poll, Context};
use crate::resolver::Resolver;


#[derive(Debug, Clone)]
struct ConnectionMetadata {
    address: String,
}

enum ConnectionRequest {
    Address(String, Resolver<ConnectionHandle>),
}

#[derive(Debug, Clone, Default)]
pub struct ConnectionManagerOptions {
    pub proactive_reconnect: Option<bool>,
    pub reconnect_backoff: Option<Duration>,
}

pub struct ConnectionManager {
    base_connection_address: String,
    connection_options: Option<ConnectionOptions>, // is there a use for these to be conn-specific?
    base_connection: u64,
    pending: BTreeMap<u64, Box<dyn Future<Output=Result<Connection, Error>>>>,
    connections: BTreeMap<u64, Connection>,
    metadata: BTreeMap<u64, ConnectionMetadata>,
    requests: mpsc::UnboundedReceiver<ConnectionManagerMessage>,
    sender: mpsc::UnboundedSender<ConnectionManagerMessage>,
    connection_ids: SerialId,
    proactive_reconnect: bool,
    reconnect_backoff: Option<Duration>, //TODO make backoff only happen after a failure
}

impl ConnectionManager {
    fn new(
        addr: String,
        conn_options: Option<ConnectionOptions>,
        manager_options: Option<ConnectionManagerOptions>,
    ) -> ConnectionManager {
        let mut connection_ids = SerialId::new();
        let base_connection_id = connection_ids.next();
        let base_conn = Connection::connect(addr.clone(), options.clone());
        let mut pending = BTreeMap::new();
        let mut metadata = BTreeMap::new();
        metadata.insert(base_connection_id, ConnectionMetadata { address: addr.clone() });
        pending.insert(base_connection_id, Box::new(base_conn));
        let (sender, requests) = mpsc::unbounded();
        let manager_options = manager_options.unwrap_or_default();
        ConnectionManager {
            base_connection_address: addr,
            connection_options: options,
            base_connection: base_connection_id,
            pending,
            metadata,
            connections: Default::default(),
            requests,
            sender,
            connection_ids,
            proactive_reconnect: manager_options.proactive_reconnect.unwrap_or(false),
            reconnect_backoff: manager_options.reconnect_backoff,
        }
    }

    pub fn handle(&self) -> ConnectionManagerHandle {
        ConnectionManagerHandle { sender: self.sender.clone() }
    }

    fn get_base_connection(&mut self) -> Poll<ConnectionHandle> {
        if let Some(conn)  = self.connections.get(&self.base_connection) {
            Poll::Ready(conn.handle())
        } else if self.pending.contains_key(&self.base_connection) {
            Poll::Pending
        } else {
            let conn = Connection::connect(self.base_connection_address.clone(), self.connection_options.clone());
            self.pending.insert(self.base_connection, conn);
            Poll::Pending
        }
    }

    fn poll_pending(&mut self, cx: &mut Context<'_>) {
        let mut resolved = Vec::new();
        let mut failed = Vec::new();
        for (id, pending) in &mut self.pending {
            match pending.poll(cx) {
                Poll::Ready(Ok(conn)) => resolved.push((*id, conn)),
                Poll::Ready(Err(e)) => failed.push(*id),
                Poll::Pending => {}
            }
        }
        for (id, resolved) in resolved {
            self.pending.remove(&id);
            self.connections.insert(id, resolved);
        }
        for id in failed {
            self.pending.remove(&id);
            if self.proactive_reconnect {
                let metadata = self.metadata.get(&id).unwrap();
                let conn = Connection::connect(metadata.address.clone(), self.connection_options.clone());
                let conn = if let Some(delay) = self.reconnect_backoff {
                    Box::new(delay_for(backoff).and_then(move |_| conn))
                } else {
                    Box::new(conn)
                };
                self.pending.insert(id, conn);
            }
        }
    }
    fn poll_connections(&mut self, cx: &mut Context<'_>) {
        let mut disconnected = Vec::new();
        for (id, conn) in &mut self.connections {
            match conn.poll(cx) {
                Poll::Ready(Ok(())) => {
                    error!("Connection unexpectedly closed");
                    disconnected.push(*id);
                },
                Poll::Ready(Err(e)) => {
                    error!("Connection error: {}", e);
                    disconnected.push(*id);
                },
                Poll::Pending => {}
            }
        }
        for id in disconnected {
            let metadata = self.metadata.get(&id).unwrap();
            self.connections.remove(&id);
            if self.proactive_reconnect {
                let conn = Connection::connect(metadata.address.clone(), self.connection_options.clone());
                self.pending.insert(id, Box::new(conn));
            }
        }
    }

    fn poll_requests(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some(request)) = self.requests {
            match request {
                ConnectionRequest::Address(addr, resolver) => {
                    let resolved = self.metadata.iter()
                        .find(|(_, m)| m.address.as_str() == addr.as_str())
                        .map(|(id, _)| *id);
                    if let Some(id) = resolved {
                        if let Some(conn) = self.connections.get(&id) {
                            resolver.resolve(Ok(conn.handle()));
                            continue;
                        }
                        if let Some(pending) = self.pending.remove(&id) {
                            self.pending.insert(id, Box::new(pending.map(move |conn| {
                                resolver.resolve(conn.as_ref()
                                    .map(|c| c.handle())
                                    .map_err(|e| e.clone())
                                );
                                conn
                            })));
                            continue
                        }
                        warn!("connection metadata found where no resolved or pending connection exists");
                    }
                    let id = self.connection_ids.next();
                    let conn = Connection::connect(addr.clone(), self.connection_options.clone());
                    self.metadata.insert(id, ConnectionMetadata {
                        address: addr.clone(),
                    });
                    self.pending.insert(id, Box::new(conn.map(move |conn| {
                        resolver.resolve(conn.as_ref()
                            .map(|c| c.handle())
                            .map_err(|e| e.clone())
                        );
                        conn
                    })));
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ConnectionManagerHandle {
    sender: mpsc::UnboundedSender<ConnectionRequest>
}

impl ConnectionManagerHandle {
    async fn get_conn(&self, addr: String) -> Result<ConnectionHandle, Error> {
        let (resolver, f) = Resolver::new();
        self.sender.send(ConnectionRequest::Address(addr, resolver))
            .map_err(Error::unexpected("connection manager not running"))?;
        resolver.await
    }
}
//
///// holds connection information for a broker
//#[derive(Debug, Clone, Hash, PartialEq, Eq)]
//pub struct BrokerAddress {
//    /// IP and port (using the proxy's if applicable)
//    pub address: SocketAddr,
//    /// pulsar URL for the broker
//    pub broker_url: String,
//    /// true if we're connecting through a proxy
//    pub proxy: bool,
//}
//
///// Look up broker addresses for topics and partitioned topics
/////
///// The ConnectionManager object provides a single interface to start
///// interacting with a cluster. It will automatically follow redirects
///// or use a proxy, and aggregate broker connections
//#[derive(Clone)]
//pub struct ConnectionManager {
//    tx: mpsc::UnboundedSender<Query>,
//    pub address: SocketAddr,
//}
//
//impl ConnectionManager {
//    pub fn new(
//        addr: SocketAddr,
//        auth: Option<Authentication>,
//        executor: TaskExecutor,
//    ) -> impl Future<Item = Self, Error = ConnectionError> {
//        Connection::new(addr.to_string(), auth.clone(), None, executor.clone())
//            .map_err(|e| e.into())
//            .and_then(move |conn| ConnectionManager::from_connection(conn, auth, addr, executor))
//    }
//
//    pub fn from_connection(
//        connection: Connection,
//        auth: Option<Authentication>,
//        address: SocketAddr,
//        executor: TaskExecutor,
//    ) -> Result<ConnectionManager, ConnectionError> {
//        let tx = engine(Arc::new(connection), auth, executor);
//        Ok(ConnectionManager { tx, address })
//    }
//
//    /// get an active Connection from a broker address
//    ///
//    /// creates a connection if not available
//    pub fn get_base_connection(
//        &self,
//    ) -> impl Future<Item = Arc<Connection>, Error = ConnectionError> {
//        if self.tx.is_closed() {
//            return Either::A(future::err(ConnectionError::Shutdown));
//        }
//
//        let (tx, rx) = oneshot::channel();
//        if self.tx.unbounded_send(Query::Base(tx)).is_err() {
//            return Either::A(future::err(ConnectionError::Shutdown));
//        }
//
//        Either::B(rx.map_err(|_| ConnectionError::Canceled).flatten())
//    }
//
//    /// get an active Connection from a broker address
//    ///
//    /// creates a connection if not available
//    pub fn get_connection(
//        &self,
//        broker: &BrokerAddress,
//    ) -> impl Future<Item = Arc<Connection>, Error = ConnectionError> {
//        if self.tx.is_closed() {
//            return Either::A(future::err(ConnectionError::Shutdown));
//        }
//
//        let (tx, rx) = oneshot::channel();
//        if self
//            .tx
//            .unbounded_send(Query::Connect(broker.clone(), tx))
//            .is_err()
//        {
//            return Either::A(future::err(ConnectionError::Shutdown));
//        }
//
//        Either::B(rx.map_err(|_| ConnectionError::Canceled).flatten())
//    }
//
//    pub fn get_connection_from_url(
//        &self,
//        broker: Option<String>,
//    ) -> impl Future<Item = Option<(bool, Arc<Connection>)>, Error = ConnectionError> {
//        if self.tx.is_closed() {
//            return Either::A(future::err(ConnectionError::Shutdown));
//        }
//
//        let (tx, rx) = oneshot::channel();
//        if self.tx.unbounded_send(Query::Get(broker, tx)).is_err() {
//            return Either::A(future::err(ConnectionError::Shutdown));
//        }
//
//        Either::B(rx.map_err(|_| ConnectionError::Canceled).flatten())
//    }
//}
//
///// enum holding the service discovery query sent to the engine function
//enum Query {
//    Base(oneshot::Sender<Result<Arc<Connection>, ConnectionError>>),
//    /// broker URL
//    Get(
//        Option<String>,
//        oneshot::Sender<Result<Option<(bool, Arc<Connection>)>, ConnectionError>>,
//    ),
//    Connect(
//        BrokerAddress,
//        /// channel to send back the response
//        oneshot::Sender<Result<Arc<Connection>, ConnectionError>>,
//    ),
//    Connected(
//        BrokerAddress,
//        Connection,
//        /// channel to send back the response
//        oneshot::Sender<Result<Arc<Connection>, ConnectionError>>,
//    ),
//}
//
///// core of the service discovery
/////
///// this function loops over the query channel and launches lookups.
///// It can send a message to itself for further queries if necessary.
//fn engine(
//    connection: Arc<Connection>,
//    auth: Option<Authentication>,
//    executor: TaskExecutor,
//) -> mpsc::UnboundedSender<Query> {
//    let (tx, rx) = mpsc::unbounded();
//    let mut connections: HashMap<BrokerAddress, Arc<Connection>> = HashMap::new();
//    let executor2 = executor.clone();
//    let tx2 = tx.clone();
//
//    let f = move || {
//        rx.for_each(move |query: Query| {
//            let exe = executor2.clone();
//            let self_tx = tx2.clone();
//
//            match query {
//                Query::Connect(broker, tx) => Either::A(match connections.get(&broker) {
//                    Some(conn) => {
//                        let _ = tx.send(Ok(conn.clone()));
//                        Either::A(future::ok(()))
//                    }
//                    None => Either::B(connect(broker, auth.clone(), tx, self_tx, exe)),
//                }),
//                Query::Base(tx) => {
//                    let _ = tx.send(Ok(connection.clone()));
//                    Either::B(future::ok(()))
//                }
//                Query::Connected(broker, conn, tx) => {
//                    let c = Arc::new(conn);
//                    connections.insert(broker, c.clone());
//                    let _ = tx.send(Ok(c));
//                    Either::B(future::ok(()))
//                }
//                Query::Get(url_opt, tx) => {
//                    let res = match url_opt {
//                        None => {
//                            debug!("using the base connection for lookup, not through a proxy");
//                            Some((false, connection.clone()))
//                        }
//                        Some(ref s) => {
//                            if let Some((b, c)) =
//                                connections.iter().find(|(k, _)| &k.broker_url == s)
//                            {
//                                debug!(
//                                    "using another connection for lookup, proxying to {:?}",
//                                    b.proxy
//                                );
//                                Some((b.proxy, c.clone()))
//                            } else {
//                                None
//                            }
//                        }
//                    };
//                    let _ = tx.send(Ok(res));
//                    Either::B(future::ok(()))
//                }
//            }
//        })
//        .map_err(|_| {
//            error!("service discovery engine stopped");
//            ()
//        })
//    };
//
//    executor.spawn(f());
//
//    tx
//}
//
//fn connect(
//    broker: BrokerAddress,
//    auth: Option<Authentication>,
//    tx: oneshot::Sender<Result<Arc<Connection>, ConnectionError>>,
//    self_tx: mpsc::UnboundedSender<Query>,
//    exe: TaskExecutor,
//) -> impl Future<Item = (), Error = ()> {
//    let proxy_url = if broker.proxy {
//        Some(broker.broker_url.clone())
//    } else {
//        None
//    };
//
//    Connection::new(broker.address.to_string(), auth, proxy_url, exe).then(move |res| {
//        match res {
//            Ok(conn) => match self_tx.unbounded_send(Query::Connected(broker, conn, tx)) {
//                Err(e) => match e.into_inner() {
//                    Query::Connected(_, _, tx) => {
//                        let _ = tx.send(Err(ConnectionError::Shutdown));
//                    }
//                    _ => {}
//                },
//                Ok(_) => {}
//            },
//            Err(e) => {
//                let _ = tx.send(Err(e));
//            }
//        };
//        future::ok(())
//    })
//}
