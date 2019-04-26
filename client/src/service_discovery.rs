use crate::connection::{Authentication, Connection, ConnectionSender};
use crate::error::{ConnectionError, ServiceDiscoveryError};
use crate::message::proto::{command_lookup_topic_response, CommandLookupTopicResponse};
use futures::{
    future::{self, join_all, Either},
    sync::{mpsc, oneshot},
    Future, Stream,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::runtime::TaskExecutor;
use trust_dns_resolver::config::*;
use trust_dns_resolver::AsyncResolver;
use url::Url;

/// holds connection information for a broker
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BrokerAddress {
    /// IP and port (using the proxy's if applicable)
    address: SocketAddr,
    /// pulsar URL for the broker
    broker_url: String,
    /// true if we're connecting through a proxy
    proxy: bool,
}

/// Look up broker addresses for topics and partitioned topics
///
/// The ServiceDiscovery object provides a single interface to start
/// interacting with a cluster. It will automatically follow redirects
/// or use a proxy, and aggregate broker connections
#[derive(Clone)]
pub struct ServiceDiscovery {
    tx: mpsc::UnboundedSender<Query>,
}

impl ServiceDiscovery {
    pub fn new(
        addr: SocketAddr,
        auth: Option<Authentication>,
        executor: TaskExecutor,
    ) -> impl Future<Item = Self, Error = ServiceDiscoveryError> {
        Connection::new(addr.to_string(), auth.clone(), None, executor.clone())
            .map_err(|e| e.into())
            .and_then(move |conn| ServiceDiscovery::from_connection(conn, auth, addr, executor))
    }

    pub fn from_connection(
        connection: Connection,
        auth: Option<Authentication>,
        address: SocketAddr,
        executor: TaskExecutor,
    ) -> Result<ServiceDiscovery, ServiceDiscoveryError> {
        let tx = engine(connection, auth, address, executor);
        Ok(ServiceDiscovery { tx })
    }

    /// get the broker address for a topic
    pub fn lookup_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> impl Future<Item = BrokerAddress, Error = ServiceDiscoveryError> {
        if self.tx.is_closed() {
            return Either::A(future::err(ServiceDiscoveryError::Shutdown));
        }

        Either::B(lookup_topic(topic, self.tx.clone()))
    }

    /// get the number of partitions for a partitioned topic
    pub fn lookup_partitioned_topic_number<S: Into<String>>(
        &self,
        topic: S,
    ) -> impl Future<Item = u32, Error = ServiceDiscoveryError> {
        if self.tx.is_closed() {
            return Either::A(future::err(ServiceDiscoveryError::Shutdown));
        }

        let (tx, rx) = oneshot::channel();
        let topic: String = topic.into();
        if self
            .tx
            .unbounded_send(Query::PartitionedTopic(topic.clone(), tx))
            .is_err()
        {
            return Either::A(future::err(ServiceDiscoveryError::Shutdown));
        }

        Either::B(rx.map_err(|_| ServiceDiscoveryError::Canceled).flatten())
    }

    /// get the list of topic names and addresses for a partitioned topic
    pub fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> impl Future<Item = Vec<(String, BrokerAddress)>, Error = ServiceDiscoveryError> {
        if self.tx.is_closed() {
            return Either::A(future::err(ServiceDiscoveryError::Shutdown));
        }

        let (tx, rx) = oneshot::channel();
        let topic: String = topic.into();

        if self
            .tx
            .unbounded_send(Query::PartitionedTopic(topic.clone(), tx))
            .is_err()
        {
            return Either::A(future::err(ServiceDiscoveryError::Shutdown));
        }

        let self_tx = self.tx.clone();

        Either::B(
            rx.map_err(|_| ServiceDiscoveryError::Canceled)
                .and_then(move |res| match res {
                    Err(e) => Either::A(future::err(e)),
                    Ok(partitions) => {
                        let topics = (0..partitions)
                            .map(|nb| {
                                let t = format!("{}-partition-{}", topic, nb);
                                lookup_topic(t.clone(), self_tx.clone())
                                    .map(move |address| (t, address))
                            })
                            .collect::<Vec<_>>();

                        Either::B(join_all(topics))
                    }
                }),
        )
    }

    /// get an active Connection from a broker address
    ///
    /// creates a connection if not available
    pub fn get_connection(
        &self,
        broker: &BrokerAddress,
    ) -> impl Future<Item = Arc<Connection>, Error = ServiceDiscoveryError> {
        if self.tx.is_closed() {
            return Either::A(future::err(ServiceDiscoveryError::Shutdown));
        }

        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .unbounded_send(Query::Connect(broker.clone(), tx))
            .is_err()
        {
            return Either::A(future::err(ServiceDiscoveryError::Shutdown));
        }

        Either::B(
            rx.map_err(|_| ServiceDiscoveryError::Canceled)
                .and_then(|res| match res {
                    Ok(conn) => Ok(conn),
                    Err(e) => Err(ServiceDiscoveryError::Connection(e)),
                }),
        )
    }
}

/// enum holding the service discovery query sent to the engine function
enum Query {
    Topic(
        /// topic
        String,
        /// broker url
        Option<String>,
        /// authoritative
        bool,
        /// channel to send back the response
        oneshot::Sender<Result<BrokerAddress, ServiceDiscoveryError>>,
    ),
    PartitionedTopic(
        /// topic
        String,
        /// channel to send back the response
        oneshot::Sender<Result<u32, ServiceDiscoveryError>>,
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

/// helper function for topic lookup
///
/// connects to the target broker if necessary
fn lookup_topic<S: Into<String>>(
    topic: S,
    self_tx: mpsc::UnboundedSender<Query>,
) -> impl Future<Item = BrokerAddress, Error = ServiceDiscoveryError> {
    let (tx, rx) = oneshot::channel();
    let topic: String = topic.into();
    if self_tx
        .unbounded_send(Query::Topic(topic.clone(), None, false, tx))
        .is_err()
    {
        return Either::A(future::err(ServiceDiscoveryError::Shutdown));
    }

    Either::B(
        rx.map_err(|_| ServiceDiscoveryError::Canceled)
            .flatten()
            .and_then(move |broker| {
                let (tx, rx) = oneshot::channel();
                if self_tx
                    .unbounded_send(Query::Connect(broker.clone(), tx))
                    .is_err()
                {
                    return Either::A(future::err(ServiceDiscoveryError::Shutdown));
                }

                Either::B(rx.map_err(|_| ServiceDiscoveryError::Canceled).and_then(
                    |res| match res {
                        Err(e) => Err(e.into()),
                        Ok(_) => Ok(broker),
                    },
                ))
            }),
    )
}

/// core of the service discovery
///
/// this function loops over the query channel and launches lookups.
/// It can send a message to itself for further queries if necessary.
fn engine(
    connection: Connection,
    auth: Option<Authentication>,
    address: SocketAddr,
    executor: TaskExecutor,
) -> mpsc::UnboundedSender<Query> {
    let (tx, rx) = mpsc::unbounded();
    let mut connections: HashMap<BrokerAddress, Arc<Connection>> = HashMap::new();
    let executor2 = executor.clone();
    let tx2 = tx.clone();
    let (resolver, resolver_future) =
        AsyncResolver::new(ResolverConfig::default(), ResolverOpts::default());
    executor.spawn(resolver_future);

    let f = move || {
        rx.for_each(move |query: Query| {
            let exe = executor2.clone();
            let self_tx = tx2.clone();
            let base_address = address.clone();
            let resolver = resolver.clone();

            match query {
                Query::Topic(topic, broker_url, authoritative, tx) => Either::A(Either::A({
                    let conn_info = match broker_url {
                        None => {
                            debug!("using the base connection for lookup, not through a proxy");
                            Some((false, connection.sender()))
                        }
                        Some(ref s) => {
                            if let Some((b, c)) =
                                connections.iter().find(|(k, _)| &k.broker_url == s)
                            {
                                debug!(
                                    "using another connection for lookup, proxying to {:?}",
                                    b.proxy
                                );
                                Some((b.proxy, c.sender()))
                            } else {
                                None
                            }
                        }
                    };

                    if let Some((proxied_query, sender)) = conn_info {
                        Either::A(lookup(
                            topic.to_string(),
                            proxied_query,
                            sender,
                            resolver,
                            base_address,
                            authoritative,
                            tx,
                            self_tx.clone(),
                        ))
                    } else {
                        let _ = tx.send(Err(ServiceDiscoveryError::Query(format!(
                            "unknown broker URL: {}",
                            broker_url.unwrap_or_else(String::new)
                        ))));
                        Either::B(future::ok(()))
                    }
                })),
                Query::PartitionedTopic(topic, tx) => Either::A(Either::B(
                    connection
                        .sender()
                        .lookup_partitioned_topic(topic)
                        .then(|res| {
                            match res {
                                Err(e) => {
                                    let _ = tx.send(Err(e.into()));
                                }
                                Ok(response) => {
                                    let _ = match response.partitions {
                                        Some(partitions) => tx.send(Ok(partitions)),
                                        None => {
                                            if let Some(s) = response.message {
                                                tx.send(Err(ServiceDiscoveryError::Query(s)))
                                            } else {
                                                tx.send(Err(ServiceDiscoveryError::Query(format!(
                                                    "server error: {:?}",
                                                    response.error
                                                ))))
                                            }
                                        }
                                    };
                                }
                            }
                            future::ok(())
                        }),
                )),
                Query::Connect(broker, tx) => {
                    Either::B(Either::A(match connections.get(&broker) {
                        Some(conn) => {
                            let _ = tx.send(Ok(conn.clone()));
                            Either::A(future::ok(()))
                        }
                        None => Either::B(connect(broker, auth.clone(), tx, self_tx, exe)),
                    }))
                }
                Query::Connected(broker, conn, tx) => {
                    let c = Arc::new(conn);
                    connections.insert(broker, c.clone());
                    let _ = tx.send(Ok(c));
                    Either::B(Either::B(future::ok(())))
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

struct LookupResponse {
    pub broker_name: String,
    pub broker_url: String,
    pub broker_port: u16,
    pub proxy: bool,
    pub redirect: bool,
    pub authoritative: bool,
}

/// extracts information from a lookup response
fn convert_lookup_response(
    response: &CommandLookupTopicResponse,
) -> Result<LookupResponse, ServiceDiscoveryError> {
    if response.response.is_none()
        || response.response == Some(command_lookup_topic_response::LookupType::Failed as i32)
    {
        if let Some(ref s) = response.message {
            return Err(ServiceDiscoveryError::Query(s.to_string()));
        } else {
            return Err(ServiceDiscoveryError::Query(format!(
                "server error: {:?}",
                response.error.unwrap()
            )));
        }
    }

    let proxy = response.proxy_through_service_url.unwrap_or(false);

    // FIXME: only using the plaintext url for now
    let url = Url::parse(
        response
            .broker_service_url
            .as_ref()
            .ok_or(ServiceDiscoveryError::NotFound)?,
    )
    .map_err(|_| ServiceDiscoveryError::NotFound)?;
    let broker_name = url
        .host_str()
        .ok_or(ServiceDiscoveryError::NotFound)?
        .to_string();
    let broker_url = if url.port().is_some() {
        format!(
            "{}:{}",
            url.host().ok_or(ServiceDiscoveryError::NotFound)?,
            url.port().ok_or(ServiceDiscoveryError::NotFound)?
        )
    } else {
        url.host()
            .ok_or(ServiceDiscoveryError::NotFound)?
            .to_string()
    };
    let broker_port = url.port().unwrap_or(6650);
    let authoritative = response.authoritative.unwrap_or(false);
    let redirect =
        response.response == Some(command_lookup_topic_response::LookupType::Redirect as i32);

    Ok(LookupResponse {
        broker_name,
        broker_url,
        broker_port,
        proxy,
        redirect,
        authoritative,
    })
}

fn lookup(
    topic: String,
    proxied_query: bool,
    sender: &ConnectionSender,
    resolver: AsyncResolver,
    base_address: SocketAddr,
    authoritative: bool,
    tx: oneshot::Sender<Result<BrokerAddress, ServiceDiscoveryError>>,
    self_tx: mpsc::UnboundedSender<Query>,
) -> impl Future<Item = (), Error = ()> {
    sender
        .lookup_topic(topic.to_string(), authoritative)
        .then(move |res| {
            match res {
                Err(e) => {
                    let _ = tx.send(Err(ServiceDiscoveryError::Connection(e)));
                    Either::A(future::ok(()))
                }

                Ok(response) => {
                    let LookupResponse {
                        broker_name,
                        broker_url,
                        broker_port,
                        proxy,
                        redirect,
                        authoritative,
                    } = match convert_lookup_response(&response) {
                        Ok(info) => info,
                        Err(e) => {
                            let _ = tx.send(Err(e));
                            return Either::A(future::ok(()));
                        }
                    };

                    // get the IP and port for the broker_name
                    // if going through a proxy, we use the base address,
                    // otherwise we look it up by DNS query
                    let address_query = if proxied_query || proxy {
                        Either::A(future::ok(base_address.clone()))
                    } else {
                        Either::B(
                            resolver
                                .lookup_ip(broker_name.as_str())
                                .map_err(move |e| {
                                    error!("DNS lookup error: {:?}", e);
                                    ServiceDiscoveryError::DnsLookupError
                                })
                                .map(move |results| {
                                    SocketAddr::new(results.iter().next().unwrap(), broker_port)
                                }),
                        )
                    };

                    Either::B(
                        address_query
                            .map(move |address| {
                                let b = BrokerAddress {
                                    address,
                                    broker_url,
                                    proxy: proxied_query || proxy,
                                };
                                b
                            })
                            .then(move |res| match res {
                                Err(e) => {
                                    let _ = tx.send(Err(e));
                                    Either::A(future::ok(()))
                                }
                                Ok(b) => {
                                    // if the response indicated a redirect, do another query
                                    // to the target broker
                                    if redirect {
                                        let (tx2, rx2) = oneshot::channel();
                                        let res = self_tx.unbounded_send(Query::Topic(
                                            topic,
                                            Some(b.broker_url),
                                            authoritative,
                                            tx2,
                                        ));
                                        match res {
                                            Err(e) => match e.into_inner() {
                                                Query::Topic(_, _, _, tx) => {
                                                    let _ = tx
                                                        .send(Err(ServiceDiscoveryError::Shutdown));
                                                }
                                                _ => {}
                                            },
                                            Ok(_) => {}
                                        }

                                        Either::B(
                                            rx2.map_err(|_| ServiceDiscoveryError::Canceled)
                                                .flatten()
                                                .then(|res| {
                                                    let _ = tx.send(res);
                                                    Ok(())
                                                }),
                                        )
                                    } else {
                                        let _ = tx.send(Ok(b));
                                        Either::A(future::ok(()))
                                    }
                                }
                            }),
                    )
                }
            }
        })
}
