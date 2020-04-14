use crate::connection::{Authentication, ConnectionSender};
use crate::connection_manager::{BrokerAddress, ConnectionManager};
use crate::error::ServiceDiscoveryError;
use crate::executor::{Executor, TaskExecutor};
use crate::message::proto::{command_lookup_topic_response, CommandLookupTopicResponse};
use futures::{
    future::{self, try_join_all, Either},
    channel::{mpsc, oneshot},
    Future, FutureExt, StreamExt,
};
use std::net::SocketAddr;
use std::sync::Arc;
use trust_dns_resolver::config::*;
use trust_dns_resolver::TokioAsyncResolver;
use url::Url;

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
    pub async fn new<E: Executor + 'static>(
        addr: SocketAddr,
        auth: Option<Authentication>,
        executor: E,
    ) -> Result<Self, ServiceDiscoveryError> {
        let executor = TaskExecutor::new(executor);
        let conn = ConnectionManager::new(addr, auth, executor.clone()).await?;
        Ok(ServiceDiscovery::with_manager(Arc::new(conn), executor))
    }

    pub fn with_manager<E: Executor + 'static>(manager: Arc<ConnectionManager>, executor: E) -> Self {
        let executor = TaskExecutor::new(executor);
        let tx = engine(manager, executor);
        ServiceDiscovery { tx }
    }

    /// get the broker address for a topic
    pub fn lookup_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> impl Future<Output = Result<BrokerAddress, ServiceDiscoveryError>> {
        if self.tx.is_closed() {
            return Either::Left(future::err(ServiceDiscoveryError::Shutdown));
        }

        Either::Right(lookup_topic(topic, self.tx.clone()))
    }

    /// get the number of partitions for a partitioned topic
    pub async fn lookup_partitioned_topic_number<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<u32, ServiceDiscoveryError> {
        if self.tx.is_closed() {
            return Err(ServiceDiscoveryError::Shutdown);
        }

        let (tx, rx) = oneshot::channel();
        let topic: String = topic.into();
        if self
            .tx
            .unbounded_send(Query::PartitionedTopic(topic, tx))
            .is_err()
        {
            return Err(ServiceDiscoveryError::Shutdown);
        }

        rx.await.map_err(|_| ServiceDiscoveryError::Canceled)?
    }

    /// get the list of topic names and addresses for a partitioned topic
    pub async fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<Vec<(String, BrokerAddress)>, ServiceDiscoveryError> {
        if self.tx.is_closed() {
            return Err(ServiceDiscoveryError::Shutdown);
        }

        let (tx, rx) = oneshot::channel();
        let topic: String = topic.into();

        if self
            .tx
            .unbounded_send(Query::PartitionedTopic(topic.clone(), tx))
            .is_err()
        {
            return Err(ServiceDiscoveryError::Shutdown);
        }

        let self_tx = self.tx.clone();

        let partitions = rx.await.map_err(|_| ServiceDiscoveryError::Canceled)??;
        let topics = (0..partitions)
            .map(|nb| {
                let t = format!("{}-partition-{}", topic, nb);
                lookup_topic(t.clone(), self_tx.clone())
                    .map(move |address_res| match address_res {
                        Err(e) => Err(e),
                        Ok(address) => Ok((t, address))
                    })
            })
        .collect::<Vec<_>>();
        try_join_all(topics).await
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
}

/// helper function for topic lookup
///
/// connects to the target broker if necessary
async fn lookup_topic<S: Into<String>>(
    topic: S,
    self_tx: mpsc::UnboundedSender<Query>,
) -> Result<BrokerAddress, ServiceDiscoveryError> {
    let (tx, rx) = oneshot::channel();
    let topic: String = topic.into();
    if self_tx
        .unbounded_send(Query::Topic(topic, None, false, tx))
        .is_err()
    {
        return Err(ServiceDiscoveryError::Shutdown);
    }

    rx.await.map_err(|_| ServiceDiscoveryError::Canceled)?
}

/// core of the service discovery
///
/// this function loops over the query channel and launches lookups.
/// It can send a message to itself for further queries if necessary.
fn engine(manager: Arc<ConnectionManager>, executor: TaskExecutor) -> mpsc::UnboundedSender<Query> {
    let (tx, rx) = mpsc::unbounded();
    let tx2 = tx.clone();
    //let resolver = //(resolver, resolver_future) =
     //   TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());
    //executor.spawn(Box::pin(resolver_future));

    let f = move || {
        TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default()).then(|resolver| {
        let resolver = resolver.expect("FIXME");

        rx.for_each(move |query: Query| {
            let self_tx = tx2.clone();
            let base_address = manager.address;
            //let resolver = resolver.clone();
            let manager = manager.clone();

            match query {
                Query::Topic(topic, broker_url, authoritative, tx) => Either::Left({
                    let url = broker_url.clone();
                    let conn_info = manager.get_connection_from_url(url);

                    conn_info.then(move |res| match res {
                        Err(conn_error) => {
                            let _ = tx.send(Err(ServiceDiscoveryError::Connection(conn_error)));
                            Either::Right(future::ready(()))
                        }
                        Ok(conn_info) => {
                            if let Some((proxied_query, conn)) = conn_info {
                                Either::Left(lookup(
                                    topic.to_string(),
                                    proxied_query,
                                    conn.sender(),
                                    resolver,
                                    base_address,
                                    authoritative,
                                    manager,
                                    tx,
                                    self_tx.clone(),
                                ))
                            } else {
                                let _ = tx.send(Err(ServiceDiscoveryError::Query(format!(
                                    "unknown broker URL: {}",
                                    broker_url.unwrap_or_else(String::new)
                                ))));
                                Either::Right(future::ready(()))
                            }
                        }
                    })
                }),
                Query::PartitionedTopic(topic, tx) => {
                    Either::Right(manager.get_base_connection().then(|res| match res {
                        Err(conn_error) => {
                            let _ = tx.send(Err(ServiceDiscoveryError::Connection(conn_error)));
                            Either::Left(future::ready(()))
                        }
                        Ok(conn) => {
                            Either::Right(conn.sender().lookup_partitioned_topic(topic).then(|res| {
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
                                                    tx.send(Err(ServiceDiscoveryError::Query(
                                                        format!(
                                                            "server error: {:?}",
                                                            response.error
                                                        ),
                                                    )))
                                                }
                                            }
                                        };
                                    }
                                }
                                future::ready(())
                            }))
                        }
                    }))
                }
            }
        })
        //.map(|_| error!("service discovery engine stopped"))
    })
        .map(|_| error!("service discovery engine stopped"))
        //.map_err(|e| error!("service discovery engine stopped: {:?}", e))
    };

    executor.spawn(Box::pin(f()));

    tx
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

async fn lookup(
    topic: String,
    proxied_query: bool,
    sender: &ConnectionSender,
    resolver: TokioAsyncResolver,
    base_address: SocketAddr,
    authoritative: bool,
    manager: Arc<ConnectionManager>,
    tx: oneshot::Sender<Result<BrokerAddress, ServiceDiscoveryError>>,
    self_tx: mpsc::UnboundedSender<Query>,
)  {
    let response = match sender.lookup_topic(topic.to_string(), authoritative).await {
        Err(e) => {
            let _ = tx.send(Err(ServiceDiscoveryError::Connection(e)));
            return;
        },
        Ok(response) => response,
    };

    let LookupResponse {
        broker_name,
        broker_url,
        broker_port,
        proxy,
        redirect,
        authoritative,
    } = match convert_lookup_response(&response) {
        Err(e) => {
            let _ = tx.send(Err(e));
            return;
        }
        Ok(info) => info,
    };

    // get the IP and port for the broker_name
    // if going through a proxy, we use the base address,
    // otherwise we look it up by DNS query
    let address = if proxied_query || proxy {
        base_address
    } else {
        let results: Result<_, _> = resolver.lookup_ip(broker_name.as_str()).await
            .map_err(move |e| {
                error!("DNS lookup error: {:?}", e);
                ServiceDiscoveryError::DnsLookupError
            });
        match results {
            Err(_) => return,
            Ok(lookup) => {
                let i: std::net::IpAddr = lookup.iter().next().unwrap();
                SocketAddr::new(i, broker_port)
            }
        }
    };

    let b = BrokerAddress {
        address,
        broker_url,
        proxy: proxied_query || proxy,
    };

    // if the response indicated a redirect, do another query
    // to the target broker
    let broker_address: BrokerAddress = if redirect {
        let (tx2, rx2) = oneshot::channel();
        let res = self_tx.unbounded_send(Query::Topic(
                topic,
                Some(b.broker_url),
                authoritative,
                tx2,
                ))
            .map_err(|e|match e.into_inner() {
                Query::Topic(_, _, _, tx) => {
                    let _ =
                        tx.send(Err(ServiceDiscoveryError::Shutdown));
                }
                _ => {}
            });

        match rx2.await.map_err(|_| ServiceDiscoveryError::Canceled) {
          //FIXME
          Ok(Ok(b)) => b,
          _ => return,
        }
    } else {
        b
    };

    let _ = tx.send(manager
        .get_connection(&broker_address.clone()).await
        .map(|_| {
            broker_address
        }).map_err(|e| {
            ServiceDiscoveryError::Connection(e)
        }));
}
