use crate::connection::{Authentication, Connection};
use crate::connection_manager::{BrokerAddress, ConnectionManager};
use crate::error::ServiceDiscoveryError;
use crate::executor::{Executor, TaskExecutor};
use crate::message::proto::{command_lookup_topic_response, CommandLookupTopicResponse};
use futures::{future::try_join_all, FutureExt};
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
    manager: Arc<ConnectionManager>,
    executor: TaskExecutor,
    resolver: TokioAsyncResolver,
}

impl ServiceDiscovery {
    pub async fn new<E: Executor + 'static>(
        addr: SocketAddr,
        auth: Option<Authentication>,
        executor: E,
    ) -> Result<Self, ServiceDiscoveryError> {
        let executor = TaskExecutor::new(executor);
        let conn = ConnectionManager::new(addr, auth, executor.clone()).await?;
        let resolver =
            TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default())
                .await
                .map_err(|e| {
                    error!("could not create DNS resolver: {:?}", e);
                    ServiceDiscoveryError::Canceled
                })?;
        Ok(ServiceDiscovery::with_manager(
            Arc::new(conn),
            executor,
            resolver,
        ))
    }

    pub fn with_manager<E: Executor + 'static>(
        manager: Arc<ConnectionManager>,
        executor: E,
        resolver: TokioAsyncResolver,
    ) -> Self {
        let executor = TaskExecutor::new(executor);
        ServiceDiscovery {
            manager,
            executor,
            resolver,
        }
    }

    /// get the broker address for a topic
    pub async fn lookup_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<BrokerAddress, ServiceDiscoveryError> {
        let topic = topic.into();
        let conn_info = self.manager.get_connection_from_url(None).await;
        let base_address = self.manager.address;
        let authoritative = false;

        if let Some((proxied_query, conn)) = conn_info {
            self.lookup(
                topic.clone(),
                proxied_query,
                conn.clone(),
                base_address,
                authoritative,
            )
            .await
        } else {
            Err(ServiceDiscoveryError::Query(
                "unknown broker URL".to_string(),
            ))
        }
    }

    /// get the number of partitions for a partitioned topic
    pub async fn lookup_partitioned_topic_number<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<u32, ServiceDiscoveryError> {
        let connection = self.manager.get_base_connection().await?;

        let response = connection.sender().lookup_partitioned_topic(topic).await?;

        match response.partitions {
            Some(partitions) => Ok(partitions),
            None => {
                if let Some(s) = response.message {
                    Err(ServiceDiscoveryError::Query(s))
                } else {
                    Err(ServiceDiscoveryError::Query(format!(
                        "server error: {:?}",
                        response.error
                    )))
                }
            }
        }
    }

    /// get the list of topic names and addresses for a partitioned topic
    pub async fn lookup_partitioned_topic<S: Into<String>>(
        &self,
        topic: S,
    ) -> Result<Vec<(String, BrokerAddress)>, ServiceDiscoveryError> {
        let topic = topic.into();
        let partitions = self.lookup_partitioned_topic_number(&topic).await?;
        let topics = (0..partitions)
            .map(|nb| {
                let t = format!("{}-partition-{}", topic, nb);
                self.lookup_topic(t.clone())
                    .map(move |address_res| match address_res {
                        Err(e) => Err(e),
                        Ok(address) => Ok((t, address)),
                    })
            })
            .collect::<Vec<_>>();
        try_join_all(topics).await
    }

    pub async fn lookup(
        &self,
        topic: String,
        mut proxied_query: bool,
        mut conn: Arc<Connection>,
        base_address: SocketAddr,
        mut is_authoritative: bool,
    ) -> Result<BrokerAddress, ServiceDiscoveryError> {
        loop {
            let response = conn
                .sender()
                .lookup_topic(topic.to_string(), is_authoritative)
                .await?;
            let LookupResponse {
                broker_name,
                broker_url,
                broker_port,
                proxy,
                redirect,
                authoritative,
            } = convert_lookup_response(&response)?;
            is_authoritative = authoritative;

            // get the IP and port for the broker_name
            // if going through a proxy, we use the base address,
            // otherwise we look it up by DNS query
            let address = if proxied_query || proxy {
                base_address
            } else {
                let results: Result<_, _> = self
                    .resolver
                    .lookup_ip(broker_name.as_str())
                    .await
                    .map_err(move |e| {
                        error!("DNS lookup error: {:?}", e);
                        ServiceDiscoveryError::DnsLookupError
                    });
                match results {
                    Err(_) => return Err(ServiceDiscoveryError::DnsLookupError),
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
                let broker_url = Some(b.broker_url);
                let conn_info = self.manager.get_connection_from_url(broker_url).await;
                if let Some((new_proxied_query, new_conn)) = conn_info {
                    proxied_query = new_proxied_query;
                    conn = new_conn.clone();
                    continue;
                } else {
                    return Err(ServiceDiscoveryError::Query(
                        "unknown broker URL".to_string(),
                    ));
                }
            } else {
                b
            };

            let res = self
                .manager
                .get_connection(&broker_address.clone())
                .await
                .map(|_| broker_address)
                .map_err(|e| ServiceDiscoveryError::Connection(e));
            break res;
        }
    }
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
