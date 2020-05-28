use crate::connection::{Authentication, Connection};
use crate::connection_manager::{BrokerAddress, ConnectionManager};
use crate::error::ServiceDiscoveryError;
use crate::executor:: Executor;
use crate::message::proto::{command_lookup_topic_response, CommandLookupTopicResponse};
use futures::{future::try_join_all, FutureExt};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use url::Url;

/// Look up broker addresses for topics and partitioned topics
///
/// The ServiceDiscovery object provides a single interface to start
/// interacting with a cluster. It will automatically follow redirects
/// or use a proxy, and aggregate broker connections
#[derive(Clone)]
pub struct ServiceDiscovery<Exe: Executor + ?Sized> {
    manager: Arc<ConnectionManager<Exe>>,
}

impl<Exe: Executor> ServiceDiscovery<Exe> {
    pub fn with_manager(
        manager: Arc<ConnectionManager<Exe>>,
    ) -> Self {
        ServiceDiscovery {
            manager,
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
                broker_hostname,
                broker_url,
                broker_port,
                proxy,
                redirect,
                authoritative,
                tls,
            } = convert_lookup_response(&response)?;
            is_authoritative = authoritative;

            // get the IP and port for the broker_name
            // if going through a proxy, we use the base address,
            // otherwise we look it up by DNS query
            let address = if proxied_query || proxy {
                base_address
            } else {
                match Exe::spawn_blocking(move|| {
                    (broker_name.as_str(), broker_port).to_socket_addrs().map_err(|e| {
                        error!("error querying '{}': {:?}", broker_name, e);
                    }).ok().and_then(|mut it| it.next())
                }).await {
                    Some(Some(address)) => address,
                    _ => return Err(ServiceDiscoveryError::DnsLookupError)
                }
            };

            let b = BrokerAddress {
                address,
                hostname: broker_hostname,
                broker_url,
                proxy: proxied_query || proxy,
                tls,
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
    pub broker_hostname: String,
    pub broker_url: String,
    pub broker_port: u16,
    pub proxy: bool,
    pub redirect: bool,
    pub authoritative: bool,
    pub tls: bool,
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
        response.broker_service_url_tls.as_ref().or(
        response
            .broker_service_url.as_ref())
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
    let broker_port = url.port().unwrap_or(match url.scheme() {
      "pulsar" => 6650,
      "pulsar+ssl" => 6651,
      s => {
          error!("invalid scheme: {}", s);
          return Err(ServiceDiscoveryError::NotFound);
      },
    });
    let authoritative = response.authoritative.unwrap_or(false);
    let redirect =
        response.response == Some(command_lookup_topic_response::LookupType::Redirect as i32);
    let tls = match url.scheme() {
      "pulsar" => false,
      "pulsar+ssl" => true,
      s => {
          error!("invalid scheme: {}", s);
          return Err(ServiceDiscoveryError::NotFound);
      },
    };

    Ok(LookupResponse {
        broker_name,
        broker_hostname: broker_url.clone(),
        broker_url,
        broker_port,
        proxy,
        redirect,
        authoritative,
        tls,
    })
}
