use std::{
    future::Future,
    sync::{Arc, RwLock},
    time::Duration,
};

use rand::Rng;

use super::TransactionId;
use crate::{
    connection::Connection,
    connection_manager::ConnectionManager,
    error::{server_error, ConnectionError},
    proto::{ServerError, TxnAction},
    Error, Executor, Pulsar,
};

pub struct TransactionMetaStoreHandler<Exe: Executor> {
    client: Pulsar<Exe>,
    connection_mgr: Arc<ConnectionManager<Exe>>,
    connection: RwLock<Arc<Connection<Exe>>>,
    executor: Arc<Exe>,
    transaction_coordinator_id: u32,
}

impl<Exe: Executor> TransactionMetaStoreHandler<Exe> {
    const OP_MAX_RETRIES: u32 = 3;
    const OP_MIN_BACKOFF: Duration = Duration::from_millis(10);
    const OP_MAX_BACKOFF: Duration = Duration::from_millis(1000);

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn new(
        client: Pulsar<Exe>,
        connection_mgr: Arc<ConnectionManager<Exe>>,
        connection: RwLock<Arc<Connection<Exe>>>,
        executor: Arc<Exe>,
        transaction_coordinator_id: u32,
    ) -> Result<Self, Error> {
        let handler = Self {
            client,
            connection_mgr,
            connection,
            executor,
            transaction_coordinator_id,
        };

        handler.tc_client_connect().await?;

        Ok(handler)
    }

    pub async fn reconnect(&self) -> Result<(), Error> {
        // `get_tc_connection` will reconnect and swap the connection associated with the address
        // if the connection is not healthy. If the topic lookup points to the same broker
        // and the connection is healthy, this is effectively a no-op. In the case we discover
        // the TC is on a different broker, we'll swap the connection to the new broker's address.
        *self.connection.write().expect("poisoned lock") = super::get_tc_connection(
            &self.client,
            &self.connection_mgr,
            self.transaction_coordinator_id,
        )
        .await?;

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn extract_with_retry<T, F, E, R, S>(&self, op: F, extract: E) -> Result<S, Error>
    where
        R: Future<Output = Result<T, ConnectionError>>,
        F: Fn() -> R,
        E: Fn(T) -> ((Option<i32>, Option<String>), S),
    {
        let mut retries = 0u32;

        loop {
            let res = op().await?;
            let ((code, message), extracted) = extract(res);

            if let Some(err) = code {
                let server_error = server_error(err);

                match server_error {
                    // TC Not Found errors shouldn't bubble up to the user before
                    // we've tried reconnecting a few times
                    error @ Some(ServerError::TransactionCoordinatorNotFound) => {
                        if retries >= Self::OP_MAX_RETRIES {
                            return Err(ConnectionError::PulsarError(error, message).into());
                        }

                        let jitter = Duration::from_millis(rand::thread_rng().gen_range(0..500));
                        let backoff = std::cmp::min(
                            Self::OP_MIN_BACKOFF * 2u32.saturating_pow(retries) + jitter,
                            Self::OP_MAX_BACKOFF,
                        );
                        retries += 1;

                        error!(
                            "Received error: {:?}. Retrying in {} ms",
                            error,
                            backoff.as_millis()
                        );

                        self.executor.delay(backoff).await;
                        self.reconnect().await?;
                        continue;
                    }
                    error => {
                        return Err(ConnectionError::PulsarError(error, message).into());
                    }
                }
            }

            return Ok(extracted);
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn tc_client_connect(&self) -> Result<(), Error> {
        let connection = self.connection.read().expect("poisoned lock").clone();
        let transaction_coordinator_id = self.transaction_coordinator_id;
        let op = || {
            connection
                .sender()
                .tc_client_connect_request(transaction_coordinator_id.into())
        };

        self.extract_with_retry(op, |res| ((res.error, res.message), ()))
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn add_publish_partitions_to_txn(
        &self,
        txn_id: TransactionId,
        partitions: Vec<String>,
    ) -> Result<(), Error> {
        let connection = self.connection.read().expect("poisoned lock").clone();
        let op = || {
            connection
                .sender()
                .add_partition_to_txn(txn_id, partitions.clone())
        };

        self.extract_with_retry(op, |res| ((res.error, res.message), ()))
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn add_subscription_to_txn(
        &self,
        txn_id: TransactionId,
        topic: String,
        subscription: String,
    ) -> Result<(), Error> {
        let connection = self.connection.read().expect("poisoned lock").clone();
        let op = || {
            connection
                .sender()
                .add_subscription_to_txn(txn_id, topic.clone(), subscription.clone())
        };

        self.extract_with_retry(op, |res| ((res.error, res.message), ()))
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn new_txn(&self, timeout: Option<u64>) -> Result<TransactionId, Error> {
        let connection = self.connection.read().expect("poisoned lock").clone();
        let transaction_coordinator_id = self.transaction_coordinator_id;
        let op = || {
            connection
                .sender()
                .new_txn(transaction_coordinator_id.into(), timeout)
        };

        let (txnid_most_bits, txnid_least_bits) = self
            .extract_with_retry(op, |res| {
                (
                    (res.error, res.message),
                    (res.txnid_most_bits, res.txnid_least_bits),
                )
            })
            .await?;

        Ok(TransactionId::new(
            txnid_most_bits.expect("should be present"),
            txnid_least_bits.expect("should be present"),
        ))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn end_txn(&self, txn_id: TransactionId, txn_action: TxnAction) -> Result<(), Error> {
        let connection = self.connection.read().expect("poisoned lock").clone();
        let op = || connection.sender().end_txn(txn_id, txn_action);

        self.extract_with_retry(op, |res| ((res.error, res.message), ()))
            .await
    }
}
