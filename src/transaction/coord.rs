use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use std::sync::RwLock;

use crate::{
    connection_manager::ConnectionManager, error::TransactionError, proto::TxnAction, Error,
    Executor, Pulsar,
};

use super::{meta_store_handler::TransactionMetaStoreHandler, TransactionId};

pub struct TransactionCoordinatorClient<Exe: Executor> {
    handlers: BTreeMap<u64, TransactionMetaStoreHandler<Exe>>,
    epoch: AtomicU64,
}

impl<Exe: Executor> TransactionCoordinatorClient<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn new(
        client: Pulsar<Exe>,
        connection_mgr: Arc<ConnectionManager<Exe>>,
    ) -> Result<Self, Error> {
        debug!("Starting transaction coordinator client");

        let partitions = client
            .lookup_partitioned_topic_number(super::TC_ASSIGN_TOPIC)
            .await?;

        if partitions == 0 {
            error!("The transaction coordinator is not enabled or has not been initialized");
            return Err(Error::Transaction(TransactionError::CoordinatorNotFound));
        }

        let mut handlers = BTreeMap::new();

        for partition in 0..partitions {
            let connection = super::get_tc_connection(&client, &connection_mgr, partition).await?;

            let handler = TransactionMetaStoreHandler::new(
                client.clone(),
                Arc::clone(&connection_mgr),
                RwLock::new(connection),
                Arc::clone(&connection_mgr.executor),
                partition,
            )
            .await?;

            handlers.insert(partition.into(), handler);
        }

        Ok(Self {
            handlers,
            epoch: AtomicU64::new(0),
        })
    }

    fn get_handler_for_txn(
        &self,
        txn_id: TransactionId,
    ) -> Result<&TransactionMetaStoreHandler<Exe>, Error> {
        self.handlers
            .get(&txn_id.most_bits())
            .ok_or(Error::Transaction(TransactionError::MetaHandlerNotFound(
                txn_id,
            )))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn commit_txn(&self, txn_id: TransactionId) -> Result<(), Error> {
        let handler = self.get_handler_for_txn(txn_id)?;

        handler.end_txn(txn_id, TxnAction::Commit).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn abort_txn(&self, txn_id: TransactionId) -> Result<(), Error> {
        let handler = self.get_handler_for_txn(txn_id)?;

        handler.end_txn(txn_id, TxnAction::Abort).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn new_txn(&self, timeout: Option<u64>) -> Result<TransactionId, Error> {
        self.next_handler().new_txn(timeout).await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn add_publish_partitions_to_txn(
        &self,
        txn_id: TransactionId,
        partitions: Vec<String>,
    ) -> Result<(), Error> {
        let handler = self.get_handler_for_txn(txn_id)?;

        handler
            .add_publish_partitions_to_txn(txn_id, partitions)
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn add_subscription_to_txn(
        &self,
        txn_id: TransactionId,
        topic: String,
        subscription: String,
    ) -> Result<(), Error> {
        let handler = self.get_handler_for_txn(txn_id)?;

        handler
            .add_subscription_to_txn(txn_id, topic, subscription)
            .await
    }

    /// Get the next transaction meta store handler to use. This effectively
    /// load balances across handlers in a round-robin fashion.
    fn next_handler(&self) -> &TransactionMetaStoreHandler<Exe> {
        let index = self
            .epoch
            .fetch_add(1, Ordering::Relaxed)
            .checked_rem(self.handlers.len() as u64)
            .expect("epoch overflow");

        self.handlers.get(&index).expect("handler not found")
    }
}
