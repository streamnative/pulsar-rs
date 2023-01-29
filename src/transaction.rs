use std::{
    fmt::{Display, Formatter},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::future::try_join_all;

use crate::{
    connection_manager::ConnectionManager, error::TransactionError,
    service_discovery::ServiceDiscovery, Error, Executor, Pulsar,
};

const TRANSACTION_COORDINATOR_ASSIGN_TOPIC: &str =
    "persistent://pulsar/system/transaction_coordinator_assign";
const TRANSACTION_COORDINATOR_LOG_TOPIC: &str = "persistent://pulsar/system/__transaction_log_";
const PARTITIONED_TOPIC_SUFFIX: &str = "-partition-";
const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(60);

pub struct TransactionBuilder<Exe: Executor> {
    tc_client: Arc<TransactionCoordinatorClient<Exe>>,
    timeout: Duration,
}

impl<Exe: Executor> TransactionBuilder<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) fn new(tc_client: Arc<TransactionCoordinatorClient<Exe>>) -> Self {
        Self {
            tc_client,
            timeout: DEFAULT_TRANSACTION_TIMEOUT,
        }
    }

    /// configure the maximum amount of time that
    /// the transaction coordinator will for a transaction to be completed by the client
    /// before proactively aborting the ongoing transaction.
    /// the config value will be sent to the transaction coordinator along with the CommandNewTxn.
    /// default is 60 seconds.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_transaction_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// start a new transaction
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn build(self) -> Result<Transaction, Error> {
        // talk to TC to begin a transaction
        //       the builder is responsible for locating the transaction coordinator (TC)
        //       and start the transaction to get the transaction id.
        //       after getting the transaction id, all the operations are handled by the
        //       `Transaction`
        let txn_result = self.tc_client.new_transaction(self.timeout).await;
        if let Err(e) = txn_result {
            error!("Failed to start a new transaction: {:?}", e);
            return Err(e);
        }
        let txn_id = txn_result.unwrap();
        debug!("Successfully got a transaction id: {}", txn_id);
        Ok(Transaction { txn_id })
    }
}

pub struct Transaction {
    txn_id: TxnID,
}

impl Transaction {
    pub fn txn_id(&self) -> &TxnID {
        &self.txn_id
    }

    pub async fn commit(self) -> Result<(), Error> {
        Ok(()) // TODO
    }

    pub async fn abort(self) -> Result<(), Error> {
        Ok(()) // TODO
    }
}

pub(crate) struct TransactionCoordinatorClient<Exe: Executor> {
    handlers: Vec<TransactionMetaStoreHandler<Exe>>,
    epoch: AtomicUsize,
}

impl<Exe: Executor> TransactionCoordinatorClient<Exe> {
    pub(crate) async fn new(
        manager: Arc<ConnectionManager<Exe>>,
        service_discovery: Arc<ServiceDiscovery<Exe>>,
    ) -> Result<Self, Error> {
        let partitions = service_discovery
            .lookup_partitioned_topic_number(TRANSACTION_COORDINATOR_ASSIGN_TOPIC)
            .await?;
        debug!(
            "Found {} partitions for transaction coordinator assign topic",
            partitions
        );

        if partitions == 0 {
            return Err(TransactionError::CoordinatorNotInitialized.into());
        }

        let handlers = try_join_all(
            (0..partitions).map(|p| TransactionMetaStoreHandler::new(p, manager.clone())),
        )
        .await?;

        Ok(Self {
            handlers,
            epoch: AtomicUsize::new(0),
        })
    }

    pub(crate) async fn new_transaction(&self, timeout: Duration) -> Result<TxnID, Error> {
        let i = self.epoch.fetch_add(1, Ordering::SeqCst);
        let handler = i % self.handlers.len();
        self.handlers[handler].new_transaction(timeout).await
    }
}

pub struct TxnID {
    most_sig_bits: u64,
    least_sig_bits: u64,
}

impl Display for TxnID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({},{})", self.most_sig_bits, self.least_sig_bits)
    }
}

struct TransactionMetaStoreHandler<Exe: Executor> {
    coordinator_id: u32,
    manager: Arc<ConnectionManager<Exe>>,
}

impl<Exe: Executor> TransactionMetaStoreHandler<Exe> {
    async fn new(coordinator_id: u32, manager: Arc<ConnectionManager<Exe>>) -> Result<Self, Error> {
        Ok(Self {
            coordinator_id,
            manager,
        })
    }

    async fn new_transaction(&self, timeout: Duration) -> Result<TxnID, Error> {
        let res = self
            .manager
            .get_base_connection()
            .await?
            .sender()
            .new_transaction(self.coordinator_id as u64, timeout)
            .await?;

        Ok(TxnID {
            most_sig_bits: res.txnid_most_bits(),
            least_sig_bits: res.txnid_least_bits(),
        })
    }
}
