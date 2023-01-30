use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::future::try_join_all;

use crate::{
    connection_manager::ConnectionManager,
    error::{self, TransactionError},
    proto,
    service_discovery::ServiceDiscovery,
    Error, Executor,
};

const TRANSACTION_COORDINATOR_ASSIGN_TOPIC: &str =
    "persistent://pulsar/system/transaction_coordinator_assign";
const TRANSACTION_COORDINATOR_LOG_TOPIC: &str = "persistent://pulsar/system/__transaction_log_";
const PARTITIONED_TOPIC_SUFFIX: &str = "-partition-"; // TODO: check if used
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
    pub async fn build(self) -> Result<Transaction<Exe>, Error> {
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
        Ok(Transaction::new(txn_id, self.tc_client))
    }
}

#[derive(Clone, Debug)]
pub enum TransactionState {
    Open,
    Committing,
    Committed,
    Aborting,
    Aborted,
    TimedOut,
}

impl Display for TransactionState {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            TransactionState::Open => write!(f, "open"),
            TransactionState::Committing => write!(f, "committing"),
            TransactionState::Committed => write!(f, "committed"),
            TransactionState::Aborting => write!(f, "aborting"),
            TransactionState::Aborted => write!(f, "aborted"),
            TransactionState::TimedOut => write!(f, "timedout"),
        }
    }
}

pub struct Transaction<Exe: Executor> {
    txn_id: TxnID,
    state: TransactionState,
    tc_client: Arc<TransactionCoordinatorClient<Exe>>,
}

impl<Exe: Executor> Transaction<Exe> {
    // TODO: handle transaction timeouts

    fn new(id: TxnID, tc_client: Arc<TransactionCoordinatorClient<Exe>>) -> Transaction<Exe> {
        Transaction {
            txn_id: id,
            state: TransactionState::Open,
            tc_client,
        }
    }

    pub fn id(&self) -> &TxnID {
        &self.txn_id
    }

    pub fn state(&self) -> TransactionState {
        self.state.clone()
    }

    pub async fn commit(mut self) -> Result<(), Error> {
        self.check_open()?;

        self.state = TransactionState::Committing;
        self.tc_client.commit(&self).await?;
        self.state = TransactionState::Committed;

        Ok(())
    }

    pub async fn abort(mut self) -> Result<(), Error> {
        self.check_open()?;

        self.state = TransactionState::Aborting;
        self.tc_client.abort(&self).await?;
        self.state = TransactionState::Aborted;

        Ok(())
    }

    pub(crate) async fn add_publish_partition(&self, topic: String) -> Result<(), Error> {
        self.check_open()?;
        self.tc_client.add_publish_partition(self, topic).await
    }

    pub(crate) async fn add_subscription(
        &self,
        topic: String,
        subscription: String,
    ) -> Result<(), Error> {
        self.check_open()?;
        self.tc_client
            .add_subscription(self, topic, subscription)
            .await
    }

    fn check_open(&self) -> Result<(), Error> {
        if !matches!(self.state, TransactionState::Open) {
            return Err(TransactionError::InvalidTransactionState(self.state.clone()).into());
        }
        Ok(())
    }
}

pub struct TxnID {
    pub(crate) most_sig_bits: u64,
    pub(crate) least_sig_bits: u64,
}

impl Display for TxnID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "({},{})", self.most_sig_bits, self.least_sig_bits)
    }
}

pub(crate) struct TransactionCoordinatorClient<Exe: Executor> {
    handlers: HashMap<u64, TransactionMetaStoreHandler<Exe>>,
    epoch: AtomicUsize,
}

impl<Exe: Executor> TransactionCoordinatorClient<Exe> {
    pub(crate) async fn new(
        manager: Arc<ConnectionManager<Exe>>,
        service_discovery: Arc<ServiceDiscovery<Exe>>,
    ) -> Result<Self, Error> {
        let partitions = service_discovery
            .lookup_partitioned_topic_number(TRANSACTION_COORDINATOR_ASSIGN_TOPIC)
            .await? as u64;
        debug!(
            "Found {} partitions for transaction coordinator assign topic",
            partitions
        );

        if partitions == 0 {
            return Err(TransactionError::CoordinatorNotInitialized.into());
        }

        let handlers_list = try_join_all(
            (0..partitions).map(|p| TransactionMetaStoreHandler::new(p, manager.clone())),
        )
        .await?;

        let handlers = (0..partitions)
            .zip(handlers_list)
            .collect::<HashMap<u64, TransactionMetaStoreHandler<Exe>>>();

        Ok(Self {
            handlers,
            epoch: AtomicUsize::new(0),
        })
    }

    pub(crate) async fn new_transaction(&self, timeout: Duration) -> Result<TxnID, Error> {
        let i = self.epoch.fetch_add(1, Ordering::SeqCst);
        let handler = (i % self.handlers.len()) as u64;
        self.handlers[&handler].new_transaction(timeout).await
    }

    pub(crate) async fn add_publish_partition(
        &self,
        txn: &Transaction<Exe>,
        topic: String,
    ) -> Result<(), Error> {
        self.handlers[&txn.id().most_sig_bits]
            .add_publish_partition(txn, topic)
            .await
    }

    pub(crate) async fn add_subscription(
        &self,
        txn: &Transaction<Exe>,
        topic: String,
        subscription: String,
    ) -> Result<(), Error> {
        self.handlers[&txn.id().most_sig_bits]
            .add_subscription(txn, topic, subscription)
            .await
    }

    pub(crate) async fn commit(&self, txn: &Transaction<Exe>) -> Result<(), Error> {
        self.handlers[&txn.id().most_sig_bits]
            .commit_transaction(txn)
            .await
    }

    pub(crate) async fn abort(&self, txn: &Transaction<Exe>) -> Result<(), Error> {
        self.handlers[&txn.id().most_sig_bits]
            .abort_transaction(txn)
            .await
    }
}

struct TransactionMetaStoreHandler<Exe: Executor> {
    coordinator_id: u64,
    manager: Arc<ConnectionManager<Exe>>,
}

impl<Exe: Executor> TransactionMetaStoreHandler<Exe> {
    async fn new(coordinator_id: u64, manager: Arc<ConnectionManager<Exe>>) -> Result<Self, Error> {
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

    pub(crate) async fn add_publish_partition(
        &self,
        txn: &Transaction<Exe>,
        topic: String,
    ) -> Result<(), Error> {
        let res = self
            .manager
            .get_base_connection()
            .await?
            .sender()
            .add_partition_txn(txn.id(), topic)
            .await?;

        match res.error.and_then(error::server_error) {
            Some(e) => Err(TransactionError::Server(e).into()),
            None => Ok(()),
        }
    }

    pub(crate) async fn add_subscription(
        &self,
        txn: &Transaction<Exe>,
        topic: String,
        subscription: String,
    ) -> Result<(), Error> {
        let res = self
            .manager
            .get_base_connection()
            .await?
            .sender()
            .add_subscription_txn(txn.id(), topic, subscription)
            .await?;

        match res.error.and_then(error::server_error) {
            Some(e) => Err(TransactionError::Server(e).into()),
            None => Ok(()),
        }
    }

    pub(crate) async fn commit_transaction(&self, txn: &Transaction<Exe>) -> Result<(), Error> {
        self.end_transaction(txn, proto::TxnAction::Commit).await
    }

    pub(crate) async fn abort_transaction(&self, txn: &Transaction<Exe>) -> Result<(), Error> {
        self.end_transaction(txn, proto::TxnAction::Abort).await
    }

    async fn end_transaction(
        &self,
        txn: &Transaction<Exe>,
        action: proto::TxnAction,
    ) -> Result<(), Error> {
        let res = self
            .manager
            .get_base_connection()
            .await?
            .sender()
            .end_txn(txn.id(), action)
            .await?;

        match res.error.and_then(error::server_error) {
            Some(e) => Err(TransactionError::Server(e).into()),
            None => Ok(()),
        }
    }
}
