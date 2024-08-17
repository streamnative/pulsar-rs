pub(crate) mod coord;
mod meta_store_handler;
// pub mod transaction;

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::lock::Mutex as AsyncMutex;

pub use crate::Executor;
use crate::{
    error::{ConnectionError, TransactionError},
    proto::ServerError,
    Error,
};

use coord::TransactionCoordinatorClient;

use crate::{
    connection::Connection, connection_manager::ConnectionManager, proto::ProtocolVersion, Pulsar,
};

const TC_ASSIGN_TOPIC: &'static str = "persistent://pulsar/system/transaction_coordinator_assign";

fn get_tc_assign_topic_name(partition: u32) -> String {
    format!("{}-partition-{}", TC_ASSIGN_TOPIC, partition)
}

async fn get_tc_connection<Exe: Executor>(
    client: &Pulsar<Exe>,
    connection_mgr: &Arc<ConnectionManager<Exe>>,
    transaction_coordinator_id: u32,
) -> Result<Arc<Connection<Exe>>, Error> {
    let address = client
        .lookup_topic(get_tc_assign_topic_name(transaction_coordinator_id))
        .await?;

    let connection = connection_mgr.get_connection(&address).await?;
    let remote_endpoint_protocol_version = connection.protocol_version();

    // If the broker isn't speaking at least protocol version 19, we should fail with an
    // error.
    if remote_endpoint_protocol_version < ProtocolVersion::V19 {
        error!(
            "The remote endpoint associated with the connection is speaking protocol version {} which is not supported",
            remote_endpoint_protocol_version.as_str_name()
        );

        return Err(ConnectionError::UnsupportedProtocolVersion(
            remote_endpoint_protocol_version as u32,
        )
        .into());
    }

    Ok(connection)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The state of a transaction.
pub enum State {
    /// When a transaction is in the `OPEN` state, messages can be produced and acked with this transaction.
    ///
    /// When a transaction is in the `OPEN` state, it can commit or abort.
    Open,
    /// When a client invokes a commit, the transaction state is changed from `OPEN` to `COMMITTING`.
    Committing,
    /// When a client invokes an abort, the transaction state is changed from `OPEN` to `ABORTING`.
    Aborting,
    /// When a client receives a response to a commit, the transaction state is changed from
    /// `COMMITTING` to `COMMITTED`.
    Committed,
    /// When a client receives a response to an abort, the transaction state is changed from `ABORTING` to `ABORTED`.
    Aborted,
    /// When a client invokes a commit or an abort, but a transaction does not exist in a coordinator,
    /// then the state is changed to `ERROR`.
    ///
    /// When a client invokes a commit, but the transaction state in a coordinator is `ABORTED` or `ABORTING`,
    /// then the state is changed to `ERROR`.
    ///
    /// When a client invokes an abort, but the transaction state in a coordinator is `COMMITTED` or `COMMITTING`,
    /// then the state is changed to `ERROR`.
    Error,
    /// When a transaction is timed out and the transaction state is `OPEN`,
    /// then the transaction state is changed from `OPEN` to `TIME_OUT`.
    TimeOut,
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            State::Open => "Open",
            State::Committing => "Committing",
            State::Aborting => "Aborting",
            State::Committed => "Committed",
            State::Aborted => "Aborted",
            State::Error => "Error",
            State::TimeOut => "TimeOut",
        };

        write!(f, "{}", state)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct TransactionId {
    /// The most significant 64 bits of this transaction id.
    most_sig_bits: u64,
    /// The least significant 64 bits of this transaction id.
    least_sig_bits: u64,
}

impl TransactionId {
    pub fn new(most_sig_bits: u64, least_sig_bits: u64) -> Self {
        Self {
            most_sig_bits,
            least_sig_bits,
        }
    }

    pub fn least_bits(&self) -> u64 {
        self.least_sig_bits
    }

    pub fn most_bits(&self) -> u64 {
        self.most_sig_bits
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({},{})", self.most_sig_bits, self.least_sig_bits)
    }
}

pub struct TransactionBuilder<Exe: Executor> {
    exe: Arc<Exe>,
    tc_client: Arc<TransactionCoordinatorClient<Exe>>,
    timeout: Option<u128>,
}

impl<Exe: Executor> TransactionBuilder<Exe> {
    const TRANSACTION_TIMEOUT_DEFAULT: u64 = 60_000; // 1 minute

    pub(crate) fn new(exe: Arc<Exe>, tc_client: Arc<TransactionCoordinatorClient<Exe>>) -> Self {
        Self {
            exe,
            tc_client,
            timeout: None,
        }
    }

    /// Set the transaction timeout in milliseconds.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout.as_millis());
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn build(self) -> Result<Transaction<Exe>, Error> {
        let timeout: u64 = self
            .timeout
            .unwrap_or(Self::TRANSACTION_TIMEOUT_DEFAULT.into())
            .try_into()
            .map_err(|_| Error::Transaction(TransactionError::InvalidTimeout))?;

        let txn_id = self.tc_client.new_txn(Some(timeout)).await?;

        Ok(Transaction::new(txn_id, self.tc_client, &self.exe, timeout))
    }
}

#[derive(Clone)]
pub struct Transaction<Exe: Executor> {
    state: Arc<Mutex<State>>,
    id: TransactionId,
    tc_client: Arc<TransactionCoordinatorClient<Exe>>,
    published_partitions: Arc<AsyncMutex<BTreeSet<String>>>,
    registered_subscriptions: Arc<AsyncMutex<BTreeSet<(String, String)>>>,
}

impl<Exe: Executor> Transaction<Exe> {
    pub(self) fn new(
        id: TransactionId,
        tc_client: Arc<TransactionCoordinatorClient<Exe>>,
        exe: &Arc<Exe>,
        timeout_ms: u64,
    ) -> Self {
        let state = Arc::new(Mutex::new(State::Open));
        // Spawn a best-effort task to timeout the transaction.
        // Checking for timeouts from the client side is not 100% reliable, but it should
        // give more information to the caller in cases where the transaction has definitely
        // timed out.
        let _ = exe.spawn({
            // We really only need a weak reference to the state here since we don't want
            // this task keeping the state alive if the transaction has already been dropped.
            let weak_state = Arc::downgrade(&state);
            let timeout_fut = exe.delay(Duration::from_millis(timeout_ms));

            Box::pin(async move {
                timeout_fut.await;

                if let Some(state) = weak_state.upgrade() {
                    *state.lock().expect("poisoned lock") = State::TimeOut;
                }
            })
        });

        Self {
            state,
            id,
            tc_client,
            published_partitions: Arc::new(AsyncMutex::new(BTreeSet::new())),
            registered_subscriptions: Arc::new(AsyncMutex::new(BTreeSet::new())),
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn register_produced_partitions(
        &self,
        partitions: Vec<String>,
    ) -> Result<(), Error> {
        self.ensure_and_swap_state(State::Open, None)?;

        let mut published_partitions = self.published_partitions.lock().await;

        if partitions.iter().all(|p| published_partitions.contains(p)) {
            return Ok(());
        }

        self.tc_client
            .add_publish_partitions_to_txn(self.id, partitions.clone())
            .await?;

        debug!(
            "Added publish partitions to txn {}: {:?}",
            self.id, partitions
        );

        published_partitions.extend(partitions);

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn register_acked_topic(
        &self,
        topic: String,
        subscription: String,
    ) -> Result<(), Error> {
        self.ensure_and_swap_state(State::Open, None)?;

        let mut registered_subscriptions = self.registered_subscriptions.lock().await;

        if registered_subscriptions.contains(&(topic.clone(), subscription.clone())) {
            return Ok(());
        }

        self.tc_client
            .add_subscription_to_txn(self.id, topic.clone(), subscription.clone())
            .await?;

        registered_subscriptions.insert((topic, subscription));

        Ok(())
    }

    /// Commit the transaction.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn commit(&self) -> Result<(), Error> {
        self.ensure_and_swap_state(State::Open, Some(State::Committing))?;

        if let Err(err) = self.tc_client.commit_txn(self.id).await {
            error!("Unable to commit transaction: {}", err);

            match err {
                Error::Connection(ConnectionError::PulsarError(
                    Some(err @ ServerError::TransactionNotFound),
                    _,
                ))
                | Error::Connection(ConnectionError::PulsarError(
                    Some(err @ ServerError::InvalidTxnStatus),
                    _,
                )) => {
                    self.set_state(State::Error);

                    if err == ServerError::TransactionNotFound {
                        error!("Transaction {} not found", self.id);
                        return Err(Error::Transaction(TransactionError::NotFound));
                    }
                }
                Error::Connection(ConnectionError::PulsarError(
                    Some(ServerError::TransactionConflict),
                    _,
                )) => {
                    warn!("Transaction conflict observed for txn {}", self.id);
                    return Err(Error::Transaction(TransactionError::Conflict));
                }
                _ => (),
            }

            return Err(err);
        }

        self.set_state(State::Committed);

        Ok(())
    }

    /// Abort the transaction.
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn abort(&self) -> Result<(), Error> {
        self.ensure_and_swap_state(State::Open, Some(State::Aborting))?;

        if let Err(err) = self.tc_client.abort_txn(self.id).await {
            error!("Unable to abort transaction: {}", err);

            match err {
                Error::Connection(ConnectionError::PulsarError(
                    Some(err @ ServerError::TransactionNotFound),
                    _,
                ))
                | Error::Connection(ConnectionError::PulsarError(
                    Some(err @ ServerError::InvalidTxnStatus),
                    _,
                )) => {
                    self.set_state(State::Error);

                    if err == ServerError::TransactionNotFound {
                        error!("Transaction {} not found", self.id);
                        return Err(Error::Transaction(TransactionError::NotFound));
                    }
                }
                Error::Connection(ConnectionError::PulsarError(
                    Some(ServerError::TransactionConflict),
                    _,
                )) => {
                    warn!("Transaction conflict observed for txn {}", self.id);
                    return Err(Error::Transaction(TransactionError::Conflict));
                }
                _ => (),
            }

            return Err(err);
        }

        self.set_state(State::Aborted);

        Ok(())
    }

    fn ensure_and_swap_state(
        &self,
        expected_state: State,
        new_state: Option<State>,
    ) -> Result<(), Error> {
        let mut actual_state = self.state.lock().expect("poisoned lock");

        if *actual_state != expected_state {
            error!(
                "Transaction {} is in state {}, expected state {}",
                self.id, *actual_state, expected_state
            );
            return Err(Error::Transaction(TransactionError::InvalidState(
                *actual_state,
            )));
        }

        if let Some(new_state) = new_state {
            *actual_state = new_state;
        }

        Ok(())
    }

    fn set_state(&self, state: State) {
        *self.state.lock().expect("poisoned lock") = state;
    }

    /// Get the transaction id.
    pub fn id(&self) -> TransactionId {
        self.id
    }

    /// Get the state of the transaction.
    pub fn state(&self) -> State {
        *self.state.lock().expect("poisoned lock")
    }
}
