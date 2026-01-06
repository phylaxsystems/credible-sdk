use crate::utils::ErrorRecoverability;
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    thread::JoinHandle,
    time::Duration,
};
use revm::{context::{BlockEnv, TxEnv}, primitives::{Address, Bytes, FixedBytes}};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::info;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// This struct contains data for giving high level context to
/// any observer about an incident (or invalidation) that occured. 
#[derive(Debug, Clone)]
pub(crate) struct IncidentData {
    adopter_address: Address,
    assertion_id: FixedBytes<32>,
    assertion_fn: FixedBytes<4>,
    revert_data: Bytes,
}

/// Contains txhash and blockenv. Used to reconstruct
/// incident txs and display them to end users.
pub(crate) type ReconstructableTx = (FixedBytes<32>, TxEnv);

/// Represents a full incident, includes transactions that preceeded
/// the invalidating(incident) tx, blockenv, and more metadata needed
/// for consumers to debug and investigate assertion failures.
#[derive(Debug)]
pub(crate) struct IncidentReport {
    /// Transaction that caused the incident
    transaction_data: ReconstructableTx,
    /// All individual assertion failures
    failures: Vec<IncidentData>,
    /// Block env of where the invalidation happened
    block_env: BlockEnv,
    /// When the invalidation happened
    incident_timestamp: u64,
    /// Transaction hashes of previous transactions in the iteration
    /// and their blockenvs
    prev_txs: Vec<ReconstructableTx>,
}

#[derive(Clone)]
pub struct TransactionObserverConfig {
    pub poll_interval: Duration,
    pub endpoint_rps_max: usize,
    pub endpoint: String,
    pub auth_token: String,
    pub db_path: String,
}

impl Default for TransactionObserverConfig {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            endpoint_rps_max: 60,
            endpoint: String::default(),
            auth_token: String::default(),
            db_path: String::default(),
        }
    }
}

/// The `TransactionObserver`s job is to accept reports of invalidating transactions
/// (or incidents), store them, and forward these events to the dapp API.
/// 
/// # Receving data
///
/// The `TransactionObserver` receives `IncidentReport`s via a mpsc channel.
/// as soon as the data is received, we write(fsync) to it in a persistent mdbx database.
/// This is treated like mission critical data. Any data that the observer receives
/// is written to disk immediately, and we only remove it from disk if we have
/// positive confirmation from the dapp API that it was indeed processed (we're fine
/// with sending duped calls for reliabilitys sake).
///
/// # Sending to the dapp
///
/// The `TransactionObserver` in the background has a process that reads the database,
/// and publishes data from it to the dapp API. This happens on a different thread to
/// not block ingest. It polls every n-ms for new updates. It then queues these updates
/// to be sent. If we receive a tcp response that indicates a success, we can remove 
/// the request safely from the database only then.
///
/// If the request fails, we have it remain in the database and save it for the next run.
/// The amount of requests we send is limited by `TransactionObserverConfig::endpoint_rps_max`
/// as to not flood the endpoint with requests if its crashing.
pub struct TransactionObserver {
    config: TransactionObserverConfig,
}

impl TransactionObserver {
    pub fn new(config: TransactionObserverConfig) -> Self {
        Self { config }
    }

    /// Spawns the transaction observer on a dedicated OS thread.
    #[allow(clippy::type_complexity)]
    pub fn spawn(
        self,
        shutdown: Arc<AtomicBool>,
    ) -> Result<
        (
            JoinHandle<Result<(), TransactionObserverError>>,
            oneshot::Receiver<Result<(), TransactionObserverError>>,
        ),
        std::io::Error,
    > {
        let (tx, rx) = oneshot::channel();

        let handle = std::thread::Builder::new()
            .name("sidecar-transaction-observer".into())
            .spawn(move || {
                let mut observer = self;
                let result = observer.run_blocking(shutdown);
                let _ = tx.send(result.clone());
                result
            })?;

        Ok((handle, rx))
    }

    fn run_blocking(
        &mut self,
        shutdown: Arc<AtomicBool>,
    ) -> Result<(), TransactionObserverError> {
        loop {
            if shutdown.load(Ordering::Relaxed) {
                info!(target = "transaction_observer", "Shutdown signal received");
                return Ok(());
            }

            self.publish_invalidations()?;
            std::thread::sleep(self.config.poll_interval);
        }
    }

    fn publish_invalidations(&self) -> Result<(), TransactionObserverError> {
        Ok(())
    }
}

#[derive(Debug, Error, Clone)]
pub enum TransactionObserverError {
    #[error("Failed to publish invalidations to dapp API: {reason}")]
    PublishFailed { reason: String },
}

impl From<&TransactionObserverError> for ErrorRecoverability {
    fn from(_: &TransactionObserverError) -> Self {
        ErrorRecoverability::Recoverable
    }
}
