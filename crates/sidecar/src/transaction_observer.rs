use crate::utils::ErrorRecoverability;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    thread::JoinHandle,
    time::{
        Duration,
        Instant,
    },
};
use revm::{context::{BlockEnv, TxEnv}, primitives::{Address, Bytes, FixedBytes}};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::info;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Timeout for recv - threads will check for the shutdown flag at this interval
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// This struct contains data for giving high level context to
/// any observer about an incident (or invalidation) that occured. 
#[derive(Debug, Clone)]
pub struct IncidentData {
    pub(crate) adopter_address: Address,
    pub(crate) assertion_id: FixedBytes<32>,
    pub(crate) assertion_fn: FixedBytes<4>,
    pub(crate) revert_data: Bytes,
}

/// Contains txhash and blockenv. Used to reconstruct
/// incident txs and display them to end users.
pub type ReconstructableTx = (FixedBytes<32>, TxEnv);

/// Represents a full incident, includes transactions that preceeded
/// the invalidating(incident) tx, blockenv, and more metadata needed
/// for consumers to debug and investigate assertion failures.
#[derive(Debug)]
pub struct IncidentReport {
    /// Transaction that caused the incident
    pub(crate) transaction_data: ReconstructableTx,
    /// All individual assertion failures
    pub(crate) failures: Vec<IncidentData>,
    /// Block env of where the invalidation happened
    pub(crate) block_env: BlockEnv,
    /// When the invalidation happened
    pub(crate) incident_timestamp: u64,
    /// Transaction hashes of previous transactions in the iteration
    /// and their blockenvs
    pub(crate) prev_txs: Vec<ReconstructableTx>,
}

pub type IncidentReportSender = flume::Sender<IncidentReport>;
pub type IncidentReportReceiver = flume::Receiver<IncidentReport>;

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
    incident_rx: IncidentReportReceiver,
    pending_reports: VecDeque<IncidentReport>,
}

impl TransactionObserver {
    pub fn new(config: TransactionObserverConfig, incident_rx: IncidentReportReceiver) -> Self {
        Self {
            config,
            incident_rx,
            pending_reports: VecDeque::new(),
        }
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
        let mut last_publish = Instant::now();
        loop {
            if shutdown.load(Ordering::Relaxed) {
                info!(target = "transaction_observer", "Shutdown signal received");
                return Ok(());
            }

            match self.incident_rx.recv_timeout(RECV_TIMEOUT) {
                Ok(report) => self.store_incident(report),
                Err(flume::RecvTimeoutError::Timeout) => {}
                Err(flume::RecvTimeoutError::Disconnected) => {
                    return Err(TransactionObserverError::ChannelClosed);
                }
            }

            if last_publish.elapsed() >= self.config.poll_interval {
                self.publish_invalidations()?;
                last_publish = Instant::now();
            }
        }
    }

    fn store_incident(&mut self, report: IncidentReport) {
        self.pending_reports.push_back(report);
    }

    fn publish_invalidations(&mut self) -> Result<(), TransactionObserverError> {
        Ok(())
    }
}

#[derive(Debug, Error, Clone)]
pub enum TransactionObserverError {
    #[error("Failed to publish invalidations to dapp API: {reason}")]
    PublishFailed { reason: String },
    #[error("Incident report channel closed")]
    ChannelClosed,
}

impl From<&TransactionObserverError> for ErrorRecoverability {
    fn from(_: &TransactionObserverError) -> Self {
        ErrorRecoverability::Recoverable
    }
}
