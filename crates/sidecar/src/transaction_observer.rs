use crate::utils::ErrorRecoverability;
use rand::random;
use reth_db::mdbx::{
    DatabaseArguments,
    DatabaseEnv,
    DatabaseEnvKind,
};
use reth_db_api::{
    Database,
    transaction::{
        DbTx,
        DbTxMut,
    },
};
use reth_libmdbx::{
    DatabaseFlags,
    WriteFlags,
};
use revm::{
    context::{
        BlockEnv,
        TxEnv,
    },
    primitives::{
        Address,
        Bytes,
        FixedBytes,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::VecDeque,
    path::Path,
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
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::info;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Timeout for recv - threads will check for the shutdown flag at this interval
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// This struct contains data for giving high level context to
/// any observer about an incident (or invalidation) that occured.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Serialize, Deserialize)]
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

const INCIDENT_REPORTS_TABLE: &str = "incident_reports";

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
    db: IncidentDb,
}

impl TransactionObserver {
    pub fn new(
        config: TransactionObserverConfig,
        incident_rx: IncidentReportReceiver,
    ) -> Result<Self, TransactionObserverError> {
        let db = IncidentDb::open(&config.db_path)?;
        Ok(Self {
            config,
            incident_rx,
            pending_reports: VecDeque::new(),
            db,
        })
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

    fn run_blocking(&mut self, shutdown: Arc<AtomicBool>) -> Result<(), TransactionObserverError> {
        let mut last_publish = Instant::now();
        loop {
            if shutdown.load(Ordering::Relaxed) {
                info!(target = "transaction_observer", "Shutdown signal received");
                return Ok(());
            }

            match self.incident_rx.recv_timeout(RECV_TIMEOUT) {
                Ok(report) => self.store_incident(report)?,
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

    /// Stores an incident on disk and in pending reports.
    fn store_incident(&mut self, report: IncidentReport) -> Result<(), TransactionObserverError> {
        self.db.store(&report)?;
        self.pending_reports.push_back(report);
        Ok(())
    }

    /// Publishes all invalidations to the dapp.
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
    #[error("Failed to open incident database: {reason}")]
    DatabaseOpen { reason: String },
    #[error("Failed to persist incident report: {reason}")]
    PersistFailed { reason: String },
}

impl From<&TransactionObserverError> for ErrorRecoverability {
    fn from(_: &TransactionObserverError) -> Self {
        ErrorRecoverability::Recoverable
    }
}

/// Opens an MDBX database holding the incident reports.
/// Used as the source of truth for what incidents we are
/// yet to report on to the dapp API.
// TODO: i dont really like that we are opening a seprate database
// for this. we should have a global sidecar mdbx.
struct IncidentDb {
    env: DatabaseEnv,
    dbi: reth_libmdbx::ffi::MDBX_dbi,
}

impl IncidentDb {
    fn open(path: &str) -> Result<Self, TransactionObserverError> {
        if path.is_empty() {
            return Err(TransactionObserverError::DatabaseOpen {
                reason: "incident db path is empty".to_string(),
            });
        }

        let path = Path::new(path);
        if !path.exists() {
            std::fs::create_dir_all(path).map_err(|e| {
                TransactionObserverError::DatabaseOpen {
                    reason: format!("Failed to create db directory: {e}"),
                }
            })?;
        }

        let args = DatabaseArguments::default();
        let env = DatabaseEnv::open(path, DatabaseEnvKind::RW, args).map_err(|e| {
            TransactionObserverError::DatabaseOpen {
                reason: e.to_string(),
            }
        })?;

        let tx = env.tx_mut().map_err(|e| {
            TransactionObserverError::DatabaseOpen {
                reason: e.to_string(),
            }
        })?;
        let db = tx
            .inner
            .create_db(Some(INCIDENT_REPORTS_TABLE), DatabaseFlags::default())
            .map_err(|e| {
                TransactionObserverError::DatabaseOpen {
                    reason: e.to_string(),
                }
            })?;
        let dbi = db.dbi();
        tx.commit().map_err(|e| {
            TransactionObserverError::DatabaseOpen {
                reason: e.to_string(),
            }
        })?;

        Ok(Self { env, dbi })
    }

    /// Store an individual incident in an on disk persistant db.
    /// Incidents stored are synced to disk immediately.
    fn store(&self, report: &IncidentReport) -> Result<(), TransactionObserverError> {
        // we need individual keys for incidents
        // we might see the same hash for multiple incidents so we just use
        // a random key
        //
        // we should never actually see duplicate incidents hit the observer
        let key = random::<u32>().to_be_bytes();

        let payload = bincode::serialize(report).map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;

        let tx = self.env.tx_mut().map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;
        let env = tx.inner.env().clone();
        tx.inner
            .put(self.dbi, &key, &payload, WriteFlags::empty())
            .map_err(|e| {
                TransactionObserverError::PersistFailed {
                    reason: e.to_string(),
                }
            })?;
        tx.commit().map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;
        env.sync(true).map_err(|e| {
            TransactionObserverError::PersistFailed {
                reason: e.to_string(),
            }
        })?;
        Ok(())
    }
}
