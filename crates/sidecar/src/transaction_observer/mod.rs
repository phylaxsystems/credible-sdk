mod client;
mod db;
mod payload;
#[cfg(test)]
mod tests;

use crate::utils::ErrorRecoverability;
use dapp_api_client::Client as DappClient;
use futures::stream::{
    FuturesUnordered,
    StreamExt,
};
use metrics::{
    counter,
    histogram,
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
use tokio::{
    runtime::Runtime,
    sync::oneshot,
};
use tracing::{
    debug,
    info,
    instrument,
    trace,
    warn,
};

use self::{
    client::build_dapp_client,
    db::IncidentDb,
    payload::build_incident_body,
};

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
    db: IncidentDb,
    dapp_client: Option<Arc<DappClient>>,
    runtime: Runtime,
}

impl TransactionObserver {
    pub fn new(
        config: TransactionObserverConfig,
        incident_rx: IncidentReportReceiver,
    ) -> Result<Self, TransactionObserverError> {
        if config.endpoint.trim().is_empty() {
            warn!(
                target = "transaction_observer",
                "Dapp endpoint not configured; incident publishing disabled"
            );
        }
        let db = IncidentDb::open(&config.db_path)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                TransactionObserverError::PublishFailed {
                    reason: format!("Failed to initialize dapp runtime: {e}"),
                }
            })?;
        let dapp_client = build_dapp_client(&config)?;
        Ok(Self {
            config,
            incident_rx,
            db,
            dapp_client,
            runtime,
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
                let result = observer.run_blocking(&shutdown);
                let _ = tx.send(result.clone());
                result
            })?;

        Ok((handle, rx))
    }

    fn run_blocking(&mut self, shutdown: &Arc<AtomicBool>) -> Result<(), TransactionObserverError> {
        let mut last_publish = Instant::now();
        loop {
            if shutdown.load(Ordering::Relaxed) {
                info!(target = "transaction_observer", "Shutdown signal received");
                return Ok(());
            }

            match self.incident_rx.recv_timeout(RECV_TIMEOUT) {
                Ok(report) => self.store_incident(&report)?,
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

    /// Stores an incident on disk.
    fn store_incident(&mut self, report: &IncidentReport) -> Result<(), TransactionObserverError> {
        counter!("transaction_observer_incidents_received_total").increment(1);
        if let Err(err) = self.db.store(report) {
            if matches!(err, TransactionObserverError::PersistFailed { .. }) {
                counter!("transaction_observer_incidents_persist_failed_total").increment(1);
            }
            return Err(err);
        }
        Ok(())
    }

    #[instrument(
        name = "transaction_observer::publish_invalidations",
        skip(self),
        fields(endpoint = %self.config.endpoint,),
        level = "debug"
    )]
    /// Publishes all invalidations to the dapp.
    ///
    /// Gets a consistent view of the db for unpublished incidents, and then
    /// tries to push them to the api in parallel. If the response is a tcp
    /// success, we can remove it from the db. If not, we leave it for the
    /// next run.
    fn publish_invalidations(&mut self) -> Result<(), TransactionObserverError> {
        if self.config.endpoint.trim().is_empty() || self.config.endpoint_rps_max == 0 {
            warn!(
                target = "transaction_observer",
                "Dapp endpoint or rps empty/0; skipping incident publishing"
            );
            return Ok(());
        }

        if self.dapp_client.is_none() {
            self.dapp_client = build_dapp_client(&self.config)?;
        }
        let Some(dapp_client) = self.dapp_client.as_ref() else {
            return Ok(());
        };

        let incidents = self.db.load_batch(self.config.endpoint_rps_max)?;
        if incidents.is_empty() {
            trace!(target = "transaction_observer", "No incidents to publish");
            return Ok(());
        }
        let publish_started_at = Instant::now();
        debug!(
            target = "transaction_observer",
            incident_count = incidents.len(),
            "Publishing incidents to dapp"
        );

        let auth_token = self.config.auth_token.trim().to_string();
        let dapp_client = Arc::clone(dapp_client);
        let results = self.runtime.block_on(async move {
            let mut tasks = FuturesUnordered::new();
            for (key, report) in incidents {
                let dapp_client = Arc::clone(&dapp_client);
                let auth_token = auth_token.clone();
                tasks.push(async move {
                    let (tx_hash, _) = &report.transaction_data;
                    let body = match build_incident_body(&report) {
                        Ok(body) => body,
                        Err(err) => {
                            warn!(
                                target = "transaction_observer",
                                error = ?err,
                                tx_hash = ?tx_hash,
                                "Failed to build incident payload"
                            );
                            return (key, false);
                        }
                    };

                    let x_api_key = if auth_token.is_empty() {
                        None
                    } else {
                        Some(auth_token.as_str())
                    };

                    let result = dapp_client
                        .inner()
                        .post_enforcer_incidents(x_api_key, &body)
                        .await;
                    let success = result.is_ok();
                    if let Err(err) = result {
                        trace!(
                            target = "transaction_observer",
                            tx_hash = ?tx_hash,
                            incidents_for_tx = report.failures.len(),
                            "Incident publish failed"
                        );
                        warn!(
                            target = "transaction_observer",
                            error = ?err,
                            tx_hash = ?tx_hash,
                            "Failed to publish invalidation incident"
                        );
                    }
                    (key, success)
                });
            }

            let mut completed = Vec::new();
            while let Some(result) = tasks.next().await {
                completed.push(result);
            }
            completed
        });

        let total_results = results.len();
        let keys_to_delete: Vec<Vec<u8>> = results
            .into_iter()
            .filter_map(|(key, success)| success.then_some(key))
            .collect();
        self.db.delete_keys(&keys_to_delete)?;
        let success_count = keys_to_delete.len();
        let failure_count = total_results.saturating_sub(success_count);
        counter!("transaction_observer_publish_success_total").increment(success_count as u64);
        counter!("transaction_observer_publish_failure_total").increment(failure_count as u64);
        histogram!("transaction_observer_publish_duration_seconds")
            .record(publish_started_at.elapsed());
        debug!(
            target = "transaction_observer",
            published = success_count,
            failed = failure_count,
            "Finished publishing incidents"
        );
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
    #[error("Incident report I/O failure: {reason}")]
    IoFailure { reason: String },
    #[error("Failed to decode incident report: {reason}")]
    DeserializeFailed { reason: String },
}

impl From<&TransactionObserverError> for ErrorRecoverability {
    fn from(_: &TransactionObserverError) -> Self {
        ErrorRecoverability::Recoverable
    }
}
