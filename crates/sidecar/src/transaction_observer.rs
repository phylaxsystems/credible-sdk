use crate::utils::ErrorRecoverability;
use chrono::{
    NaiveDateTime,
    Utc,
};
use dapp_api_client::{
    Client as DappClient,
    Config as DappConfig,
    generated::types as dapp_types,
};
use futures::stream::{
    FuturesUnordered,
    StreamExt,
};
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
    context_interface::either::Either,
    context_interface::transaction::{
        AccessList,
        RecoveredAuthorization,
        SignedAuthorization,
    },
    primitives::{
        Address,
        Bytes,
        FixedBytes,
        TxKind,
        U256,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    Map,
    Value,
};
use std::{
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
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::{
    info,
    warn,
};
use url::Url;

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
    db: IncidentDb,
    dapp_client: Option<Arc<DappClient>>,
    runtime: Runtime,
}

impl TransactionObserver {
    pub fn new(
        config: TransactionObserverConfig,
        incident_rx: IncidentReportReceiver,
    ) -> Result<Self, TransactionObserverError> {
        let db = IncidentDb::open(&config.db_path)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| TransactionObserverError::PublishFailed {
                reason: format!("Failed to initialize dapp runtime: {e}"),
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

    /// Stores an incident on disk.
    fn store_incident(&mut self, report: IncidentReport) -> Result<(), TransactionObserverError> {
        self.db.store(&report)?;
        Ok(())
    }

    /// Publishes all invalidations to the dapp.
    ///
    /// Gets a consistent view of the db for unpublished incidents, and then
    /// tries to push them to the api in parallel. If the response is a tcp
    /// success, we can remove it from the db. If not, we leave it for the
    /// next run.
    fn publish_invalidations(&mut self) -> Result<(), TransactionObserverError> {
        if self.config.endpoint.trim().is_empty() || self.config.endpoint_rps_max == 0 {
            return Ok(());
        }

        if self.dapp_client.is_none() {
            self.dapp_client = build_dapp_client(&self.config)?;
        }
        let Some(dapp_client) = self.dapp_client.as_ref() else {
            return Ok(());
        };

        let incidents = self
            .db
            .load_batch(self.config.endpoint_rps_max)?;
        if incidents.is_empty() {
            return Ok(());
        }

        let auth_token = Arc::new(self.config.auth_token.trim().to_string());
        let dapp_client = Arc::clone(dapp_client);
        let results = self.runtime.block_on(async move {
            let mut tasks = FuturesUnordered::new();
            for (key, report) in incidents {
                let dapp_client = Arc::clone(&dapp_client);
                let auth_token = Arc::clone(&auth_token);
                tasks.push(async move {
                    let body = match build_incident_body(&report) {
                        Ok(body) => body,
                        Err(err) => {
                            warn!(
                                target = "transaction_observer",
                                error = ?err,
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
                        warn!(
                            target = "transaction_observer",
                            error = ?err,
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

        let keys_to_delete: Vec<Vec<u8>> = results
            .into_iter()
            .filter_map(|(key, success)| success.then_some(key))
            .collect();
        self.db.delete_keys(&keys_to_delete)?;
        Ok(())
    }
}

fn build_dapp_client(
    config: &TransactionObserverConfig,
) -> Result<Option<Arc<DappClient>>, TransactionObserverError> {
    let endpoint = config.endpoint.trim();
    if endpoint.is_empty() {
        return Ok(None);
    }

    let base_url = endpoint_base_url(endpoint);
    if base_url.is_empty() {
        return Ok(None);
    }

    let client = DappClient::new(DappConfig::new(base_url)).map_err(|e| {
        TransactionObserverError::PublishFailed {
            reason: format!("Failed to create dapp API client: {e}"),
        }
    })?;

    Ok(Some(Arc::new(client)))
}

fn endpoint_base_url(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }

    if let Ok(mut url) = Url::parse(trimmed) {
        if url.path().ends_with("/enforcer/incidents") {
            let new_path = url.path().trim_end_matches("/enforcer/incidents");
            let new_path = new_path.trim_end_matches('/');
            url.set_path(if new_path.is_empty() { "/" } else { new_path });
        }
        return url.to_string().trim_end_matches('/').to_string();
    }

    trimmed
        .trim_end_matches("/enforcer/incidents")
        .trim_end_matches('/')
        .to_string()
}

fn build_incident_body(
    report: &IncidentReport,
) -> Result<dapp_types::PostEnforcerIncidentsBody, TransactionObserverError> {
    let incident_timestamp = format_incident_timestamp(report.incident_timestamp)?;
    let failures = report
        .failures
        .iter()
        .map(build_failure_payload)
        .collect::<Result<Vec<_>, _>>()?;
    let transaction_data = build_transaction_data_payload(&report.transaction_data)?;

    let mut body = Map::new();
    body.insert("failures".to_string(), Value::Array(failures));
    body.insert(
        "incident_timestamp".to_string(),
        Value::String(incident_timestamp),
    );
    body.insert("transaction_data".to_string(), transaction_data);
    body.insert(
        "block_env".to_string(),
        Value::Object(build_block_env_payload(&report.block_env)),
    );

    let previous_transactions = build_previous_transactions_payload(&report.prev_txs)?;
    if !previous_transactions.is_empty() {
        body.insert(
            "previous_transactions".to_string(),
            Value::Array(previous_transactions),
        );
    }

    serde_json::from_value(Value::Object(body)).map_err(|e| {
        TransactionObserverError::PublishFailed {
            reason: format!("Failed to build incident payload: {e}"),
        }
    })
}

fn format_incident_timestamp(timestamp: u64) -> Result<String, TransactionObserverError> {
    let seconds = i64::try_from(timestamp).map_err(|_| {
        TransactionObserverError::PublishFailed {
            reason: format!("Incident timestamp out of range: {timestamp}"),
        }
    })?;
    let naive = NaiveDateTime::from_timestamp_opt(seconds, 0).ok_or_else(|| {
        TransactionObserverError::PublishFailed {
            reason: format!("Invalid incident timestamp: {timestamp}"),
        }
    })?;
    Ok(chrono::DateTime::<Utc>::from_utc(naive, Utc).to_rfc3339())
}

fn build_block_env_payload(block_env: &BlockEnv) -> Map<String, Value> {
    let mut payload = Map::new();
    payload.insert(
        "number".to_string(),
        Value::String(block_env.number.to_string()),
    );
    payload.insert(
        "beneficiary".to_string(),
        Value::String(bytes_to_hex(block_env.beneficiary.as_slice())),
    );
    payload.insert(
        "timestamp".to_string(),
        Value::String(block_env.timestamp.to_string()),
    );
    payload.insert(
        "gas_limit".to_string(),
        Value::String(block_env.gas_limit.to_string()),
    );
    payload.insert(
        "basefee".to_string(),
        Value::String(block_env.basefee.to_string()),
    );
    payload.insert(
        "difficulty".to_string(),
        Value::String(block_env.difficulty.to_string()),
    );

    if let Some(prevrandao) = block_env.prevrandao {
        payload.insert(
            "prevrandao".to_string(),
            Value::String(bytes_to_hex(prevrandao.as_slice())),
        );
    }

    if let Some(blob) = block_env.blob_excess_gas_and_price {
        let mut blob_payload = Map::new();
        blob_payload.insert(
            "excess_blob_gas".to_string(),
            Value::String(blob.excess_blob_gas.to_string()),
        );
        blob_payload.insert(
            "blob_gasprice".to_string(),
            Value::String(blob.blob_gasprice.to_string()),
        );
        payload.insert(
            "blob_excess_gas_and_price".to_string(),
            Value::Object(blob_payload),
        );
    }

    payload
}

fn build_failure_payload(failure: &IncidentData) -> Result<Value, TransactionObserverError> {
    let mut payload = Map::new();
    payload.insert(
        "assertion_adopter_address".to_string(),
        Value::String(bytes_to_hex(failure.adopter_address.as_slice())),
    );
    payload.insert(
        "assertion_id".to_string(),
        Value::String(bytes_to_hex(failure.assertion_id.as_slice())),
    );
    payload.insert(
        "assertion_fn_selector".to_string(),
        Value::String(bytes_to_hex(failure.assertion_fn.as_slice())),
    );

    if !failure.revert_data.is_empty() {
        payload.insert(
            "revert_reason".to_string(),
            Value::String(bytes_to_hex(failure.revert_data.as_ref())),
        );
    }

    Ok(Value::Object(payload))
}

fn build_previous_transactions_payload(
    previous_txs: &[ReconstructableTx],
) -> Result<Vec<Value>, TransactionObserverError> {
    previous_txs
        .iter()
        .map(|(_, tx_env)| build_previous_transaction_payload(tx_env))
        .collect()
}

fn build_previous_transaction_payload(tx_env: &TxEnv) -> Result<Value, TransactionObserverError> {
    let mut payload = Map::new();
    payload.insert(
        "from".to_string(),
        Value::String(bytes_to_hex(tx_env.caller.as_slice())),
    );
    payload.insert(
        "to".to_string(),
        Value::String(tx_kind_to_address(&tx_env.kind, false)),
    );
    payload.insert(
        "value".to_string(),
        Value::String(tx_env.value.to_string()),
    );
    if !tx_env.data.is_empty() {
        payload.insert(
            "calldata".to_string(),
            Value::String(bytes_to_hex(tx_env.data.as_ref())),
        );
    }
    Ok(Value::Object(payload))
}

fn build_transaction_data_payload(
    transaction: &ReconstructableTx,
) -> Result<Value, TransactionObserverError> {
    let (tx_hash, tx_env) = transaction;
    let chain_id = match tx_env.chain_id {
        Some(chain_id) if chain_id > 0 => chain_id,
        _ => {
            return Err(TransactionObserverError::PublishFailed {
                reason: "Transaction chain_id missing or invalid".to_string(),
            });
        }
    };

    let mut payload = Map::new();
    payload.insert(
        "transaction_hash".to_string(),
        Value::String(bytes_to_hex(tx_hash.as_slice())),
    );
    payload.insert("chain_id".to_string(), Value::Number(chain_id.into()));
    payload.insert(
        "nonce".to_string(),
        Value::String(tx_env.nonce.to_string()),
    );
    payload.insert(
        "gas_limit".to_string(),
        Value::String(tx_env.gas_limit.to_string()),
    );
    payload.insert(
        "to_address".to_string(),
        Value::String(tx_kind_to_address(&tx_env.kind, true)),
    );
    payload.insert(
        "from_address".to_string(),
        Value::String(bytes_to_hex(tx_env.caller.as_slice())),
    );
    payload.insert(
        "value".to_string(),
        Value::String(tx_env.value.to_string()),
    );
    payload.insert(
        "type".to_string(),
        Value::Number((tx_env.tx_type as u64).into()),
    );
    if !tx_env.data.is_empty() {
        payload.insert(
            "data".to_string(),
            Value::String(bytes_to_hex(tx_env.data.as_ref())),
        );
    }

    match tx_env.tx_type {
        0 => {
            payload.insert(
                "gas_price".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
        }
        1 => {
            payload.insert(
                "gas_price".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
            let access_list = access_list_payload(&tx_env.access_list);
            if !access_list.is_empty() {
                payload.insert("access_list".to_string(), Value::Array(access_list));
            }
        }
        2 => {
            payload.insert(
                "max_fee_per_gas".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
            payload.insert(
                "max_priority_fee_per_gas".to_string(),
                Value::String(tx_env.gas_priority_fee.unwrap_or_default().to_string()),
            );
            let access_list = access_list_payload(&tx_env.access_list);
            if !access_list.is_empty() {
                payload.insert("access_list".to_string(), Value::Array(access_list));
            }
        }
        3 => {
            payload.insert(
                "max_fee_per_blob_gas".to_string(),
                Value::String(tx_env.max_fee_per_blob_gas.to_string()),
            );
            payload.insert(
                "max_fee_per_gas".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
            payload.insert(
                "max_priority_fee_per_gas".to_string(),
                Value::String(tx_env.gas_priority_fee.unwrap_or_default().to_string()),
            );
            payload.insert(
                "blob_versioned_hashes".to_string(),
                Value::Array(
                    tx_env
                        .blob_hashes
                        .iter()
                        .map(|hash| Value::String(bytes_to_hex(hash.as_slice())))
                        .collect(),
                ),
            );
            let access_list = access_list_payload(&tx_env.access_list);
            if !access_list.is_empty() {
                payload.insert("access_list".to_string(), Value::Array(access_list));
            }
        }
        4 => {
            payload.insert(
                "max_fee_per_gas".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
            payload.insert(
                "max_priority_fee_per_gas".to_string(),
                Value::String(tx_env.gas_priority_fee.unwrap_or_default().to_string()),
            );
            let access_list = access_list_payload(&tx_env.access_list);
            if !access_list.is_empty() {
                payload.insert("access_list".to_string(), Value::Array(access_list));
            }
            payload.insert(
                "authorization_list".to_string(),
                Value::Array(authorization_list_payload(&tx_env.authorization_list)),
            );
        }
        tx_type => {
            return Err(TransactionObserverError::PublishFailed {
                reason: format!("Unsupported transaction type: {tx_type}"),
            });
        }
    }

    Ok(Value::Object(payload))
}

fn access_list_payload(access_list: &AccessList) -> Vec<Value> {
    access_list
        .iter()
        .map(|item| {
            let mut payload = Map::new();
            payload.insert(
                "address".to_string(),
                Value::String(bytes_to_hex(item.address.as_slice())),
            );
            payload.insert(
                "storage_keys".to_string(),
                Value::Array(
                    item.storage_keys
                        .iter()
                        .map(|key| Value::String(bytes_to_hex(key.as_slice())))
                        .collect(),
                ),
            );
            Value::Object(payload)
        })
        .collect()
}

fn authorization_list_payload(
    authorization_list: &[Either<SignedAuthorization, RecoveredAuthorization>],
) -> Vec<Value> {
    authorization_list
        .iter()
        .map(|authorization| {
            let (authorization, r, s, v) = match authorization {
                Either::Left(signed) => (signed.inner(), signed.r(), signed.s(), signed.y_parity()),
                Either::Right(recovered) => {
                    warn!(
                        target = "transaction_observer",
                        "Authorization list entry missing signature, using zeroed values"
                    );
                    (&**recovered, U256::ZERO, U256::ZERO, 0)
                }
            };

            let mut payload = Map::new();
            payload.insert(
                "chain_id".to_string(),
                Value::String(u256_to_hex(*authorization.chain_id())),
            );
            payload.insert(
                "address".to_string(),
                Value::String(bytes_to_hex(authorization.address().as_slice())),
            );
            payload.insert(
                "nonce".to_string(),
                Value::String(u64_to_hex(authorization.nonce())),
            );
            payload.insert("v".to_string(), Value::String(u64_to_hex(u64::from(v))));
            payload.insert("r".to_string(), Value::String(u256_to_hex(r)));
            payload.insert("s".to_string(), Value::String(u256_to_hex(s)));
            Value::Object(payload)
        })
        .collect()
}

fn tx_kind_to_address(kind: &TxKind, allow_empty: bool) -> String {
    match kind {
        TxKind::Call(to) => bytes_to_hex(to.as_slice()),
        TxKind::Create => {
            if allow_empty {
                String::new()
            } else {
                bytes_to_hex(Address::ZERO.as_slice())
            }
        }
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn u256_to_hex(value: U256) -> String {
    format!("0x{value:x}")
}

fn u64_to_hex(value: u64) -> String {
    format!("0x{value:x}")
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

    /// Loads up to `limit` incident reports from a consistent snapshot.
    ///
    /// Returns (key, report) pairs so callers can delete on success.
    fn load_batch(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, IncidentReport)>, TransactionObserverError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let tx = self.env.tx().map_err(|e| TransactionObserverError::PublishFailed {
            reason: e.to_string(),
        })?;
        let mut cursor = tx
            .inner
            .cursor_with_dbi(self.dbi)
            .map_err(|e| TransactionObserverError::PublishFailed {
                reason: e.to_string(),
            })?;

        let mut reports = Vec::new();
        let mut next = cursor
            .first::<Vec<u8>, Vec<u8>>()
            .map_err(|e| TransactionObserverError::PublishFailed {
                reason: e.to_string(),
            })?;
        while let Some((key, payload)) = next {
            let report = bincode::deserialize(&payload).map_err(|e| {
                TransactionObserverError::PublishFailed {
                    reason: e.to_string(),
                }
            })?;
            reports.push((key, report));
            if reports.len() >= limit {
                break;
            }
            next = cursor
                .next::<Vec<u8>, Vec<u8>>()
                .map_err(|e| TransactionObserverError::PublishFailed {
                    reason: e.to_string(),
                })?;
        }

        Ok(reports)
    }

    /// Deletes incident reports by key in a single write transaction.
    fn delete_keys(&self, keys: &[Vec<u8>]) -> Result<(), TransactionObserverError> {
        if keys.is_empty() {
            return Ok(());
        }

        let tx = self.env.tx_mut().map_err(|e| TransactionObserverError::PublishFailed {
            reason: e.to_string(),
        })?;
        for key in keys {
            tx.inner
                .del(self.dbi, key, None)
                .map_err(|e| TransactionObserverError::PublishFailed {
                    reason: e.to_string(),
                })?;
        }
        tx.commit().map_err(|e| TransactionObserverError::PublishFailed {
            reason: e.to_string(),
        })?;
        Ok(())
    }
}
