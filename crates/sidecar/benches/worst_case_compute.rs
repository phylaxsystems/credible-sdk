//! `worst-case-compute` benchmarks.
//!
//! Send 100 transactions targeting the same adopter (500 total assertion executions).
//! - `worst_case_compute`: Basic benchmark without observer
//! - `worst_case_compute_with_observer`: With transaction observer enabled

use assertion_executor::{
    primitives::{
        Address,
        Bytes,
        TxEnv,
        U256,
        hex,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
};
use criterion::{
    BatchSize,
    Criterion,
    criterion_group,
    criterion_main,
};
use revm::{
    context::tx::TxEnvBuilder,
    primitives::TxKind,
};
use sidecar::{
    db::SidecarDb,
    execution_ids::TxExecutionId,
    transaction_observer::{
        IncidentReportReceiver,
        IncidentReportSender,
        TransactionObserver,
        TransactionObserverConfig,
        TransactionObserverError,
    },
    utils::{
        instance::{
            LocalInstance,
            TestTransport,
        },
        profiling::{
            self,
            ProfilingGuard,
        },
        test_drivers::LocalInstanceMockDriver,
    },
};
use std::{
    fs::File,
    future::Future,
    io::BufReader,
    path::PathBuf,
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
use tempfile::TempDir;
use tokio::runtime::Runtime;

const ASSERTIONS_PER_ADOPTER: usize = 5;
const GAS_LIMIT_PER_TX: u64 = 50_000;
const OBSERVER_POLL_INTERVAL: Duration = Duration::from_secs(60);

// ============================================================================
// Shared utilities
// ============================================================================

fn read_assertions_file(input: &str) -> Vec<Bytes> {
    let file = File::open(input).expect("Failed to open assertions file");
    let reader = BufReader::new(file);
    let artifacts: Vec<String> =
        serde_json::from_reader(reader).expect("Failed to parse assertions JSON");

    artifacts
        .into_iter()
        .map(|bytecode| {
            let raw = hex::decode(bytecode).expect("Failed to decode assertion bytecode");
            Bytes::from(raw)
        })
        .collect()
}

fn read_adopters_file(input: &str) -> Vec<Address> {
    let file = File::open(input).expect("Failed to open adopters file");
    let reader = BufReader::new(file);
    let artifacts: Vec<String> =
        serde_json::from_reader(reader).expect("Failed to parse adopters JSON");

    artifacts
        .into_iter()
        .map(|address| {
            let raw = hex::decode(address).expect("Failed to decode adopter address");
            Address::from_slice(&raw)
        })
        .collect()
}

fn build_assertion_store(bytecodes: &[Bytes], adopters: &[Address]) -> AssertionStore {
    let needed = adopters.len() * ASSERTIONS_PER_ADOPTER;
    assert!(
        bytecodes.len() >= needed,
        "expected at least {needed} bytecodes, found {}",
        bytecodes.len()
    );

    let store = AssertionStore::new_ephemeral();

    for (idx, adopter) in adopters.iter().enumerate() {
        let start = idx * ASSERTIONS_PER_ADOPTER;
        let end = start + ASSERTIONS_PER_ADOPTER;
        for bytecode in &bytecodes[start..end] {
            let assertion = AssertionState::new_test(bytecode);
            store
                .insert(*adopter, assertion)
                .expect("Failed to insert assertion into store");
        }
    }

    store
}

fn get_bench_paths() -> (PathBuf, PathBuf) {
    let mut working_dir = std::env::current_dir().expect("Failed to read current directory");
    if working_dir.ends_with("credible-sdk") {
        working_dir = working_dir.join("crates/sidecar");
    }
    let assertions_path = working_dir.join("benches/assertions.json");
    let adopters_path = working_dir.join("benches/adopters.json");
    (assertions_path, adopters_path)
}

// ============================================================================
// Basic worst_case_compute benchmark
// ============================================================================

async fn setup_basic_iteration<T, F, Fut>(
    bytecodes: Arc<Vec<Bytes>>,
    adopters: Arc<Vec<Address>>,
    builder: F,
) -> (LocalInstance<T>, Vec<(TxExecutionId, TxEnv)>)
where
    T: TestTransport,
    F: FnOnce(AssertionStore) -> Fut,
    Fut: Future<Output = Result<LocalInstance<T>, String>>,
{
    let store = build_assertion_store(&bytecodes, &adopters[..1]);
    let mut instance = builder(store)
        .await
        .expect("Failed to create LocalInstance");

    instance.new_block().await.unwrap();

    let adopter = adopters[0];
    let mut transactions = Vec::with_capacity(100);
    let block_execution_id = instance.current_block_execution_id();

    for idx in 0..100 {
        let mut payload = vec![0u8; 32];
        payload[..4].copy_from_slice(&(idx as u32).to_be_bytes());
        let call_data = Bytes::from(payload);

        let nonce = instance.next_nonce(instance.default_account(), block_execution_id);
        let tx_env = TxEnvBuilder::new()
            .caller(instance.default_account())
            .gas_limit(GAS_LIMIT_PER_TX)
            .gas_price(0)
            .value(U256::ZERO)
            .nonce(nonce)
            .kind(TxKind::Call(adopter))
            .data(call_data)
            .build()
            .expect("Failed to build transaction");

        let tx_hash = LocalInstance::<T>::generate_random_tx_hash();
        let tx_execution_id = TxExecutionId::new(
            block_execution_id.block_number,
            block_execution_id.iteration_id,
            tx_hash,
            idx as u64,
        );
        transactions.push((tx_execution_id, tx_env));
    }

    (instance, transactions)
}

async fn execute_basic_iteration<T: TestTransport>(
    mut instance: LocalInstance<T>,
    transactions: Vec<(TxExecutionId, TxEnv)>,
) {
    let last_tx_execution_id = transactions
        .last()
        .map(|(tx_execution_id, _)| *tx_execution_id)
        .expect("expected at least one transaction");

    for (idx, (tx_execution_id, tx_env)) in transactions.into_iter().enumerate() {
        tracing::debug!(
            "Sending transaction {}/{}: {}",
            idx + 1,
            100,
            tx_execution_id
        );

        instance
            .transport
            .send_transaction(tx_execution_id, tx_env)
            .await
            .unwrap();
    }

    tracing::debug!(
        "Waiting for last transaction {} to complete",
        last_tx_execution_id
    );
    loop {
        match instance
            .is_transaction_successful(&last_tx_execution_id)
            .await
        {
            Ok(success) => {
                tracing::debug!(
                    "Transaction {} completed with success={}",
                    last_tx_execution_id,
                    success
                );
                break;
            }
            Err(e) => {
                if e.to_string().contains("Timeout") {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                } else {
                    panic!("error getting hash {last_tx_execution_id:?}: {}", e)
                }
            }
        }
    }
}

// ============================================================================
// Observer-enabled worst_case_compute benchmark
// ============================================================================

struct ObserverHandle {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<(), TransactionObserverError>>>,
    _tempdir: TempDir,
}

impl ObserverHandle {
    fn new(incident_rx: IncidentReportReceiver) -> Self {
        let tempdir = TempDir::new().expect("Failed to create observer temp dir");
        let config = TransactionObserverConfig {
            poll_interval: OBSERVER_POLL_INTERVAL,
            endpoint_rps_max: 0,
            endpoint: String::new(),
            auth_token: String::new(),
        };
        let sidecar_db = Arc::new(
            SidecarDb::open(&tempdir.path().to_string_lossy()).expect("Failed to open sidecar db"),
        );
        let observer = TransactionObserver::new(config, incident_rx, sidecar_db)
            .expect("Failed to create transaction observer");
        let shutdown = Arc::new(AtomicBool::new(false));
        let (handle, _exit_rx) = observer
            .spawn(Arc::clone(&shutdown))
            .expect("Failed to spawn transaction observer");
        Self {
            shutdown,
            handle: Some(handle),
            _tempdir: tempdir,
        }
    }
}

impl Drop for ObserverHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct ObservedInstance<T: TestTransport> {
    instance: LocalInstance<T>,
    _observer: ObserverHandle,
}

async fn setup_observer_iteration<T, F, Fut>(
    bytecodes: Arc<Vec<Bytes>>,
    adopters: Arc<Vec<Address>>,
    builder: F,
) -> (ObservedInstance<T>, Vec<(TxExecutionId, TxEnv)>)
where
    T: TestTransport,
    F: FnOnce(AssertionStore, IncidentReportSender) -> Fut,
    Fut: Future<Output = Result<LocalInstance<T>, String>>,
{
    let store = build_assertion_store(&bytecodes, &adopters[..1]);
    let (incident_tx, incident_rx) = flume::unbounded();
    let observer = ObserverHandle::new(incident_rx);
    let mut instance = builder(store, incident_tx)
        .await
        .expect("Failed to create LocalInstance");

    instance.new_block().await.unwrap();

    let adopter = adopters[0];
    let mut transactions = Vec::with_capacity(100);
    let block_execution_id = instance.current_block_execution_id();

    for idx in 0..100 {
        let mut payload = vec![0u8; 32];
        payload[..4].copy_from_slice(&(idx as u32).to_be_bytes());
        let call_data = Bytes::from(payload);

        let nonce = instance.next_nonce(instance.default_account(), block_execution_id);
        let tx_env = TxEnvBuilder::new()
            .caller(instance.default_account())
            .gas_limit(GAS_LIMIT_PER_TX)
            .gas_price(0)
            .value(U256::ZERO)
            .nonce(nonce)
            .kind(TxKind::Call(adopter))
            .data(call_data)
            .build()
            .expect("Failed to build transaction");

        let tx_hash = LocalInstance::<T>::generate_random_tx_hash();
        let tx_execution_id = TxExecutionId::new(
            block_execution_id.block_number,
            block_execution_id.iteration_id,
            tx_hash,
            idx as u64,
        );
        transactions.push((tx_execution_id, tx_env));
    }

    (
        ObservedInstance {
            instance,
            _observer: observer,
        },
        transactions,
    )
}

async fn execute_observer_iteration<T: TestTransport>(
    mut observed_instance: ObservedInstance<T>,
    transactions: Vec<(TxExecutionId, TxEnv)>,
) {
    let instance = &mut observed_instance.instance;
    let last_tx_execution_id = transactions
        .last()
        .map(|(tx_execution_id, _)| *tx_execution_id)
        .expect("expected at least one transaction");

    for (idx, (tx_execution_id, tx_env)) in transactions.into_iter().enumerate() {
        tracing::debug!(
            "Sending transaction {}/{}: {}",
            idx + 1,
            100,
            tx_execution_id
        );

        instance
            .transport
            .send_transaction(tx_execution_id, tx_env)
            .await
            .unwrap();
    }

    tracing::debug!(
        "Waiting for last transaction {} to complete",
        last_tx_execution_id
    );
    loop {
        match instance
            .is_transaction_successful(&last_tx_execution_id)
            .await
        {
            Ok(success) => {
                tracing::debug!(
                    "Transaction {} completed with success={}",
                    last_tx_execution_id,
                    success
                );
                break;
            }
            Err(e) => {
                if e.to_string().contains("Timeout") {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                } else {
                    panic!("error getting hash {last_tx_execution_id:?}: {}", e)
                }
            }
        }
    }
}

// ============================================================================
// Benchmark registration
// ============================================================================

fn worst_case_compute_benchmarks(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let _profiling_guard: ProfilingGuard = profiling::init_profiling(runtime.handle())
        .expect("Failed to initialize profiling utilities");

    let (assertions_path, adopters_path) = get_bench_paths();
    let bytecodes = Arc::new(read_assertions_file(&assertions_path.to_string_lossy()));
    let adopters = Arc::new(read_adopters_file(&adopters_path.to_string_lossy()));

    let mut group = c.benchmark_group("worst_case_compute");

    // Basic worst_case_compute (mock transport only)
    group.bench_function("mock", |b| {
        let bytecodes = Arc::clone(&bytecodes);
        let adopters = Arc::clone(&adopters);
        b.iter_batched(
            || {
                let bytecodes = Arc::clone(&bytecodes);
                let adopters = Arc::clone(&adopters);
                runtime.block_on(setup_basic_iteration::<LocalInstanceMockDriver, _, _>(
                    bytecodes,
                    adopters,
                    LocalInstanceMockDriver::new_with_store,
                ))
            },
            |(instance, transactions)| {
                std::hint::black_box(runtime.block_on(async move {
                    execute_basic_iteration(instance, transactions).await
                }));
            },
            BatchSize::SmallInput,
        );
    });

    // With observer (mock transport only)
    group.bench_function("with_observer", |b| {
        let bytecodes = Arc::clone(&bytecodes);
        let adopters = Arc::clone(&adopters);
        b.iter_batched(
            || {
                let bytecodes = Arc::clone(&bytecodes);
                let adopters = Arc::clone(&adopters);
                runtime.block_on(setup_observer_iteration::<LocalInstanceMockDriver, _, _>(
                    bytecodes,
                    adopters,
                    LocalInstanceMockDriver::new_with_store_and_incident_sender,
                ))
            },
            |(instance, transactions)| {
                std::hint::black_box(runtime.block_on(async move {
                    execute_observer_iteration(instance, transactions).await;
                }));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, worst_case_compute_benchmarks);
criterion_main!(benches);
