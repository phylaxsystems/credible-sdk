//! `worst-case-compute` benchmark.
//! Send 100 transactions targeting the same adopter (500 total assertion executions)

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
};
use revm::{
    context::tx::TxEnvBuilder,
    primitives::{
        TxKind,
    },
};
use sidecar::utils::{
    instance::{
        LocalInstance,
        TestTransport,
    },
    profiling::{
        self,
        ProfilingGuard,
    },
    test_drivers::{
        LocalInstanceGrpcDriver,
        LocalInstanceHttpDriver,
        LocalInstanceMockDriver,
    },
};
use sidecar::execution_ids::TxExecutionId;
use std::{
    fs::File,
    future::Future,
    io::BufReader,
    sync::Arc,
    time::Duration,
};
use tokio::runtime::Runtime;

use serde::{
    Deserialize,
    de::value::StringDeserializer,
};

const ASSERTIONS_PER_ADOPTER: usize = 5;
const GAS_LIMIT_PER_TX: u64 = 50_000;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BenchTransport {
    Mock,
    Http,
    Grpc,
}

impl Default for BenchTransport {
    fn default() -> Self {
        Self::Mock
    }
}

impl BenchTransport {
    fn as_str(self) -> &'static str {
        match self {
            Self::Mock => "mock",
            Self::Http => "http",
            Self::Grpc => "grpc",
        }
    }
}

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

    let store = AssertionStore::new_ephemeral().expect("Failed to create assertion store");

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

// Setup function: creates instance and builds transactions (not measured)
async fn setup_iteration<T, F, Fut>(
    bytecodes: Arc<Vec<Bytes>>,
    adopters: Arc<Vec<Address>>,
    builder: F,
) -> (LocalInstance<T>, Vec<(TxExecutionId, TxEnv)>)
where
    T: TestTransport,
    F: FnOnce(AssertionStore) -> Fut,
    Fut: Future<Output = Result<LocalInstance<T>, String>>,
{
    // this creates 1 adopter with 5 assertions, 5 total assertions in the store
    let store = build_assertion_store(&bytecodes, &adopters[..1]);
    let mut instance = builder(store)
        .await
        .expect("Failed to create LocalInstance");

    instance.new_block().await.unwrap();

    // build 100 transactions all targeting the same adopter
    // each transaction will execute against all 5 assertions for that adopter
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

// Execution function: sends transactions and waits for completion (measured)
async fn execute_iteration<T: TestTransport>(
    mut instance: LocalInstance<T>,
    transactions: Vec<(TxExecutionId, TxEnv)>,
) {
    // send all 100 transactions to the engine in a single block
    // 100 transactions with 5 assertions, 500 assertion executions
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

    // wait for the last transaction to complete
    //
    // txs are processed sequentially so the only way this
    // can be successful if all tx before are successful
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

fn run_benchmark_for_driver<T, SetupFn, Fut>(
    criterion: &mut Criterion,
    runtime: &Runtime,
    label: &str,
    bytecodes: Arc<Vec<Bytes>>,
    adopters: Arc<Vec<Address>>,
    setup_fn: SetupFn,
) where
    T: TestTransport + 'static,
    SetupFn: Fn(AssertionStore) -> Fut + Copy + Send + 'static,
    Fut: Future<Output = Result<LocalInstance<T>, String>>,
{
    criterion.bench_function(label, |b| {
        let bytecodes = Arc::clone(&bytecodes);
        let adopters = Arc::clone(&adopters);
        b.iter_batched(
            || {
                let bytecodes = Arc::clone(&bytecodes);
                let adopters = Arc::clone(&adopters);
                runtime.block_on(setup_iteration::<T, _, _>(bytecodes, adopters, setup_fn))
            },
            |(instance, transactions)| {
                std::hint::black_box(runtime.block_on(async move {
                    execute_iteration(instance, transactions).await
                }));
            },
            BatchSize::SmallInput,
        );
    });
}

fn main() {
    let runtime = Runtime::new().unwrap();
    let _profiling_guard: ProfilingGuard = profiling::init_profiling(runtime.handle())
        .expect("Failed to initialize profiling utilities");

    // for flamegraphs w/ root: .../credible-sdk
    // for non root its: ...credible-sdk/crates/sidecar
    let mut working_dir = std::env::current_dir().expect("Failed to read current directory");
    if working_dir.ends_with("credible-sdk") {
        working_dir = working_dir.join("crates/sidecar");
    }
    let assertions_path = working_dir.join("benches/assertions.json");
    let adopters_path = working_dir.join("benches/adopters.json");

    let bytecodes = Arc::new(read_assertions_file(&assertions_path.to_string_lossy()));
    let adopters = Arc::new(read_adopters_file(&adopters_path.to_string_lossy()));

    let mut criterion = Criterion::default();

    let transport = std::env::var("SIDECAR_BENCH_TRANSPORT")
        .ok()
        .and_then(|raw| {
            let de = StringDeserializer::<serde::de::value::Error>::new(raw.to_ascii_lowercase());
            BenchTransport::deserialize(de).ok()
        })
        .unwrap_or_default();
    let bench_label = format!("worst_case_compute ({})", transport.as_str());
    tracing::info!("Running benchmark with `{}` transport", transport.as_str());

    match transport {
        BenchTransport::Grpc => {
            run_benchmark_for_driver::<LocalInstanceGrpcDriver, _, _>(
                &mut criterion,
                &runtime,
                &bench_label,
                Arc::clone(&bytecodes),
                Arc::clone(&adopters),
                LocalInstanceGrpcDriver::new_with_store,
            )
        }
        BenchTransport::Http => {
            run_benchmark_for_driver::<LocalInstanceHttpDriver, _, _>(
                &mut criterion,
                &runtime,
                &bench_label,
                Arc::clone(&bytecodes),
                Arc::clone(&adopters),
                LocalInstanceHttpDriver::new_with_store,
            )
        }
        BenchTransport::Mock => {
            run_benchmark_for_driver::<LocalInstanceMockDriver, _, _>(
                &mut criterion,
                &runtime,
                &bench_label,
                Arc::clone(&bytecodes),
                Arc::clone(&adopters),
                LocalInstanceMockDriver::new_with_store,
            )
        }
    }

    criterion.final_summary();
}
