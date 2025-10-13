//! `worst-case-compute`
//!
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
        B256,
        TxKind,
    },
};
use sidecar::utils::{
    instance::{
        LocalInstance,
        TestTransport,
    },
    test_drivers::LocalInstanceGrpcDriver,
};
use std::{
    fs::File,
    io::BufReader,
    sync::Arc,
    time::Duration,
};
use tokio::runtime::Runtime;

const ASSERTIONS_PER_ADOPTER: usize = 5;
const GAS_LIMIT_PER_TX: u64 = 50_000;

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
async fn setup_iteration(
    bytecodes: Arc<Vec<Bytes>>,
    adopters: Arc<Vec<Address>>,
) -> (
    LocalInstance<LocalInstanceGrpcDriver>,
    Vec<(B256, TxEnv)>,
    Vec<B256>,
) {
    // this creates 1 adopter with 5 assertions, 5 total assertions in the store
    let store = build_assertion_store(&bytecodes, &adopters[..1]);
    let mut instance = LocalInstanceGrpcDriver::new_with_store(store)
        .await
        .expect("Failed to create LocalInstance");

    // build 100 transactions all targeting the same adopter
    // each transaction will execute against all 5 assertions for that adopter
    let adopter = adopters[0];
    let mut transactions = Vec::with_capacity(100);
    let mut hashes = Vec::with_capacity(100);

    for idx in 0..100 {
        let mut payload = vec![0u8; 32];
        payload[..4].copy_from_slice(&(idx as u32).to_be_bytes());
        let call_data = Bytes::from(payload);

        let nonce = instance.next_nonce();
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

        let tx_hash = LocalInstance::<LocalInstanceGrpcDriver>::generate_random_tx_hash();
        hashes.push(tx_hash);
        transactions.push((tx_hash, tx_env));
    }

    instance.new_block().await.unwrap();

    (instance, transactions, hashes)
}

// Execution function: sends transactions and waits for completion (measured)
async fn execute_iteration(
    mut instance: LocalInstance<LocalInstanceGrpcDriver>,
    transactions: Vec<(B256, TxEnv)>,
    hashes: Vec<B256>,
) {
    // send all 100 transactions to the engine in a single block
    // 100 transactions with 5 assertions, 500 assertion executions
    for (idx, (tx_hash, tx_env)) in transactions.into_iter().enumerate() {
        tracing::debug!("Sending transaction {}/{}: {}", idx + 1, 100, tx_hash);

        instance
            .transport
            .send_transaction(tx_hash, tx_env)
            .await
            .unwrap();
    }

    // wait for the last transaction to complete
    //
    // txs are processed sequentially so the only way this
    // can be successful if all tx before are successful
    let tx_hash = hashes.last().unwrap();
    tracing::debug!("Waiting for last transaction {} to complete", tx_hash);
    loop {
        match instance.is_transaction_successful(&tx_hash).await {
            Ok(success) => {
                tracing::debug!("Transaction {} completed with success={}", tx_hash, success);
                break;
            }
            Err(e) => {
                if e.to_string().contains("Timeout") {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                } else {
                    panic!("error getting hash {tx_hash:?}: {}", e)
                }
            }
        }
    }
}

fn main() {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let runtime = Runtime::new().unwrap();

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

    criterion.bench_function("worst_case_compute", |b| {
        let bytecodes = Arc::clone(&bytecodes);
        let adopters = Arc::clone(&adopters);
        b.iter_batched(
            || {
                // setup (not measured): create instance and build transactions
                let bytecodes = Arc::clone(&bytecodes);
                let adopters = Arc::clone(&adopters);
                runtime.block_on(async move { setup_iteration(bytecodes, adopters).await })
            },
            |(instance, transactions, hashes)| {
                std::hint::black_box(runtime.block_on(async move {
                    execute_iteration(instance, transactions, hashes).await
                }));
            },
            BatchSize::SmallInput,
        );
    });

    criterion.final_summary();
}
