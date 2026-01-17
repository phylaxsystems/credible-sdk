//! `reorg_per_tx` benchmark.
//! Send ERC20 transfers with a reorg after each transaction:
//! tx1 -> reorg tx1 -> tx1 -> tx2 -> reorg tx2 -> tx2 -> ...
//!
//! This measures the overhead of reorg handling in the sidecar.
//!
//! As a part of the setup, deploys an ERC20 contract, and mints `U256::MAX` to the
//! default instance account. The transactions then send `u32::MAX` tokens to the
//! `0x000...` burn address.

use assertion_executor::{
    primitives::{
        Address,
        Bytes,
        TxEnv,
        U256,
        hex,
    },
    store::AssertionStore,
};
use criterion::{
    BatchSize,
    Criterion,
};
use revm::{
    context::tx::TxEnvBuilder,
    primitives::TxKind,
};
use serde::Deserialize;
use sidecar::{
    execution_ids::TxExecutionId,
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
    path::{
        Path,
        PathBuf,
    },
    time::Duration,
};
use tokio::runtime::Runtime;

const NUM_TRANSACTIONS: usize = 20;
const DEPLOY_GAS_LIMIT: u64 = 2_000_000;
const GAS_LIMIT_PER_TX: u64 = 100_000;
const ERC20_TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];

#[derive(Deserialize)]
struct BytecodeArtifact {
    bytecode: BytecodeObject,
}

#[derive(Deserialize)]
struct BytecodeObject {
    object: String,
}

fn read_erc20_bytecode(path: &Path) -> Bytes {
    let file = File::open(path).expect("Failed to open ERC20 bytecode artifact");
    let reader = BufReader::new(file);
    let artifact: BytecodeArtifact =
        serde_json::from_reader(reader).expect("Failed to parse ERC20 bytecode artifact");
    let bytecode = artifact.bytecode.object;
    let raw = hex::decode(bytecode.strip_prefix("0x").unwrap_or(&bytecode))
        .expect("Failed to decode ERC20 bytecode");
    Bytes::from(raw)
}

fn erc20_bytecode_path() -> PathBuf {
    let working_dir = std::env::current_dir().expect("Failed to read current directory");
    let repo_root = if working_dir.ends_with("credible-sdk") {
        working_dir
    } else if working_dir.ends_with(Path::new("crates/sidecar")) {
        working_dir
            .parent()
            .and_then(|parent| parent.parent())
            .expect("Failed to resolve repo root from crates/sidecar")
            .to_path_buf()
    } else {
        working_dir
    };
    repo_root.join("testdata/mock-protocol/out/erc20.sol/GLDToken.json")
}

fn encode_erc20_transfer(to: Address, amount: U256) -> Bytes {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&ERC20_TRANSFER_SELECTOR);
    data.extend_from_slice(&[0u8; 12]);
    data.extend_from_slice(to.as_slice());
    data.extend_from_slice(&amount.to_be_bytes::<32>());
    Bytes::from(data)
}

async fn deploy_erc20<T: TestTransport>(
    instance: &mut LocalInstance<T>,
    erc20_bytecode: Bytes,
) -> Address {
    instance
        .new_block()
        .await
        .expect("Failed to open deploy block");
    let block_execution_id = instance.current_block_execution_id();
    let nonce = instance.next_nonce(instance.default_account(), block_execution_id);
    let contract_address = instance.default_account().create(nonce);
    let tx_env = TxEnvBuilder::new()
        .caller(instance.default_account())
        .gas_limit(DEPLOY_GAS_LIMIT)
        .gas_price(0)
        .value(U256::ZERO)
        .nonce(nonce)
        .kind(TxKind::Create)
        .data(erc20_bytecode)
        .build()
        .expect("Failed to build deploy transaction");

    let tx_hash = LocalInstance::<T>::generate_random_tx_hash();
    let tx_execution_id = TxExecutionId::new(
        block_execution_id.block_number,
        block_execution_id.iteration_id,
        tx_hash,
        0,
    );

    instance
        .transport
        .send_transaction(tx_execution_id, tx_env)
        .await
        .expect("Failed to send deploy transaction");

    loop {
        match instance.is_transaction_successful(&tx_execution_id).await {
            Ok(success) => {
                if success {
                    break;
                }
                panic!("ERC20 deploy transaction failed");
            }
            Err(e) => {
                if e.to_string().contains("Timeout") {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }
                panic!("error getting deploy result {tx_execution_id:?}: {}", e);
            }
        }
    }

    contract_address
}

/// A transaction template that can be re-submitted after reorg.
/// Contains all the data needed to rebuild the transaction with the same nonce.
struct TxTemplate {
    caller: Address,
    nonce: u64,
    to: Address,
    data: Bytes,
    index: u64,
}

// Setup function: creates instance and builds transaction templates (not measured)
async fn setup_iteration<T, F, Fut>(
    erc20_bytecode: Bytes,
    builder: F,
) -> (LocalInstance<T>, Vec<TxTemplate>)
where
    T: TestTransport,
    F: FnOnce(AssertionStore) -> Fut,
    Fut: Future<Output = Result<LocalInstance<T>, String>>,
{
    let store = AssertionStore::new_ephemeral();
    let mut instance = builder(store)
        .await
        .expect("Failed to create LocalInstance");

    let erc20_address = deploy_erc20(&mut instance, erc20_bytecode).await;
    instance.new_block().await.unwrap();

    let transfer_data = encode_erc20_transfer(Address::ZERO, U256::from(u32::MAX));

    // Build transaction templates
    let mut templates = Vec::with_capacity(NUM_TRANSACTIONS);
    let block_execution_id = instance.current_block_execution_id();

    for idx in 0..NUM_TRANSACTIONS {
        let nonce = instance.next_nonce(instance.default_account(), block_execution_id);
        templates.push(TxTemplate {
            caller: instance.default_account(),
            nonce,
            to: erc20_address,
            data: transfer_data.clone(),
            index: idx as u64,
        });
    }

    (instance, templates)
}

fn build_tx_env(template: &TxTemplate) -> TxEnv {
    TxEnvBuilder::new()
        .caller(template.caller)
        .gas_limit(GAS_LIMIT_PER_TX)
        .gas_price(0)
        .value(U256::ZERO)
        .nonce(template.nonce)
        .kind(TxKind::Call(template.to))
        .data(template.data.clone())
        .build()
        .expect("Failed to build transaction")
}

/// Wait for a transaction to complete processing.
async fn wait_for_tx<T: TestTransport>(
    instance: &LocalInstance<T>,
    tx_execution_id: &TxExecutionId,
) {
    loop {
        match instance.is_transaction_successful(tx_execution_id).await {
            Ok(_success) => {
                break;
            }
            Err(e) => {
                if e.to_string().contains("Timeout") {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                } else {
                    panic!("error getting tx result {tx_execution_id:?}: {}", e);
                }
            }
        }
    }
}

// Execution function: sends transactions with reorg after each (measured)
// Pattern: tx1 -> reorg tx1 -> tx1 -> tx2 -> reorg tx2 -> tx2 -> ...
async fn execute_iteration<T: TestTransport>(
    mut instance: LocalInstance<T>,
    templates: Vec<TxTemplate>,
) {
    let block_execution_id = instance.current_block_execution_id();

    for (idx, template) in templates.into_iter().enumerate() {
        // First submission of the transaction
        let tx_hash_1 = LocalInstance::<T>::generate_random_tx_hash();
        let tx_execution_id_1 = TxExecutionId::new(
            block_execution_id.block_number,
            block_execution_id.iteration_id,
            tx_hash_1,
            template.index,
        );

        let tx_env_1 = build_tx_env(&template);

        tracing::debug!(
            "Sending transaction {}/{} (first attempt): {}",
            idx + 1,
            NUM_TRANSACTIONS,
            tx_execution_id_1
        );

        instance
            .transport
            .send_transaction(tx_execution_id_1, tx_env_1)
            .await
            .unwrap();

        // Wait for the transaction to complete
        wait_for_tx(&instance, &tx_execution_id_1).await;

        // Reorg: remove the transaction
        tracing::debug!(
            "Reorging transaction {}/{}: {}",
            idx + 1,
            NUM_TRANSACTIONS,
            tx_execution_id_1
        );

        instance
            .transport
            .reorg(tx_execution_id_1)
            .await
            .expect("Failed to send reorg");

        // Re-submit the transaction with the same nonce but new hash
        let tx_hash_2 = LocalInstance::<T>::generate_random_tx_hash();
        let tx_execution_id_2 = TxExecutionId::new(
            block_execution_id.block_number,
            block_execution_id.iteration_id,
            tx_hash_2,
            template.index,
        );

        let tx_env_2 = build_tx_env(&template);

        tracing::debug!(
            "Sending transaction {}/{} (after reorg): {}",
            idx + 1,
            NUM_TRANSACTIONS,
            tx_execution_id_2
        );

        instance
            .transport
            .send_transaction(tx_execution_id_2, tx_env_2)
            .await
            .unwrap();

        // Wait for the re-submitted transaction to complete
        wait_for_tx(&instance, &tx_execution_id_2).await;
    }
}

fn run_benchmark_for_driver<T, SetupFn, Fut>(
    criterion: &mut Criterion,
    runtime: &Runtime,
    label: &str,
    erc20_bytecode: Bytes,
    setup_fn: SetupFn,
) where
    T: TestTransport + 'static,
    SetupFn: Fn(AssertionStore) -> Fut + Copy + Send + 'static,
    Fut: Future<Output = Result<LocalInstance<T>, String>>,
{
    criterion.bench_function(label, |b| {
        let erc20_bytecode = erc20_bytecode.clone();
        b.iter_batched(
            || runtime.block_on(setup_iteration::<T, _, _>(erc20_bytecode.clone(), setup_fn)),
            |(instance, templates)| {
                std::hint::black_box(
                    runtime.block_on(async move { execute_iteration(instance, templates).await }),
                );
            },
            BatchSize::SmallInput,
        );
    });
}

fn main() {
    let runtime = Runtime::new().unwrap();
    let _profiling_guard: ProfilingGuard = profiling::init_profiling(runtime.handle())
        .expect("Failed to initialize profiling utilities");

    let mut criterion = Criterion::default();
    let erc20_bytecode = read_erc20_bytecode(&erc20_bytecode_path());

    let bench_label = format!("reorg_per_tx");

    run_benchmark_for_driver::<LocalInstanceMockDriver, _, _>(
        &mut criterion,
        &runtime,
        &bench_label,
        erc20_bytecode,
        LocalInstanceMockDriver::new_with_store,
    );

    criterion.final_summary();
}
