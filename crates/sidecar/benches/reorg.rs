//! Reorg benchmarks for the sidecar.
//!
//! This module contains benchmarks measuring reorg handling overhead:
//! - `deep_reorg`: Batches of transactions with multi-tx reorgs
//! - `reorg_per_tx`: Single transaction reorgs after each tx

mod bench_utils;

use assertion_executor::{
    primitives::{
        Address,
        Bytes,
        TxEnv,
        U256,
    },
    store::AssertionStore,
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
    future::Future,
    time::Duration,
};
use tokio::runtime::Runtime;

use bench_utils::{
    GAS_LIMIT_PER_TX,
    deploy_erc20,
    encode_erc20_transfer,
    erc20_bytecode_path,
    read_erc20_bytecode,
};

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

fn build_tx_env(caller: Address, nonce: u64, to: Address, data: &Bytes) -> TxEnv {
    TxEnvBuilder::new()
        .caller(caller)
        .gas_limit(GAS_LIMIT_PER_TX)
        .gas_price(0)
        .value(U256::ZERO)
        .nonce(nonce)
        .kind(TxKind::Call(to))
        .data(data.clone())
        .build()
        .expect("Failed to build transaction")
}

// ============================================================================
// Deep reorg benchmark
// ============================================================================

/// Number of batches to run. Each batch is 3 txs with 2 reorged.
const DEEP_REORG_NUM_BATCHES: usize = 10;
/// Transactions per batch
const DEEP_REORG_TXS_PER_BATCH: usize = 3;
/// How many transactions to reorg from the end of each batch
const DEEP_REORG_DEPTH: usize = 2;

/// Setup data for the deep reorg benchmark
struct DeepReorgSetupData {
    erc20_address: Address,
    transfer_data: Bytes,
    caller: Address,
}

async fn setup_deep_reorg<T, F, Fut>(
    erc20_bytecode: Bytes,
    builder: F,
) -> (LocalInstance<T>, DeepReorgSetupData)
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
    let caller = instance.default_account();

    let setup_data = DeepReorgSetupData {
        erc20_address,
        transfer_data,
        caller,
    };

    (instance, setup_data)
}

/// Execute deep reorg benchmark iteration.
/// Pattern: tx1 tx2 tx3 -> reorg(tx2, tx3) -> tx4 tx5 tx6 -> reorg(tx5, tx6) -> ...
async fn execute_deep_reorg<T: TestTransport>(
    mut instance: LocalInstance<T>,
    setup: DeepReorgSetupData,
) {
    let block_execution_id = instance.current_block_execution_id();
    let mut tx_index: u64 = 0;

    for batch_idx in 0..DEEP_REORG_NUM_BATCHES {
        tracing::debug!(
            "Starting batch {}/{}",
            batch_idx + 1,
            DEEP_REORG_NUM_BATCHES
        );

        let mut batch_tx_ids: Vec<TxExecutionId> = Vec::with_capacity(DEEP_REORG_TXS_PER_BATCH);

        for tx_in_batch in 0..DEEP_REORG_TXS_PER_BATCH {
            let nonce = instance.next_nonce(setup.caller, block_execution_id);

            let tx_hash = LocalInstance::<T>::generate_random_tx_hash();
            let tx_execution_id = TxExecutionId::new(
                block_execution_id.block_number,
                block_execution_id.iteration_id,
                tx_hash,
                tx_index,
            );
            tx_index += 1;

            let tx_env = build_tx_env(
                setup.caller,
                nonce,
                setup.erc20_address,
                &setup.transfer_data,
            );

            tracing::debug!(
                "Sending tx {} in batch {} (nonce={}, idx={}): {}",
                tx_in_batch + 1,
                batch_idx + 1,
                nonce,
                tx_index - 1,
                tx_execution_id
            );

            instance
                .transport
                .send_transaction(tx_execution_id, tx_env)
                .await
                .unwrap();

            batch_tx_ids.push(tx_execution_id);
        }

        let last_tx = batch_tx_ids.last().unwrap();
        wait_for_tx(&instance, last_tx).await;

        let reorg_start_idx = DEEP_REORG_TXS_PER_BATCH - DEEP_REORG_DEPTH;
        let tx_hashes: Vec<_> = batch_tx_ids[reorg_start_idx..]
            .iter()
            .map(|id| id.tx_hash)
            .collect();

        let last_tx_to_reorg = *batch_tx_ids.last().unwrap();

        tracing::debug!(
            "Deep reorg batch {}: removing {} txs (hashes: {:?})",
            batch_idx + 1,
            DEEP_REORG_DEPTH,
            tx_hashes
        );

        instance
            .transport
            .reorg_depth(last_tx_to_reorg, tx_hashes)
            .await
            .expect("Failed to send deep reorg");

        let current_nonces = instance.current_nonce();
        let key = (setup.caller, block_execution_id.iteration_id);
        if let Some(&current) = current_nonces.get(&key) {
            instance.set_nonce(
                setup.caller,
                current.saturating_sub(DEEP_REORG_DEPTH as u64),
                block_execution_id,
            );
        }

        tx_index -= DEEP_REORG_DEPTH as u64;
    }
}

// ============================================================================
// Reorg per tx benchmark
// ============================================================================

const REORG_PER_TX_NUM_TRANSACTIONS: usize = 20;

/// A transaction template that can be re-submitted after reorg.
struct TxTemplate {
    caller: Address,
    nonce: u64,
    to: Address,
    data: Bytes,
    index: u64,
}

async fn setup_reorg_per_tx<T, F, Fut>(
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

    let mut templates = Vec::with_capacity(REORG_PER_TX_NUM_TRANSACTIONS);
    let block_execution_id = instance.current_block_execution_id();

    for idx in 0..REORG_PER_TX_NUM_TRANSACTIONS {
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

fn build_tx_env_from_template(template: &TxTemplate) -> TxEnv {
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

/// Execute reorg per tx benchmark iteration.
/// Pattern: tx1 -> reorg tx1 -> tx1 -> tx2 -> reorg tx2 -> tx2 -> ...
async fn execute_reorg_per_tx<T: TestTransport>(
    mut instance: LocalInstance<T>,
    templates: Vec<TxTemplate>,
) {
    let block_execution_id = instance.current_block_execution_id();

    for (idx, template) in templates.into_iter().enumerate() {
        let tx_hash_1 = LocalInstance::<T>::generate_random_tx_hash();
        let tx_execution_id_1 = TxExecutionId::new(
            block_execution_id.block_number,
            block_execution_id.iteration_id,
            tx_hash_1,
            template.index,
        );

        let tx_env_1 = build_tx_env_from_template(&template);

        tracing::debug!(
            "Sending transaction {}/{} (first attempt): {}",
            idx + 1,
            REORG_PER_TX_NUM_TRANSACTIONS,
            tx_execution_id_1
        );

        instance
            .transport
            .send_transaction(tx_execution_id_1, tx_env_1)
            .await
            .unwrap();

        wait_for_tx(&instance, &tx_execution_id_1).await;

        tracing::debug!(
            "Reorging transaction {}/{}: {}",
            idx + 1,
            REORG_PER_TX_NUM_TRANSACTIONS,
            tx_execution_id_1
        );

        instance
            .transport
            .reorg(tx_execution_id_1)
            .await
            .expect("Failed to send reorg");

        let tx_hash_2 = LocalInstance::<T>::generate_random_tx_hash();
        let tx_execution_id_2 = TxExecutionId::new(
            block_execution_id.block_number,
            block_execution_id.iteration_id,
            tx_hash_2,
            template.index,
        );

        let tx_env_2 = build_tx_env_from_template(&template);

        tracing::debug!(
            "Sending transaction {}/{} (after reorg): {}",
            idx + 1,
            REORG_PER_TX_NUM_TRANSACTIONS,
            tx_execution_id_2
        );

        instance
            .transport
            .send_transaction(tx_execution_id_2, tx_env_2)
            .await
            .unwrap();

        wait_for_tx(&instance, &tx_execution_id_2).await;
    }
}

// ============================================================================
// Benchmark registration
// ============================================================================

fn reorg_benchmarks(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let _profiling_guard: ProfilingGuard = profiling::init_profiling(runtime.handle())
        .expect("Failed to initialize profiling utilities");

    let erc20_bytecode = read_erc20_bytecode(&erc20_bytecode_path());

    let mut group = c.benchmark_group("reorg");

    // Deep reorg benchmark
    group.bench_function("deep_reorg", |b| {
        let erc20_bytecode = erc20_bytecode.clone();
        b.iter_batched(
            || {
                runtime.block_on(setup_deep_reorg::<LocalInstanceMockDriver, _, _>(
                    erc20_bytecode.clone(),
                    LocalInstanceMockDriver::new_with_store,
                ))
            },
            |(instance, setup)| {
                std::hint::black_box(
                    runtime.block_on(async move { execute_deep_reorg(instance, setup).await }),
                );
            },
            BatchSize::SmallInput,
        );
    });

    // Reorg per tx benchmark
    group.bench_function("reorg_per_tx", |b| {
        let erc20_bytecode = erc20_bytecode.clone();
        b.iter_batched(
            || {
                runtime.block_on(setup_reorg_per_tx::<LocalInstanceMockDriver, _, _>(
                    erc20_bytecode.clone(),
                    LocalInstanceMockDriver::new_with_store,
                ))
            },
            |(instance, templates)| {
                std::hint::black_box(
                    runtime
                        .block_on(async move { execute_reorg_per_tx(instance, templates).await }),
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, reorg_benchmarks);
criterion_main!(benches);
