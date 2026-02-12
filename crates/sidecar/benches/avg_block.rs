//! `avg_block` benchmark.
//! Send 100 ERC20 transfers inside a single block. Measures baseline performance
//! of the sidecar with no assertions.
//!
//! As a part of the setup, deploys an ERC20 contract, and mints `U256::MAX` to the
//! default instance account. The transactions then send `u32::MAX` tokens to the
//! `0x000...` burn address.

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

// Setup function: creates instance and builds transactions (not measured)
async fn setup_iteration<T, F, Fut>(
    erc20_bytecode: Bytes,
    builder: F,
) -> (LocalInstance<T>, Vec<(TxExecutionId, TxEnv)>)
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

    // build 100 ERC20 transfers targeting the same contract
    let mut transactions = Vec::with_capacity(100);
    let block_execution_id = instance.current_block_execution_id();

    for idx in 0..100 {
        let nonce = instance.next_nonce(instance.default_account(), block_execution_id);
        let tx_env = TxEnvBuilder::new()
            .caller(instance.default_account())
            .gas_limit(GAS_LIMIT_PER_TX)
            .gas_price(0)
            .value(U256::ZERO)
            .nonce(nonce)
            .kind(TxKind::Call(erc20_address))
            .data(transfer_data.clone())
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
            |(instance, transactions)| {
                std::hint::black_box(
                    runtime
                        .block_on(async move { execute_iteration(instance, transactions).await }),
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

    let bench_label = format!("avg_block");

    run_benchmark_for_driver::<LocalInstanceMockDriver, _, _>(
        &mut criterion,
        &runtime,
        &bench_label,
        erc20_bytecode,
        LocalInstanceMockDriver::new_with_store,
    );

    criterion.final_summary();
}
