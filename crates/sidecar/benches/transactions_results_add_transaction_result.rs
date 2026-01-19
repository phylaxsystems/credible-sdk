//! Targeted benchmark for `TransactionsResults::add_transaction_result`.

use alloy::primitives::U256;
use assertion_executor::primitives::ExecutionResult;
use criterion::{
    Criterion,
    criterion_group,
    criterion_main,
};
use revm::{
    context::result::{
        Output,
        SuccessReason,
    },
    primitives::{
        Bytes,
        alloy_primitives::TxHash,
    },
};
use sidecar::{
    TransactionsState,
    engine::{
        TransactionResult,
        TransactionsResults,
    },
    execution_ids::TxExecutionId,
};

const MAX_CAPACITY: usize = 1024;
const TX_ID_POOL_SIZE: usize = MAX_CAPACITY + 16_384;

fn dummy_result() -> TransactionResult {
    TransactionResult::ValidationCompleted {
        execution_result: ExecutionResult::Success {
            reason: SuccessReason::Stop,
            gas_used: 21_000,
            gas_refunded: 0,
            logs: Vec::new(),
            output: Output::Call(Bytes::new()),
        },
        is_valid: true,
    }
}

fn make_tx_execution_id(i: u64) -> TxExecutionId {
    let byte = u8::try_from(i % 256).expect("i % 256 always fits into u8");
    TxExecutionId::new(U256::from(i), 0, TxHash::from([byte; 32]), i)
}

fn build_tx_ids() -> Vec<TxExecutionId> {
    (0..(TX_ID_POOL_SIZE as u64))
        .map(make_tx_execution_id)
        .collect()
}

fn transactions_results_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactions_results");

    let tx_ids = build_tx_ids();
    let dummy = dummy_result();

    // Benchmark: add_transaction_result_prune
    {
        let transactions_state = TransactionsState::new();
        let mut results = TransactionsResults::new(transactions_state, MAX_CAPACITY);

        for tx_execution_id in tx_ids.iter().take(MAX_CAPACITY).copied() {
            results.add_transaction_result(tx_execution_id, &dummy);
        }

        group.bench_function("add_transaction_result_prune", |b| {
            let mut idx = MAX_CAPACITY;
            b.iter(|| {
                let tx_execution_id = tx_ids[idx];
                idx += 1;
                if idx >= tx_ids.len() {
                    idx = MAX_CAPACITY;
                }
                results.add_transaction_result(tx_execution_id, std::hint::black_box(&dummy));
            });
        });
    }

    // Benchmark: add_transaction_result_update_existing
    {
        let transactions_state = TransactionsState::new();
        let mut results = TransactionsResults::new(transactions_state, MAX_CAPACITY);

        for tx_execution_id in tx_ids.iter().take(MAX_CAPACITY).copied() {
            results.add_transaction_result(tx_execution_id, &dummy);
        }

        group.bench_function("add_transaction_result_update_existing", |b| {
            let mut idx = 0usize;
            b.iter(|| {
                let tx_execution_id = tx_ids[idx];
                idx += 1;
                if idx >= MAX_CAPACITY {
                    idx = 0;
                }
                results.add_transaction_result(tx_execution_id, std::hint::black_box(&dummy));
            });
        });
    }

    group.finish();
}

criterion_group!(benches, transactions_results_benchmarks);
criterion_main!(benches);
