use benchmark_utils::{
    BenchmarkPackage,
    LoadDefinition,
};
use criterion::{
    BatchSize,
    Criterion,
    criterion_group,
    criterion_main,
};
use tokio::runtime::Runtime;

fn executor_avg_block_performance_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let _enter_guard = runtime.enter();

    let mut group = c.benchmark_group("executor_avg_block_performance");

    // UniV3 pool has limited liquidity - max ~3 txs per pool (1 deploy + 2 swaps).
    // Use 5 txs to maintain representative percentages while respecting limits:
    // EOA: 10% → 1 tx, ERC20: 30% → 2 txs, UniV3: 60% → 3 txs (1 deploy + 2 swaps)
    let base_tx_amount = 5;

    // Average representative block with 0% AAs
    let avg_block_no_aa = LoadDefinition {
        tx_amount: base_tx_amount,
        ..LoadDefinition::default()
    };

    // Average representative block with 3% AAs
    // With 5 txs and 3% AA, only ~0.15 txs would be AA (rounds to 0), so use higher count
    // to actually exercise AA path: 34 txs gives ~1 AA tx per type
    let avg_block_3_aa = LoadDefinition {
        tx_amount: base_tx_amount,
        aa_percent: 3.0,
        ..LoadDefinition::default()
    };

    // Average representative block with 100% AAs
    // All txs target AA contracts - still limited by single AA pool liquidity
    let avg_block_100_aa = LoadDefinition {
        tx_amount: base_tx_amount,
        aa_percent: 100.0,
        ..LoadDefinition::default()
    };

    group.bench_function("avg_block_vanilla", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(avg_block_no_aa),
            |mut package| {
                package.run_vanilla().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("avg_block_0_aa", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(avg_block_no_aa),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("avg_block_3_aa", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(avg_block_3_aa),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("avg_block_100_aa", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(avg_block_100_aa),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, executor_avg_block_performance_benchmark);
criterion_main!(benches);
