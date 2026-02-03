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

#[allow(clippy::too_many_lines)]
fn executor_transaction_performance_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let _enter_guard = runtime.enter();

    let mut group = c.benchmark_group("executor_transaction_performance");

    let single_tx = LoadDefinition {
        tx_amount: 1,
        eoa_percent: 100.0,
        erc20_percent: 0.0,
        uni_percent: 0.0,
        aa_percent: 0.0,
    };

    let mut single_tx_aa = single_tx;
    single_tx_aa.aa_percent = 100.0;

    group.bench_function("eoa_vanilla", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(single_tx),
            |mut package| {
                package.run_vanilla().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("eoa_transaction", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(single_tx),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("eoa_transaction_aa", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(single_tx_aa),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    let single_erc20_tx = LoadDefinition {
        tx_amount: 1,
        eoa_percent: 0.0,
        erc20_percent: 100.0,
        uni_percent: 0.0,
        aa_percent: 0.0,
    };

    let mut single_erc20_tx_aa = single_erc20_tx;
    single_erc20_tx_aa.aa_percent = 100.0;

    group.bench_function("erc20_vanilla", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(single_erc20_tx),
            |mut package| {
                package.run_vanilla().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("erc20_transaction", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(single_erc20_tx),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("erc20_transaction_aa", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(single_erc20_tx_aa),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    let single_uni_tx = LoadDefinition {
        tx_amount: 1,
        eoa_percent: 0.0,
        erc20_percent: 0.0,
        uni_percent: 100.0,
        aa_percent: 0.0,
    };

    let mut single_uni_tx_aa = single_uni_tx;
    single_uni_tx_aa.aa_percent = 100.0;

    group.bench_function("uniswap_vanilla", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(single_uni_tx),
            |mut package| {
                package.run_vanilla().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("uniswap_transaction", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(single_uni_tx),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("uniswap_transaction_aa", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(single_uni_tx_aa),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, executor_transaction_performance_benchmark);
criterion_main!(benches);
