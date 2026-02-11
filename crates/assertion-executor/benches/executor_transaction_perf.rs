use benchmark_utils::{
    BenchmarkPackage,
    LoadDefinition,
};
use criterion::{
    BatchSize,
    BenchmarkGroup,
    Criterion,
    criterion_group,
    criterion_main,
    measurement::WallTime,
};
use tokio::runtime::Runtime;

fn bench_load_variants(group: &mut BenchmarkGroup<WallTime>, name: &str, load: LoadDefinition) {
    group.bench_function(format!("{name}_vanilla"), |b| {
        b.iter_batched(
            || BenchmarkPackage::new(load),
            |mut package| {
                package.run_vanilla().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function(format!("{name}_transaction"), |b| {
        b.iter_batched(
            || BenchmarkPackage::new(load),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });

    let mut load_aa = load;
    load_aa.aa_percent = 100.0;
    group.bench_function(format!("{name}_transaction_aa"), |b| {
        b.iter_batched(
            || BenchmarkPackage::new(load_aa),
            |mut package| {
                package.run().expect("benchmark run failed");
            },
            BatchSize::SmallInput,
        );
    });
}

fn executor_transaction_performance_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let _enter_guard = runtime.enter();

    let mut group = c.benchmark_group("executor_transaction_performance");

    let eoa_load = LoadDefinition {
        tx_amount: 1,
        eoa_percent: 100.0,
        erc20_percent: 0.0,
        uni_percent: 0.0,
        aa_percent: 0.0,
    };

    bench_load_variants(&mut group, "eoa", eoa_load);

    let erc20_load = LoadDefinition {
        tx_amount: 1,
        eoa_percent: 0.0,
        erc20_percent: 100.0,
        uni_percent: 0.0,
        aa_percent: 0.0,
    };

    bench_load_variants(&mut group, "erc20", erc20_load);

    let uniswap_load = LoadDefinition {
        tx_amount: 1,
        eoa_percent: 0.0,
        erc20_percent: 0.0,
        uni_percent: 100.0,
        aa_percent: 0.0,
    };

    bench_load_variants(&mut group, "uniswap", uniswap_load);

    group.finish();
}

criterion_group!(benches, executor_transaction_performance_benchmark);
criterion_main!(benches);
