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

    let avg_block_no_aa = LoadDefinition::default();

    let avg_block_3_aa = LoadDefinition {
        aa_percent: 3.0,
        ..LoadDefinition::default()
    };

    let avg_block_100_aa = LoadDefinition {
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
