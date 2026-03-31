use benchmark_utils::{
    BenchmarkPackage,
    BenchmarkPreset,
};
use criterion::{
    BatchSize,
    Criterion,
    criterion_group,
    criterion_main,
};
use std::hint::black_box;
use tokio::runtime::Runtime;

fn executor_phase_performance_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let _enter_guard = runtime.enter();

    let load = BenchmarkPreset::AvgBlock100Aa.load_definition();
    // Keep the phase benches on one representative AA-heavy preset so before/after
    // diffs answer "which stage changed?" without input-shape noise.
    let mut group = c.benchmark_group("executor_phase_performance");

    group.bench_function("avg_block_100_aa_trace_only", |b| {
        b.iter_batched(
            || BenchmarkPackage::new(load),
            |mut package| {
                black_box(
                    package
                        .run_trace_only()
                        .expect("trace-only benchmark run failed"),
                );
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("avg_block_100_aa_store_read_only", |b| {
        b.iter_batched(
            || {
                BenchmarkPackage::new(load)
                    .prepare_store_read()
                    .expect("prepare store-read benchmark package")
            },
            |package| {
                black_box(package.run().expect("store-read benchmark run failed"));
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("avg_block_100_aa_assertion_setup_only", |b| {
        b.iter_batched(
            || {
                BenchmarkPackage::new(load)
                    .prepare_assertion_phases()
                    .expect("prepare assertion benchmark package")
            },
            |package| {
                black_box(
                    package
                        .run_assertion_setup_only()
                        .expect("assertion-setup benchmark run failed"),
                );
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("avg_block_100_aa_assertions_only", |b| {
        b.iter_batched(
            || {
                BenchmarkPackage::new(load)
                    .prepare_assertion_phases()
                    .expect("prepare assertion benchmark package")
            },
            |package| {
                black_box(
                    package
                        .run_assertions_only()
                        .expect("assertions-only benchmark run failed"),
                );
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, executor_phase_performance_benchmark);
criterion_main!(benches);
