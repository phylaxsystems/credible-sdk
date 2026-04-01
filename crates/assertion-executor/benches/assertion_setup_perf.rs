use benchmark_utils::{
    BenchmarkPreset,
    synthetic_assertion_setup_case,
};
use criterion::{
    BatchSize,
    Criterion,
    criterion_group,
    criterion_main,
};
use std::hint::black_box;
use tokio::runtime::Runtime;

const MANY_SELECTORS: usize = 16;
const MANY_CONTRACTS: usize = 16;

fn assertion_setup_performance_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let _enter_guard = runtime.enter();

    // These cases separate selector fanout from contract fanout, and "cold" vs "warm"
    // presets let us compare setup cost when the traced tx produced very little vs a
    // heavily matched assertion workload.
    let mut group = c.benchmark_group("assertion_setup_performance");

    group.bench_function("cold_one_contract_many_selectors", |b| {
        b.iter_batched(
            || {
                synthetic_assertion_setup_case(
                    BenchmarkPreset::Erc20TransactionAa,
                    1,
                    MANY_SELECTORS,
                )
                .expect("prepare cold one-contract setup case")
            },
            |case| black_box(case.run()),
            BatchSize::LargeInput,
        );
    });

    group.bench_function("cold_many_contracts_one_selector", |b| {
        b.iter_batched(
            || {
                synthetic_assertion_setup_case(
                    BenchmarkPreset::Erc20TransactionAa,
                    MANY_CONTRACTS,
                    1,
                )
                .expect("prepare cold many-contract setup case")
            },
            |case| black_box(case.run()),
            BatchSize::LargeInput,
        );
    });

    group.bench_function("warm_one_contract_many_selectors", |b| {
        b.iter_batched(
            || {
                synthetic_assertion_setup_case(BenchmarkPreset::AvgBlock100Aa, 1, MANY_SELECTORS)
                    .expect("prepare warm one-contract setup case")
            },
            |case| black_box(case.run()),
            BatchSize::LargeInput,
        );
    });

    group.bench_function("warm_many_contracts_one_selector", |b| {
        b.iter_batched(
            || {
                synthetic_assertion_setup_case(BenchmarkPreset::AvgBlock100Aa, MANY_CONTRACTS, 1)
                    .expect("prepare warm many-contract setup case")
            },
            |case| black_box(case.run()),
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, assertion_setup_performance_benchmark);
criterion_main!(benches);
