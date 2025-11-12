mod block_validation_fixture;

use block_validation_fixture::{
    BenchFixture,
    BlockValidationScenario,
};
use criterion::{
    Criterion,
    criterion_group,
    criterion_main,
};
use revm::DatabaseCommit;

fn optimistic_vs_iterative(c: &mut Criterion) {
    let default_fixture = BenchFixture::new();
    let success_fixture = BenchFixture::with_scenario(BlockValidationScenario::NoInvalidation);
    let no_assertions_fixture = BenchFixture::with_scenario(BlockValidationScenario::NoAssertions);

    let mut group = c.benchmark_group("block_validation");
    group.bench_function("optimistic_validate_block", |b| {
        b.iter(|| {
            let mut fork_db = default_fixture.fork_db.clone();
            let mut mock_db = default_fixture.mock_db.clone();
            let mut executor = default_fixture.executor.clone();
            let _ = executor
                .validate_block(
                    default_fixture.block_env.clone(),
                    default_fixture.transactions.clone(),
                    &mut fork_db,
                    &mut mock_db,
                )
                .unwrap();
        })
    });

    group.bench_function("optimistic_validate_block_no_invalidation", |b| {
        b.iter(|| {
            let mut fork_db = success_fixture.fork_db.clone();
            let mut mock_db = success_fixture.mock_db.clone();
            let mut executor = success_fixture.executor.clone();
            let _ = executor
                .validate_block(
                    success_fixture.block_env.clone(),
                    success_fixture.transactions.clone(),
                    &mut fork_db,
                    &mut mock_db,
                )
                .unwrap();
        })
    });

    group.bench_function("optimistic_validate_block_no_assertions", |b| {
        b.iter(|| {
            let mut fork_db = no_assertions_fixture.fork_db.clone();
            let mut mock_db = no_assertions_fixture.mock_db.clone();
            let mut executor = no_assertions_fixture.executor.clone();
            let _ = executor
                .validate_block(
                    no_assertions_fixture.block_env.clone(),
                    no_assertions_fixture.transactions.clone(),
                    &mut fork_db,
                    &mut mock_db,
                )
                .unwrap();
        })
    });

    group.bench_function("iterative_validate_tx", |b| {
        b.iter(|| {
            let mut fork_db = default_fixture.fork_db.clone();
            let mut mock_db = default_fixture.mock_db.clone();
            let mut executor = default_fixture.executor.clone();

            for tx in default_fixture.transactions.iter() {
                let result = executor
                    .validate_transaction_ext_db::<_, _>(
                        default_fixture.block_env.clone(),
                        tx.clone(),
                        &mut fork_db,
                        &mut mock_db,
                    )
                    .unwrap();

                if !result.transaction_valid {
                    break;
                }

                mock_db.commit(result.result_and_state.state.clone());
            }
        })
    });

    group.bench_function("iterative_validate_tx_no_invalidation", |b| {
        b.iter(|| {
            let mut fork_db = success_fixture.fork_db.clone();
            let mut mock_db = success_fixture.mock_db.clone();
            let mut executor = success_fixture.executor.clone();

            for tx in success_fixture.transactions.iter() {
                let result = executor
                    .validate_transaction_ext_db::<_, _>(
                        success_fixture.block_env.clone(),
                        tx.clone(),
                        &mut fork_db,
                        &mut mock_db,
                    )
                    .unwrap();

                if !result.transaction_valid {
                    break;
                }

                mock_db.commit(result.result_and_state.state.clone());
            }
        })
    });

    group.bench_function("iterative_validate_tx_no_assertions", |b| {
        b.iter(|| {
            let mut fork_db = no_assertions_fixture.fork_db.clone();
            let mut mock_db = no_assertions_fixture.mock_db.clone();
            let mut executor = no_assertions_fixture.executor.clone();

            for tx in no_assertions_fixture.transactions.iter() {
                let result = executor
                    .validate_transaction_ext_db::<_, _>(
                        no_assertions_fixture.block_env.clone(),
                        tx.clone(),
                        &mut fork_db,
                        &mut mock_db,
                    )
                    .unwrap();

                if !result.transaction_valid {
                    break;
                }

                mock_db.commit(result.result_and_state.state.clone());
            }
        })
    });
    group.finish();
}

criterion_group!(benches, optimistic_vs_iterative);
criterion_main!(benches);
