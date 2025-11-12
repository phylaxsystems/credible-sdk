#[path = "../benches/block_validation_fixture.rs"]
mod block_validation_fixture;

use block_validation_fixture::{
    trigger_tx,
    BenchFixture,
    BlockValidationScenario,
    CALLER_TRANSACTION_START_NONCE,
};

#[test]
fn trigger_transaction_runs_assertions() {
    let fixture = BenchFixture::new();

    let mut executor = fixture.executor.clone();
    let mut fork_db = fixture.fork_db.clone();
    let mut mock_db = fixture.mock_db.clone();

    let trigger_tx = trigger_tx(fixture.block_env.basefee, CALLER_TRANSACTION_START_NONCE);
    let result = executor
        .validate_transaction_ext_db::<_, _>(
            fixture.block_env.clone(),
            trigger_tx,
            &mut fork_db,
            &mut mock_db,
        )
        .unwrap();

    assert!(
        !result.assertions_executions.is_empty(),
        "trigger() tx should run assertions"
    );
    assert!(
        result
            .assertions_executions
            .iter()
            .any(|exec| exec.total_assertion_funcs_ran > 0),
        "trigger() tx should execute at least one assertion function"
    );
}

#[test]
fn no_assertions_scenario_skips_assertions_entirely() {
    let fixture = BenchFixture::with_scenario(BlockValidationScenario::NoAssertions);
    let mut executor = fixture.executor.clone();
    let mut fork_db = fixture.fork_db.clone();
    let mut mock_db = fixture.mock_db.clone();

    let trigger_tx = trigger_tx(fixture.block_env.basefee, CALLER_TRANSACTION_START_NONCE);
    let result = executor
        .validate_transaction_ext_db::<_, _>(
            fixture.block_env.clone(),
            trigger_tx,
            &mut fork_db,
            &mut mock_db,
        )
        .unwrap();

    assert!(
        result.assertions_executions.is_empty(),
        "When assertions are disabled, no selectors should match even if trigger() is called"
    );
}
