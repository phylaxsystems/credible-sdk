use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::{
        DatabaseCommit,
        overlay::OverlayDb,
    },
    primitives::{
        Account,
        AccountInfo,
        AccountStatus,
        Address,
        BlockEnv,
        EvmState,
        TxEnv,
        U256,
    },
    store::{
        AssertionState,
        AssertionStore,
        PruneConfig,
    },
    test_utils::{
        COUNTER_ADDRESS,
        SIMPLE_ASSERTION_COUNTER,
        bytecode,
        counter_acct_info,
        counter_call,
    },
};
use criterion::{
    Criterion,
    criterion_group,
    criterion_main,
};
use revm::database::InMemoryDB;
use tokio::runtime::Runtime;

fn build_fork_db() -> (
    assertion_executor::db::fork_db::ForkDb<OverlayDb<InMemoryDB>>,
    TxEnv,
    BlockEnv,
) {
    let basefee = 10;
    let block_env = BlockEnv {
        number: U256::from(1),
        basefee,
        ..Default::default()
    };

    let mut tx_env = TxEnv {
        gas_price: basefee.into(),
        gas_limit: 10_000_000,
        ..counter_call()
    };
    tx_env.caller = Address::from([0x11u8; 20]);
    tx_env.nonce = 0;

    let db: OverlayDb<InMemoryDB> = OverlayDb::<InMemoryDB>::new_test();
    let mut fork_db = db.fork();

    let mut initial_state = EvmState::default();
    initial_state.insert(
        COUNTER_ADDRESS,
        Account {
            info: counter_acct_info(),
            transaction_id: 0,
            storage: std::collections::HashMap::default(),
            status: AccountStatus::Touched,
        },
    );
    initial_state.insert(
        tx_env.caller,
        Account {
            info: AccountInfo {
                balance: U256::MAX,
                nonce: 0,
                ..Default::default()
            },
            transaction_id: 0,
            storage: std::collections::HashMap::default(),
            status: AccountStatus::Touched,
        },
    );
    fork_db.commit(initial_state);

    (fork_db, tx_env, block_env)
}

fn store_no_prune() -> AssertionStore {
    AssertionStore::new_ephemeral_with_config(PruneConfig {
        interval_ms: 60_000_000,
        retention_blocks: u64::MAX,
    })
    .expect("create assertion store")
}

fn bench_validate_transaction(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create tokio runtime");
    let _enter_guard = runtime.enter();

    let mut group = c.benchmark_group("executor::validate_transaction");

    group.bench_function("no_assertions", |b| {
        let assertion_store = store_no_prune();
        let mut executor = AssertionExecutor::new(ExecutorConfig::default(), assertion_store);

        let (mut fork_db, tx_env, block_env) = build_fork_db();

        b.iter(|| {
            let result = executor
                .validate_transaction(
                    std::hint::black_box(block_env.clone()),
                    std::hint::black_box(&tx_env),
                    std::hint::black_box(&mut fork_db),
                    false,
                )
                .expect("validate_transaction");
            std::hint::black_box(result);
        });
    });

    group.bench_function("single_assertion", |b| {
        let assertion_store = store_no_prune();
        let assertion_bytecode = bytecode(SIMPLE_ASSERTION_COUNTER);
        assertion_store
            .insert(
                COUNTER_ADDRESS,
                AssertionState::new_active(&assertion_bytecode, &ExecutorConfig::default())
                    .expect("build assertion state"),
            )
            .expect("insert assertion");
        let mut executor = AssertionExecutor::new(ExecutorConfig::default(), assertion_store);

        let (mut fork_db, tx_env, block_env) = build_fork_db();

        b.iter(|| {
            let result = executor
                .validate_transaction(
                    std::hint::black_box(block_env.clone()),
                    std::hint::black_box(&tx_env),
                    std::hint::black_box(&mut fork_db),
                    false,
                )
                .expect("validate_transaction");
            std::hint::black_box(result);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_validate_transaction);
criterion_main!(benches);
