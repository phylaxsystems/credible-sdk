#![allow(clippy::too_many_lines)]

use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    constants::{
        ASSERTION_CONTRACT,
        CALLER,
        PRECOMPILE_ADDRESS,
    },
    db::{
        fork_db::ForkDb,
        overlay::{
            OverlayDb,
            test_utils::MockDb,
        },
    },
    primitives::{
        AccountInfo,
        Address,
        BlockEnv,
        Bytecode,
        Bytes,
        TxEnv,
        TxKind,
        U256,
        address,
        hex,
        keccak256,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
};
use criterion::{
    Criterion,
    criterion_group,
    criterion_main,
};
use revm::{
    DatabaseCommit,
    DatabaseRef,
    database::{
        CacheDB,
        EmptyDBTyped,
    },
};
use serde_json::Value;
use std::{
    convert::Infallible,
    fs::File,
    path::Path,
    sync::Arc,
};

const BENCH_TRIGGER_ADDRESS: Address = address!("00000000000000000000000000000000000000F0");
const BLOCK_TX_COUNT: usize = 200;
const SET_FAIL_INDEX: usize = 98;
const FAIL_INDEX: usize = 99;
const ARTIFACT_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../testdata/mock-protocol/out"
);
const SIDECAR_BENCH_ASSERTIONS_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../crates/sidecar/benches/assertions.json"
);
const HEAVY_ASSERTIONS_PER_TX: usize = 16;
const CALLER_TRANSACTION_START_NONCE: u64 = 42;
const BLOCK_BASEFEE: u64 = 10;

type TestDB = OverlayDb<CacheDB<EmptyDBTyped<Infallible>>>;
type TestForkDB = ForkDb<TestDB>;

#[derive(Clone, Copy)]
enum BlockValidationScenario {
    Default,
    NoInvalidation,
    NoAssertions,
}

struct BenchFixture {
    executor: AssertionExecutor,
    fork_db: TestForkDB,
    mock_db: MockDb,
    block_env: BlockEnv,
    transactions: Vec<TxEnv>,
}

fn bench_artifact_bytes(input: &str, section: &str) -> Bytes {
    let mut parts = input.split(':');
    let file = parts.next().unwrap();
    let contract = parts.next().unwrap();
    let path = Path::new(ARTIFACT_ROOT).join(format!("{file}/{contract}.json"));
    let file = File::open(&path)
        .unwrap_or_else(|err| panic!("failed to open `{}`: {err}", path.display()));
    let artifact: Value = serde_json::from_reader(file).expect("failed to parse artifact");
    let bytecode = artifact[section]["object"]
        .as_str()
        .expect("failed to read bytecode");
    let raw = hex::decode(bytecode).expect("failed to decode bytecode");
    raw.into()
}

fn bench_bytecode(input: &str) -> Bytes {
    bench_artifact_bytes(input, "deployedBytecode")
}

fn bench_creation_bytecode(input: &str) -> Bytes {
    bench_artifact_bytes(input, "bytecode")
}

fn load_heavy_assertion_bytecode(count: usize) -> Vec<Bytes> {
    let path = Path::new(SIDECAR_BENCH_ASSERTIONS_PATH);
    let file = File::open(path).unwrap_or_else(|err| {
        panic!(
            "failed to open heavy assertion file `{}`: {err}",
            SIDECAR_BENCH_ASSERTIONS_PATH,
        )
    });
    let bytecodes: Vec<String> =
        serde_json::from_reader(file).expect("failed to parse heavy assertions json");
    bytecodes
        .into_iter()
        .take(count)
        .map(|bytecode| {
            Bytes::from(hex::decode(bytecode).expect("failed to decode heavy assertion bytecode"))
        })
        .collect()
}

fn selector(signature: &str) -> [u8; 4] {
    let hash = keccak256(signature);
    let mut selector = [0u8; 4];
    selector.copy_from_slice(&hash[0..4]);
    selector
}

fn bool_arg(value: bool) -> [u8; 32] {
    let mut arg = [0u8; 32];
    if value {
        arg[31] = 1;
    }
    arg
}

impl BenchFixture {
    fn new() -> Self {
        Self::with_scenario(BlockValidationScenario::Default)
    }

    fn with_scenario(scenario: BlockValidationScenario) -> Self {
        let bench_trigger_bytes = bench_bytecode("BenchFailAssertion.sol:BenchTrigger");
        let bench_assertion_bytes =
            bench_creation_bytecode("BenchFailAssertion.sol:BenchFailAssertion");

        let bench_trigger_account = AccountInfo {
            nonce: 1,
            balance: U256::MAX,
            code_hash: keccak256(&bench_trigger_bytes),
            code: Some(Bytecode::new_legacy(bench_trigger_bytes.clone())),
            ..Default::default()
        };

        let inner_db = CacheDB::default();
        let test_db: TestDB = OverlayDb::new(Some(Arc::new(inner_db)));
        let mut fork_db: TestForkDB = test_db.fork();
        fork_db.insert_account_info(BENCH_TRIGGER_ADDRESS, bench_trigger_account.clone());

        let mut mock_db = MockDb::new();
        mock_db.insert_account(BENCH_TRIGGER_ADDRESS, bench_trigger_account);

        let executor = AssertionExecutor::new(
            ExecutorConfig::default(),
            AssertionStore::new_ephemeral().unwrap(),
        );

        let bench_assertion_state = AssertionState::new_test(&bench_assertion_bytes);
        let bench_assertion_contract = bench_assertion_state.assertion_contract.clone();
        executor
            .store
            .insert(BENCH_TRIGGER_ADDRESS, bench_assertion_state)
            .unwrap();
        for heavy_bytes in load_heavy_assertion_bytecode(HEAVY_ASSERTIONS_PER_TX) {
            let heavy_state = AssertionState::new_test(&heavy_bytes);
            executor
                .store
                .insert(BENCH_TRIGGER_ADDRESS, heavy_state)
                .unwrap();
        }
        executor.insert_persistent_accounts(&bench_assertion_contract, &mut fork_db);

        let assertion_account = fork_db
            .basic_ref(ASSERTION_CONTRACT)
            .unwrap()
            .unwrap()
            .clone();
        mock_db.insert_account(ASSERTION_CONTRACT, assertion_account);
        let caller_account = fork_db.basic_ref(CALLER).unwrap().unwrap().clone();
        mock_db.insert_account(CALLER, caller_account);
        let precompile_account = fork_db
            .basic_ref(PRECOMPILE_ADDRESS)
            .unwrap()
            .unwrap()
            .clone();
        mock_db.insert_account(PRECOMPILE_ADDRESS, precompile_account);

        let block_env = BlockEnv {
            number: 1234,
            basefee: BLOCK_BASEFEE,
            ..Default::default()
        };

        let transactions = build_block_transactions(block_env.basefee, scenario);

        Self {
            executor,
            fork_db,
            mock_db,
            block_env,
            transactions,
        }
    }
}

fn build_block_transactions(basefee: u64, scenario: BlockValidationScenario) -> Vec<TxEnv> {
    let trigger_selector = selector("trigger()");
    let noop_selector = selector("noop()");
    let set_fail_selector = selector("setFail(bool)");
    let should_fail = matches!(scenario, BlockValidationScenario::Default);

    (0..BLOCK_TX_COUNT)
        .map(|index| {
            let mut tx = TxEnv {
                kind: TxKind::Call(BENCH_TRIGGER_ADDRESS),
                caller: CALLER,
                gas_price: basefee.into(),
                gas_limit: 1_000_000,
                nonce: CALLER_TRANSACTION_START_NONCE + index as u64,
                ..Default::default()
            };

            match scenario {
                BlockValidationScenario::NoAssertions => {
                    let mut data = Vec::with_capacity(4);
                    data.extend_from_slice(&noop_selector);
                    tx.data = data.into();
                }
                _ => {
                    match index {
                        SET_FAIL_INDEX => {
                            tx.kind = TxKind::Call(ASSERTION_CONTRACT);
                            let mut data = Vec::with_capacity(4 + 32);
                            data.extend_from_slice(&set_fail_selector);
                            data.extend_from_slice(&bool_arg(should_fail));
                            tx.data = data.into();
                        }
                        FAIL_INDEX => {
                            let mut data = Vec::with_capacity(4);
                            data.extend_from_slice(&trigger_selector);
                            tx.data = data.into();
                        }
                        _ => {
                            let mut data = Vec::with_capacity(4);
                            data.extend_from_slice(&noop_selector);
                            tx.data = data.into();
                        }
                    }
                }
            }
            tx
        })
        .collect()
}

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
