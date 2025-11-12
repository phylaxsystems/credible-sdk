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
use revm::{
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

pub const BENCH_TRIGGER_ADDRESS: Address = address!("00000000000000000000000000000000000000F0");
pub const BLOCK_TX_COUNT: usize = 200;
pub const SET_FAIL_INDEX: usize = 98;
pub const FAIL_INDEX: usize = 99;
pub const ARTIFACT_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../testdata/mock-protocol/out"
);
pub const SIDECAR_BENCH_ASSERTIONS_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../crates/sidecar/benches/assertions.json"
);
pub const HEAVY_ASSERTIONS_PER_TX: usize = 16;
pub const CALLER_TRANSACTION_START_NONCE: u64 = 42;
pub const BLOCK_BASEFEE: u64 = 10;

pub type TestDB = OverlayDb<CacheDB<EmptyDBTyped<Infallible>>>;
pub type TestForkDB = ForkDb<TestDB>;

#[derive(Clone, Copy)]
#[allow(dead_code)]
pub enum BlockValidationScenario {
    Default,
    NoInvalidation,
    NoAssertions,
}

#[allow(dead_code)]
pub struct BenchFixture {
    pub executor: AssertionExecutor,
    pub fork_db: TestForkDB,
    pub mock_db: MockDb,
    pub block_env: BlockEnv,
    pub transactions: Vec<TxEnv>,
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
    pub fn new() -> Self {
        Self::with_scenario(BlockValidationScenario::Default)
    }

    pub fn with_scenario(scenario: BlockValidationScenario) -> Self {
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

        if !matches!(scenario, BlockValidationScenario::NoAssertions) {
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

pub fn trigger_tx(basefee: u64, nonce: u64) -> TxEnv {
    let mut data = Vec::with_capacity(4);
    data.extend_from_slice(&selector("trigger()"));
    TxEnv {
        kind: TxKind::Call(BENCH_TRIGGER_ADDRESS),
        caller: CALLER,
        gas_price: basefee.into(),
        gas_limit: 1_000_000,
        nonce,
        data: data.into(),
        ..Default::default()
    }
}

fn noop_tx(basefee: u64, nonce: u64) -> TxEnv {
    let mut data = Vec::with_capacity(4);
    data.extend_from_slice(&selector("noop()"));
    TxEnv {
        kind: TxKind::Call(BENCH_TRIGGER_ADDRESS),
        caller: CALLER,
        gas_price: basefee.into(),
        gas_limit: 1_000_000,
        nonce,
        data: data.into(),
        ..Default::default()
    }
}

fn set_fail_tx(basefee: u64, nonce: u64, should_fail: bool) -> TxEnv {
    let mut data = Vec::with_capacity(4 + 32);
    data.extend_from_slice(&selector("setFail(bool)"));
    data.extend_from_slice(&bool_arg(should_fail));
    TxEnv {
        kind: TxKind::Call(ASSERTION_CONTRACT),
        caller: CALLER,
        gas_price: basefee.into(),
        gas_limit: 1_000_000,
        nonce,
        data: data.into(),
        ..Default::default()
    }
}

pub fn build_block_transactions(basefee: u64, scenario: BlockValidationScenario) -> Vec<TxEnv> {
    let should_fail = matches!(scenario, BlockValidationScenario::Default);

    (0..BLOCK_TX_COUNT)
        .map(|index| {
            let nonce = CALLER_TRANSACTION_START_NONCE + index as u64;
            match scenario {
                BlockValidationScenario::NoAssertions => noop_tx(basefee, nonce),
                _ => {
                    match index {
                        SET_FAIL_INDEX => set_fail_tx(basefee, nonce, should_fail),
                        FAIL_INDEX => trigger_tx(basefee, nonce),
                        _ => noop_tx(basefee, nonce),
                    }
                }
            }
        })
        .collect()
}
