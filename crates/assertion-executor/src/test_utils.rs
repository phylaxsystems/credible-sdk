#![cfg(any(test, feature = "test"))]

use crate::{
    db::{
        overlay::OverlayDb,
        DatabaseCommit,
    },
    inspectors::TriggerRecorder,
    primitives::{
        address,
        fixed_bytes,
        hex,
        keccak256,
        AccountInfo,
        Address,
        AssertionContract,
        BlockEnv,
        Bytecode,
        Bytes,
        FixedBytes,
        TxEnv,
        TxKind,
        TxValidationResult,
        U256,
    },
    store::{
        extract_assertion_contract,
        AssertionState,
        AssertionStore,
    },
    ExecutorConfig,
};
use revm::db::CacheDB;
use revm::db::EmptyDBTyped;
use std::convert::Infallible;

use alloy_rpc_types::{
    BlockId,
    BlockTransactionsKind,
    Header,
};

use alloy_node_bindings::{
    Anvil,
    AnvilInstance,
};

use alloy_provider::{
    ext::AnvilApi,
    Provider,
    ProviderBuilder,
    RootProvider,
};

#[cfg(feature = "optimism")]
use crate::executor::config::create_optimism_fields;

use alloy_transport_ws::WsConnect;

/// Deployed bytecode of contract-mocks/src/SimpleCounterAssertion.sol:Counter
pub const COUNTER: &str = "SimpleCounterAssertion.sol:Counter";
pub const COUNTER_ADDRESS: Address = Address::new([1u8; 20]);

pub fn counter_call() -> TxEnv {
    TxEnv {
        transact_to: TxKind::Call(COUNTER_ADDRESS),
        data: fixed_bytes!("d09de08a").into(),
        #[cfg(feature = "optimism")]
        optimism: create_optimism_fields(),
        ..TxEnv::default()
    }
}

pub fn counter_acct_info() -> AccountInfo {
    let code = deployed_bytecode(COUNTER);
    let code_hash = keccak256(&code);
    AccountInfo {
        balance: U256::ZERO,
        nonce: 1,
        code_hash,
        code: Some(Bytecode::LegacyRaw(code)),
    }
}

pub const SIMPLE_ASSERTION_COUNTER: &str = "SimpleCounterAssertion.sol:SimpleCounterAssertion";

pub fn counter_assertion() -> AssertionContract {
    get_assertion_contract(SIMPLE_ASSERTION_COUNTER).0
}

pub const FN_SELECTOR: &str = "SelectorImpl.sol:SelectorImpl";

fn get_assertion_contract(artifact: &str) -> (AssertionContract, TriggerRecorder) {
    extract_assertion_contract(bytecode(artifact), &ExecutorConfig::default()).unwrap()
}

pub fn selector_assertion() -> (AssertionContract, TriggerRecorder) {
    get_assertion_contract(FN_SELECTOR)
}

/// Returns a random FixedBytes of length N
pub fn random_bytes<const N: usize>() -> FixedBytes<N> {
    let mut value = [0u8; N];
    value.iter_mut().for_each(|x| *x = rand::random());
    FixedBytes::new(value)
}

pub fn random_address() -> Address {
    random_bytes::<20>().into()
}

pub fn random_u256() -> U256 {
    random_bytes::<32>().into()
}

pub fn random_selector() -> FixedBytes<4> {
    random_bytes::<4>()
}

pub fn random_bytes32() -> FixedBytes<32> {
    random_bytes::<32>()
}

fn read_artifact(input: &str) -> serde_json::Value {
    let mut parts = input.split(':');
    let file_name = parts.next().expect("Failed to read filename");
    let contract_name = parts.next().expect("Failed to read contract name");
    let path = format!("contract-mocks/out/{file_name}/{contract_name}.json");

    let file = std::fs::File::open(path).expect("Failed to open file");
    serde_json::from_reader(file).expect("Failed to parse JSON")
}

/// Reads deployment bytecode from a contract-mocks artifact
///
/// # Arguments
/// * `input` - ${file_name}:${contract_name}
pub fn bytecode(input: &str) -> Bytes {
    let value = read_artifact(input);
    let bytecode = value["bytecode"]["object"]
        .as_str()
        .expect("Failed to read bytecode");
    hex::decode(bytecode)
        .expect("Failed to decode bytecode")
        .into()
}

/// Reads deployed bytecode from a contract-mocks artifact
///
/// # Arguments
/// * `input` - ${file_name}:${contract_name}
pub fn deployed_bytecode(input: &str) -> Bytes {
    let value = read_artifact(input);
    let bytecode = value["deployedBytecode"]["object"]
        .as_str()
        .expect("Failed to read bytecode");
    hex::decode(bytecode)
        .expect("Failed to decode bytecode")
        .into()
}

pub async fn run_precompile_test(artifact: &str) -> TxValidationResult {
    let caller = address!("5fdcca53617f4d2b9134b29090c87d01058e27e9");
    let target = address!("118dd24a3b0d02f90d8896e242d3838b4d37c181");

    let db = OverlayDb::<CacheDB<EmptyDBTyped<Infallible>>>::new_test();

    let mut fork_db = db.fork();

    // Write test assertion to assertion store
    // bytecode of contract-mocks/src/GetLogsTest.sol:GetLogsTest
    let assertion_code = bytecode(&format!("{artifact}.sol:{artifact}"));

    let assertion_store = AssertionStore::new_ephemeral().unwrap();
    assertion_store
        .insert(target, AssertionState::new_test(assertion_code))
        .unwrap();

    let mut executor = ExecutorConfig::default().build(db, assertion_store);

    // Deploy mock using bytecode of contract-mocks/src/GetLogsTest.sol:Target
    let target_deployment_tx = TxEnv {
        caller,
        data: bytecode("Target.sol:Target"),
        transact_to: TxKind::Create,
        #[cfg(feature = "optimism")]
        optimism: create_optimism_fields(),
        ..Default::default()
    };

    let mut mock_db = crate::db::overlay::test_utils::MockDb::new();

    // Execute target deployment tx
    let result = executor
        .execute_forked_tx_ext_db(
            BlockEnv::default(),
            target_deployment_tx,
            &mut fork_db,
            &mut mock_db,
        )
        .unwrap();
    mock_db.commit(result.1.state.clone());

    // Deploy TriggeringTx contract using bytecode of
    // contract-mocks/src/GetLogsTest.sol:TriggeringTx
    let trigger_tx = TxEnv {
        caller,
        data: bytecode(&format!("{}.sol:{}", artifact, "TriggeringTx")),
        transact_to: TxKind::Create,
        #[cfg(feature = "optimism")]
        optimism: create_optimism_fields(),
        ..Default::default()
    };

    //Execute triggering tx.
    executor
        .validate_transaction_ext_db(BlockEnv::default(), trigger_tx, &mut fork_db, &mut mock_db)
        .unwrap()
}
/// Mines a block from an anvil provider, returning the block header
pub async fn mine_block(provider: &RootProvider<alloy_network::Ethereum>) -> Header {
    let _ = provider.evm_mine(None).await;
    let block = provider
        .get_block(BlockId::latest(), BlockTransactionsKind::Hashes)
        .await
        .unwrap()
        .unwrap();

    block.header
}

/// Get anvil provider
pub async fn anvil_provider() -> (RootProvider<alloy_network::Ethereum>, AnvilInstance) {
    let anvil = Anvil::new().spawn();
    let provider = ProviderBuilder::new()
        .on_ws(WsConnect::new(anvil.ws_endpoint()))
        .await
        .unwrap();
    provider.anvil_set_auto_mine(false).await.unwrap();
    #[allow(deprecated)]
    let provider = provider.root().clone().boxed();

    (provider, anvil)
}
