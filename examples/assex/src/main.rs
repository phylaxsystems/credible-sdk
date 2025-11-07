use assex::{
    ExecutorConfig,
    constants::{
        ASSERTION_CONTRACT,
        CALLER,
    },
    primitives::{
        AccountInfo,
        TxEnv,
        TxKind,
        U256,
    },
    store::AssertionStore,
    test_utils::{
        counter_acct_info,
        COUNTER_ADDRESS,
    },
};
use alloy_sol_types::{
    SolCall,
    sol,
};
use revm::database::InMemoryDB;

sol! {
    #[allow(missing_docs)]
    contract CounterInterface {
        function increment();
        function incrementBy(uint256 amount);
    }
}

fn populate_db(node_db: &mut InMemoryDB) {
    node_db.insert_account_info(COUNTER_ADDRESS, counter_acct_info());

    node_db.insert_account_info(
        CALLER,
        AccountInfo {
            balance: U256::MAX,
            ..Default::default()
        },
    );
}

fn build_counter_tx(data: Vec<u8>, nonce: u64) -> TxEnv {
    let mut tx = TxEnv::default();
    tx.caller = CALLER;
    tx.nonce = nonce;
    tx.gas_limit = 1_000_000;
    tx.kind = TxKind::Call(COUNTER_ADDRESS);
    tx.data = data.into();
    tx
}

fn increment_by(amount: u64, nonce: u64) -> TxEnv {
    let call = CounterInterface::incrementByCall {
        amount: U256::from(amount),
    };
    build_counter_tx(call.abi_encode(), nonce)
}

fn increment(nonce: u64) -> TxEnv {
    let call = CounterInterface::incrementCall {};
    build_counter_tx(call.abi_encode(), nonce)
}

fn main() {
    println!("Hello, Assex!");
    println!("Assertion contract address: {ASSERTION_CONTRACT:?}");

    // Executor is still available for later wiring, but we don't preload assertions here.
    let store = AssertionStore::new_ephemeral().expect("failed to create assertion store");
    let executor = ExecutorConfig::default().build(store);

    // Seed an in-memory node database with the counter contract and a funded caller.
    let mut node_db = InMemoryDB::default();
    populate_db(&mut node_db);

    // This vector of tx objects simulates a block:
    //  - First 200 transactions call increment_by(0)
    //  - Final 2 transactions call increment()
    const ZERO_INCREMENT_COUNT: usize = 200;
    let mut block: Vec<TxEnv> = Vec::with_capacity(ZERO_INCREMENT_COUNT + 2);
    for nonce in 0..ZERO_INCREMENT_COUNT as u64 {
        block.push(increment_by(0, nonce));
    }
    block.push(increment(ZERO_INCREMENT_COUNT as u64));
    block.push(increment(ZERO_INCREMENT_COUNT as u64 + 1));
}
