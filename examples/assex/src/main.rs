use std::convert::Infallible;

use assex::{
    ExecutorConfig,
    constants::{
        ASSERTION_CONTRACT,
        CALLER,
    },
    primitives::{
        AccountInfo,
        U256,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
    test_utils::{
        bytecode,
        counter_acct_info,
        counter_call,
        COUNTER_ADDRESS,
        SIMPLE_ASSERTION_COUNTER,
    },
};
use revm::database::{CacheDB, EmptyDBTyped, InMemoryDB};

fn populate_db(node_db: &mut CacheDB<EmptyDBTyped<Infallible>>) {
    node_db.insert_account_info(COUNTER_ADDRESS, counter_acct_info());

    let counter_call = counter_call();
    node_db.insert_account_info(
        counter_call.caller,
        AccountInfo {
            balance: U256::MAX,
            ..Default::default()
        },
    );
    node_db.insert_account_info(
        CALLER,
        AccountInfo {
            balance: U256::MAX,
            ..Default::default()
        },
    );
}

fn main() {
    println!("Hello, Assex!");
    println!("Assertion contract address: {ASSERTION_CONTRACT:?}");

    // Prepare the assertion store with the same counter assertion used in executor tests.
    let store = AssertionStore::new_ephemeral().expect("failed to create assertion store");
    let assertion_bytecode = bytecode(SIMPLE_ASSERTION_COUNTER);
    store
        .insert(
            COUNTER_ADDRESS,
            AssertionState::new_test(&assertion_bytecode),
        )
        .expect("failed to insert counter assertion");
    let executor = ExecutorConfig::default().build(store);

    // Seed an in-memory node database with the counter contract and a funded caller.
    let mut node_db = InMemoryDB::default();
    populate_db(&mut node_db);

}
