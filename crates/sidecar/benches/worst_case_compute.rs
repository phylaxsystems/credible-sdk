use assertion_executor::{
    primitives::{
        Address,
        address,
        Bytecode,
        Bytes,
        hex,
    },
    store::AssertionState,
};

use criterion::Criterion;
use revm::{context::{tx::TxEnvBuilder, TxEnv}, primitives::FixedBytes};
use std::{
    hint::black_box,
    time::Duration,
};

use sidecar::utils::test_drivers::LocalInstanceMockDriver;
use sidecar::utils::instance::LocalInstance;

const TARGET: Address = address!("118dd24a3b0d02f90d8896e242d3838b4d37c181");

fn create_txs_vec(i: u64, caller: Address) -> Vec<(FixedBytes::<32>, TxEnv)> {
    let mut txs = vec![];

    for n in 0..i {
        let tx = TxEnvBuilder::new().nonce(n).gas_limit(130_000).caller(caller).kind(revm::primitives::TxKind::Call(TARGET)).build();
        txs.push((FixedBytes::<32>::random(), tx));
    }

    txs
}

/// Reads a json file in the format of `["deployed_bytecode", ...]` and
/// returns a `Vec<Bytecode>`.
fn read_assertions_file(input: &str) -> Vec<Bytecode> {
    println!("Reading assertions from: {input}");

    let file = std::fs::File::open(input).expect("Failed to open file");
    let reader = std::io::BufReader::new(file);
    let artifacts: Vec<String> = serde_json::from_reader(reader).expect("Failed to parse JSON");

    let mut bytecodes = vec![];
    for bytecode in artifacts {
        // the bytecode we get is a hex string of bytecode
        let bytecode = hex::decode(bytecode).expect("Failed to decode bytecode");
        let bytecode = Bytecode::new_raw(bytecode.into());
        bytecodes.push(bytecode);
    }

    bytecodes
}

/// Reads a json file in the format of `["address", ...]` and
/// returns a `Vec<Address>`.
fn read_adopters_file(input: &str) -> Vec<Address> {
    println!("Reading assertions from: {input}");

    let file = std::fs::File::open(input).expect("Failed to open file");
    let reader = std::io::BufReader::new(file);
    let artifacts: Vec<String> = serde_json::from_reader(reader).expect("Failed to parse JSON");

    let mut addresses = vec![];
    for address in artifacts {
        // the bytecode we get is a hex string of bytecode
        let address = hex::decode(address).expect("Failed to decode bytecode");
        let address = Address::from_slice(&address);
        addresses.push(address);
    }

    addresses
}

fn worst_case_compute(c: &mut Criterion, local_instance: &mut LocalInstance<_>) {
    group.bench_function("worst_case_compute", |b| {
        b.iter(|| {
            // Create fresh overlay_db and store for each iteration
            let mut overlay_db = create_test_overlaydb();
            let store = create_mock_store();

            // Clone bytecodes for this iteration
            let mut bytecodes = bytecodes_orig.clone();

            // Insert all of the assertions
            for adopter in &adopters {
                for _ in 0..5 {
                    let assertion_state =
                        AssertionState::new_test(bytecodes.pop().unwrap().bytes());
                    store.insert(*adopter, assertion_state).unwrap();
                }
            }

            insert_account_info(&mut overlay_db, TARGET, bytecode_call.clone());

            // Set up executor and overlay for this iteration
            let executor = create_test_executor_with_store(store);
            {
                let mut lock = EXECUTOR.lock().unwrap();
                *lock = Some(executor);
            }
            let _ = OVERLAY.set(overlay_db.clone());

            let mut info = create_execution_info();
            let txs = create_valid_txs_vec(10);

            // Execute benchmarked function
            black_box({
                local_instance.send_block_with_txs(transactions);
                local_instance.get_transaction_result(txs.last().0).await;
            })
            .unwrap();
        })
    });
}

fn main() {
    use tracing_subscriber;
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);

    let mut criterion = Criterion::default();

    let pwd = std::env::current_dir().unwrap();
    // When running cargo bench from the crate, pwd is already in the crate directory
    let dir_str = "/benches/assertions.json";
    let adopters_str = "/benches/adopters.json";

    //50 unique assertion bytecodes
    let bytecodes_orig = read_assertions_file(&(pwd.to_str().unwrap().to_owned() + dir_str));
    // 10 adopters
    let adopters = read_adopters_file(&(pwd.to_str().unwrap().to_owned() + adopters_str));

    let bytecode_call: Bytes = hex::decode(
        "5f5f60805f5f73000000000000000000000000000000000000000063fffffffff1505f5f60805f5f73000000000000000000000000000000000000000163fffffffff1505f5f60805f5f73000000000000000000000000000000000000000263fffffffff1505f5f60805f5f73000000000000000000000000000000000000000363fffffffff1505f5f60805f5f73000000000000000000000000000000000000000463fffffffff1505f5f60805f5f73000000000000000000000000000000000000000563fffffffff1505f5f60805f5f73000000000000000000000000000000000000000663fffffffff1505f5f60805f5f73000000000000000000000000000000000000000763fffffffff1505f5f60805f5f73000000000000000000000000000000000000000863fffffffff1505f5f60805f5f73000000000000000000000000000000000000000963fffffffff150",
    ).unwrap().into();

    let assertion_store = AssertionStore::new_ephemeral().unwrap();

    // Clone bytecodes for this iteration
    let mut bytecodes = bytecodes_orig.clone();

    // Insert all of the assertions
    for adopter in &adopters {
        for _ in 0..5 {
            let assertion_state = AssertionState::new_test(&bytecodes.pop().unwrap().bytes());
            store.insert(*adopter, assertion_state).unwrap();
        }
    }

    let mut local_instance = LocalInstanceMockDriver::new_with_store(assertion_store);

    worst_case_compute(&mut criterion, &mut local_instance);
    criterion.final_summary();
}
