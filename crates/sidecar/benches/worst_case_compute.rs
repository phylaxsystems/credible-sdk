use assertion_executor::{
    primitives::{
        Address,
        Bytecode,
        Bytes,
        hex,
    },
    store::AssertionState,
};
use criterion::Criterion;
use std::{
    hint::black_box,
    time::Duration,
};

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

    worst_case_compute(&mut criterion);
    criterion.final_summary();
}
