#![no_main]
use alloy_sol_types::{
    SolCall,
    SolValue,
};
use assertion_executor::{
    db::{
        multi_fork_db::ForkId,
        overlay::test_utils::MockDb,
        MultiForkDb,
    },
    inspectors::{
        precompiles::load::load_external_slot,
        sol_primitives::PhEvm::loadCall,
    },
    primitives::{
        Address,
        Bytes,
        JournaledState,
        SpecId,
        U256,
    },
};
use libfuzzer_sys::fuzz_target;

use alloy_primitives::map::HashSet;

use revm::{
    interpreter::CallInputs,
    DatabaseRef,
    InnerEvmContext,
};

/// Struct that returns generated call input params so we can querry them
/// for validity
#[derive(Debug, Default)]
struct CallInputParams {
    target: Address,
    slot: U256,
}

/// Helper to create CallInputs from fuzzer data
fn create_call_inputs(data: &[u8]) -> (CallInputs, CallInputParams) {
    // Need at least enough bytes for target address + slot
    if data.len() < 52 {
        let tx_env = revm::primitives::TxEnv {
            caller: Address::default(),
            value: U256::ZERO,
            data: Bytes::default(),
            gas_limit: 100_000,
            transact_to: revm::primitives::TxKind::Call(Address::default()),
            ..Default::default()
        };
        return (
            CallInputs::new(&tx_env, 100_000).expect("Unable to create default CallInputs"),
            CallInputParams::default(),
        );
    }

    // Extract target address (20 bytes)
    let mut target_bytes = [0u8; 20];
    target_bytes.copy_from_slice(&data[0..20]);
    let target_address = Address::from(target_bytes);

    // Extract slot (32 bytes)
    let mut slot_bytes = [0u8; 32];
    slot_bytes.copy_from_slice(&data[20..52]);
    let slot = U256::from_be_bytes(slot_bytes);

    // Create a valid loadCall input
    let call = loadCall {
        target: target_address,
        slot: slot.into(),
    };

    let call_input_params = CallInputParams {
        target: target_address,
        slot,
    };

    // Encode the call
    let encoded = loadCall::abi_encode(&call);

    // Extract additional address values if available
    let caller = if data.len() >= 72 {
        let mut caller_bytes = [0u8; 20];
        caller_bytes.copy_from_slice(&data[52..72]);
        Address::from(caller_bytes)
    } else {
        Address::default()
    };

    // Set a proper gas limit based on fuzzer data
    let gas_limit = if data.len() >= 80 {
        let mut gas_bytes = [0u8; 8];
        gas_bytes.copy_from_slice(&data[72..80]);
        u64::from_be_bytes(gas_bytes)
    } else {
        100000 // Default gas limit
    };

    // Extract value if available
    let value = if data.len() >= 112 {
        let mut value_bytes = [0u8; 32];
        value_bytes.copy_from_slice(&data[80..112]);
        U256::from_be_bytes(value_bytes)
    } else {
        U256::ZERO
    };

    let tx_env = revm::primitives::TxEnv {
        caller,
        value,
        data: encoded.into(),
        gas_limit,
        transact_to: revm::primitives::TxKind::Call(target_address),
        ..Default::default()
    };

    (
        CallInputs::new(&tx_env, gas_limit).expect("Unable to create CallInputs"),
        call_input_params,
    )
}

// The fuzz target definition for libFuzzer
fuzz_target!(|data: &[u8]| {
    // Need at least some minimum amount of data
    if data.len() < 53 {
        return;
    }

    // Create call inputs from fuzzer data
    let (call_inputs, params) = create_call_inputs(data);

    let mut post_tx_db = MockDb::new();
    post_tx_db.insert_storage(params.target, params.slot, U256::from(0xdeadbeef_u32));
    let slot_value = post_tx_db.storage_ref(params.target, params.slot).unwrap();

    let journaled_state = JournaledState::new(SpecId::default(), HashSet::default());

    // Create a minimally viable context
    let mut multi_fork = MultiForkDb::new(MockDb::new(), post_tx_db);
    multi_fork
        .switch_fork(
            ForkId::PostTx,
            &mut journaled_state.clone(),
            &journaled_state,
        )
        .unwrap();
    let context = InnerEvmContext::new(&mut multi_fork);

    // Call the target function and catch any panics
    let _ = std::panic::catch_unwind(|| {
        match load_external_slot(&context, &call_inputs) {
            Ok(rax) => {
                let bytes: Bytes = SolValue::abi_encode(&slot_value).into();
                // Check if the result is valid
                assert_eq!(rax, bytes);
            }
            Err(err) => {
                // Handle the error case
                panic!("Error loading external slot: {err}");
            }
        }
    });
});
