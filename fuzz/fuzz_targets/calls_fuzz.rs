#![no_main]
use alloy_primitives::{
    FixedBytes,
    Log,
};
use alloy_sol_types::{
    SolCall,
    SolType,
};

use assertion_executor::{
    inspectors::{
        precompiles::calls::get_call_inputs,
        sol_primitives::PhEvm,
        sol_primitives::PhEvm::loadCall,
        spec_recorder::AssertionSpec,
        CallTracer,
        LogsAndTraces,
        PhEvmContext,
        TriggerRecorder,
    },
    primitives::{
        Address,
        Bytes,
        JournalEntry,
        U256,
        AssertionContract,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
};
use libfuzzer_sys::fuzz_target;

use revm::context::JournalInner;
use revm::interpreter::{
    CallInputs,
    CallScheme,
    CallInput,
};

/// Struct that returns generated call input params so we can querry them
/// for validity
#[derive(Debug, Default)]
#[allow(dead_code)]
struct CallInputParams {
    target: Address,
    slot: U256,
}

fn dummy_assertion_state() -> AssertionState {
    AssertionState {
        activation_block: 0,
        inactivation_block: None,
        assertion_contract: AssertionContract::default(),
        trigger_recorder: TriggerRecorder::default(),
    }
}

fn insert_dummy_assertion(store: &AssertionStore, adopter: Address) {
    // Presence is enough for the call tracer to treat calldata as relevant.
    store.insert(adopter, dummy_assertion_state()).unwrap();
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
    if data.len() < 65 {
        return;
    }

    // Create call inputs from fuzzer data
    let (call_inputs, params) = create_call_inputs(data);

    // Create a minimally viable context
    let log_array: &[Log] = &[];
    let assertion_store = AssertionStore::new_ephemeral().unwrap();
    insert_dummy_assertion(&assertion_store, params.target);
    let mut call_tracer = CallTracer::new(assertion_store);

    // Explicitly create a call input with the correct selector for the context
    let selector_bytes = if data.len() >= 28 {
        let mut sel = [0u8; 4];
        sel.copy_from_slice(&data[24..28]); // Use 4 bytes for selector
        FixedBytes::from(sel)
    } else {
        FixedBytes::default()
    };

    // Create a properly formatted input for the CallTracer
    let target_call_input = CallInputs {
        // Use the selector as the first 4 bytes of input
        input: Bytes::from([&selector_bytes[..], &call_inputs.input[..]].concat()),
        return_memory_offset: 0..0,
        gas_limit: call_inputs.gas_limit,
        bytecode_address: params.target,
        target_address: params.target,
        caller: call_inputs.caller,
        value: call_inputs.value.clone(),
        is_static: false,
        scheme: CallScheme::Call,
    };

    let mut journal = JournalInner::<JournalEntry>::new();
    let input_bytes = match &target_call_input.input {
        CallInput::Bytes(bytes) => bytes.clone(),
        _ => Bytes::new(),
    };
    call_tracer.record_call_start(target_call_input.clone(), &input_bytes, &mut journal);
    call_tracer.result.clone().unwrap();
    call_tracer.record_call_end(&mut journal);
    call_tracer.result.clone().unwrap();
    let logs_traces = LogsAndTraces {
        tx_logs: log_array,
        call_traces: &call_tracer,
    };
    let default_tx_env = revm::primitives::TxEnv::default();
    let context = PhEvmContext::new(&logs_traces, params.target, &default_tx_env, AssertionSpec::Legacy);

    // Modify the call_inputs to include the selector in the expected position
    // in the input data for get_call_inputs
    let mut precompile_input = Vec::with_capacity(68); // 4 bytes selector + 64 bytes params

    // Precompile selector (can be any 4 bytes)
    precompile_input.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]);

    // First parameter: target address (padded to 32 bytes)
    let mut param1 = [0u8; 32];
    let target_bytes = params.target.as_slice();
    param1[12..32].copy_from_slice(target_bytes);
    precompile_input.extend_from_slice(&param1);

    // Second parameter: selector (padded to 32 bytes)
    let mut param2 = [0u8; 32];
    param2[0..4].copy_from_slice(&selector_bytes[..]);
    precompile_input.extend_from_slice(&param2);

    let precompile_call = CallInputs {
        input: Bytes::from(precompile_input),
        return_memory_offset: 0..0,
        gas_limit: call_inputs.gas_limit,
        bytecode_address: Address::default(),
        target_address: Address::default(),
        caller: call_inputs.caller,
        value: call_inputs.value,
        is_static: false,
        scheme: CallScheme::Call,
    };

    // Call the target function with the modified inputs
    let _ = std::panic::catch_unwind(|| {
        match get_call_inputs(&precompile_call, &context) {
            Ok(rax) => {
                let inputs = PhEvm::CallInputs {
                    caller: target_call_input.caller,
                    gas_limit: target_call_input.gas_limit,
                    bytecode_address: target_call_input.bytecode_address,
                    input: target_call_input.input.clone()[4..].to_vec().into(), // Skip the first 4 bytes (selector)
                    target_address: target_call_input.target_address,
                    value: target_call_input.value.get(),
                };
                let inputs = vec![inputs];

                let encoded: Bytes =
                    <alloy_sol_types::sol_data::Array<PhEvm::CallInputs>>::abi_encode(&inputs)
                        .into();
                assert_eq!(rax, encoded);
            }
            Err(err) => {
                panic!("Error loading external call inputs: {err}");
            }
        }
    });
});
