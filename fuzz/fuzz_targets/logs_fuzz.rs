#![no_main]
use alloy_primitives::{
    FixedBytes,
    Log,
    LogData,
};
use alloy_sol_types::{
    SolCall,
    SolType,
};
use assertion_executor::{
    inspectors::{
        precompiles::logs::get_logs,
        sol_primitives::{
            PhEvm,
            PhEvm::loadCall,
        },
        CallTracer,
        LogsAndTraces,
        PhEvmContext,
    },
    primitives::{
        Address,
        Bytes,
        U256,
    },
};
use libfuzzer_sys::fuzz_target;

use revm::interpreter::CallInputs;

/// Struct that returns generated call input params so we can querry them
/// for validity
#[derive(Debug, Default)]
#[allow(dead_code)]
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

fn create_log(data: &[u8]) -> Log {
    // We need 20 bytes for the address and everything else
    // for the data.
    let mut target_bytes = [0u8; 20];
    target_bytes.copy_from_slice(&data[0..20]);
    let address = Address::from(target_bytes);

    // Now create data with the rest
    let topic = &data[20..52];

    let log_data = LogData::new_unchecked(
        vec![FixedBytes::<32>::from_slice(topic)],
        Bytes::copy_from_slice(&data[52..]),
    );

    Log {
        data: log_data,
        address,
    }
}

// The fuzz target definition for libFuzzer
fuzz_target!(|data: &[u8]| {
    // Need at least some minimum amount of data
    if data.len() < 53 {
        return;
    }

    // Create a log from fuzzer data
    let log = create_log(data);

    // Create a minimally viable context
    let log_array: &[Log] = std::slice::from_ref(&log);
    let mut call_tracer = CallTracer::default();
    let (call_inputs, params) = create_call_inputs(data);
    call_tracer.record_call(call_inputs);

    let logs_traces = LogsAndTraces {
        tx_logs: log_array,
        call_traces: &call_tracer,
    };
    let context = PhEvmContext::new(&logs_traces, params.target);

    // Call the target function and catch any panics
    let _ = std::panic::catch_unwind(|| {
        match get_logs(&context) {
            Ok(rax) => {
                // Create the expected encoding - we need to match exactly what's returned
                let expected_log = PhEvm::Log {
                    topics: log.topics().to_vec(),
                    data: log.data.data.clone(),
                    emitter: log.address,
                };
                let expected_logs = vec![expected_log];
                let expected: Bytes =
                    <alloy_sol_types::sol_data::Array<PhEvm::Log>>::abi_encode(&expected_logs)
                        .into();

                // Verify the encoding matches exactly
                assert_eq!(rax, expected, "Encoded logs don't match expected value");
            }
            Err(_) => {
                // This should never happen since the function returns Infallible
                panic!("Error getting logs - this should never happen with Infallible error type");
            }
        }
    });
});
