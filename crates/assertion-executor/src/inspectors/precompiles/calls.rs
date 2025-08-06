use crate::{
    db::{
        DatabaseRef,
        MultiForkDb,
    },
    inspectors::{
        phevm::PhEvmContext,
        sol_primitives::PhEvm::{
            CallInputs as PhEvmCallInputs,
            getCallInputsCall,
        },
        tracer::CallInputsWithId,
    },
    primitives::Bytes,
};

use alloy_primitives::U256;
use revm::{
    context::{
        ContextTr,
        Journal,
    },
    interpreter::CallInputs,
};

use alloy_sol_types::{
    SolCall,
    SolType,
};

#[derive(thiserror::Error, Debug)]
pub enum GetCallInputsError {
    #[error("Failed to decode getCallInputs call: {0:?}")]
    FailedToDecodeGetCallInputsCall(#[from] alloy_sol_types::Error),
    #[error(
        "Expected Bytes in CallInput input. This should be restricted to only CallInput::Bytes by the call tracer."
    )]
    ExpectedBytes,
}

/// Returns the call inputs of a transaction.
pub fn get_call_inputs<'db, ExtDb: DatabaseRef + 'db, CTX>(
    inputs: &CallInputs,
    context: &mut CTX,
    ph_context: &PhEvmContext,
) -> Result<Bytes, GetCallInputsError>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let get_call_inputs = getCallInputsCall::abi_decode(&inputs.input.bytes(context))?;

    let target = get_call_inputs.target;
    let selector = get_call_inputs.selector;

    let call_inputs = ph_context
        .logs_and_traces
        .call_traces
        .get_call_inputs(target, selector);

    let mut sol_call_inputs = Vec::new();
    for CallInputsWithId { call_input, id } in call_inputs {
        let original_input_data = match &call_input.input {
            revm::interpreter::CallInput::Bytes(bytes) => bytes.clone(),
            _ => return Err(GetCallInputsError::ExpectedBytes),
        };
        let input_data_wo_selector = match original_input_data.len() >= 4 {
            true => original_input_data.slice(4..),
            false => Bytes::new(),
        };
        sol_call_inputs.push(PhEvmCallInputs {
            input: input_data_wo_selector,
            gas_limit: call_input.gas_limit,
            bytecode_address: call_input.bytecode_address,
            target_address: call_input.target_address,
            caller: call_input.caller,
            value: call_input.value.get(),
            id: U256::from(id),
        });
    }

    let encoded: Bytes =
        <alloy_sol_types::sol_data::Array<PhEvmCallInputs>>::abi_encode(&sol_call_inputs).into();

    Ok(encoded)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::{
            fork_db::ForkDb,
            overlay::test_utils::MockDb,
        },
        inspectors::{
            phevm::{
                LogsAndTraces,
                PhEvmContext,
            },
            tracer::CallTracer,
        },
        test_utils::{
            random_address,
            random_bytes,
            random_selector,
            random_u256,
            run_precompile_test,
        },
    };

    fn test_with_inputs_and_tracer(
        call_inputs: &CallInputs,
        call_tracer: CallTracer,
    ) -> Result<Bytes, GetCallInputsError> {
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let mock_db = MockDb::new();
        let pre_tx_db = ForkDb::new(mock_db);
        let mut multi_fork_db = MultiForkDb::new(pre_tx_db);
        let mut context = revm::handler::MainnetContext::new(&mut multi_fork_db, SpecId::default());
        let ph_context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter: Address::ZERO,
            console_logs: vec![],
        };
        get_call_inputs(call_inputs, &mut context, &ph_context)
    }
    use alloy_primitives::{
        Address,
        Bytes,
        FixedBytes,
        U256,
    };
    use revm::{
        context::JournalInner,
        interpreter::{
            CallInput,
            CallInputs,
            CallScheme,
            CallValue,
        },
        primitives::hardfork::SpecId,
    };

    fn create_get_call_input(target: Address, selector: FixedBytes<4>) -> CallInputs {
        let get_call_inputs = getCallInputsCall { target, selector };

        let input_data = get_call_inputs.abi_encode();

        CallInputs {
            input: CallInput::Bytes(input_data.into()),
            gas_limit: 1_000_000,
            bytecode_address: Address::ZERO,
            target_address: Address::ZERO,
            caller: Address::ZERO,
            value: CallValue::Transfer(U256::ZERO),
            scheme: CallScheme::Call,
            is_static: false,
            is_eof: false,
            return_memory_offset: 0..0,
        }
    }

    fn create_random_call_input<const INPUT_SIZE: usize>(
        target: Address,
        selector: FixedBytes<4>,
    ) -> (CallInputs, Bytes) {
        let input_data = [&selector[..], &random_bytes::<INPUT_SIZE>()[..]].concat();
        let call_inputs = CallInputs {
            input: CallInput::Bytes(input_data.clone().into()),
            gas_limit: 100_000,
            bytecode_address: random_address(),
            target_address: target,
            caller: random_address(),
            value: CallValue::Transfer(random_u256()),
            scheme: CallScheme::Call,
            is_static: false,
            is_eof: false,
            return_memory_offset: 0..0,
        };
        (call_inputs, input_data.into())
    }

    fn create_call_tracer_with_inputs<I>(call_inputs: I) -> CallTracer
    where
        I: IntoIterator<Item = (CallInputs, Bytes)>,
    {
        let mut call_tracer = CallTracer::new();
        for input in call_inputs {
            call_tracer.record_call_start(input.0, &input.1, &mut JournalInner::new());
            call_tracer.result.clone().unwrap();

            call_tracer.record_call_end(&mut JournalInner::new());
            call_tracer.result.clone().unwrap();
        }
        call_tracer
    }

    #[test]
    fn test_get_call_inputs_success() {
        let target = random_address();
        let selector = random_selector();

        // Set up the context
        let mock_call_input = create_random_call_input::<32>(target, selector);

        let get_call_inputs = create_get_call_input(target, selector);
        let call_tracer = create_call_tracer_with_inputs(vec![mock_call_input.clone()]);

        let result = test_with_inputs_and_tracer(&get_call_inputs, call_tracer);

        let encoded = result.unwrap();

        // Verify we can decode the result
        let decoded = <alloy_sol_types::sol_data::Array<PhEvmCallInputs>>::abi_decode(&encoded);

        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 1);

        assert_eq!(
            decoded_array[0].target_address,
            mock_call_input.0.target_address
        );

        assert_eq!(decoded_array[0].input, mock_call_input.1.slice(4..));
    }

    #[test]
    fn test_get_call_inputs_empty_result() {
        let target = random_address();
        let selector = random_selector();

        let get_call_inputs = create_get_call_input(target, selector);

        // Create context with no matching call inputs (different target and selector)
        let call_tracer = CallTracer::new();

        let result = test_with_inputs_and_tracer(&get_call_inputs, call_tracer);
        assert!(result.is_ok());

        let encoded = result.unwrap();

        // Should return empty array
        let decoded = <alloy_sol_types::sol_data::Array<PhEvmCallInputs>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 0);
    }

    #[test]
    fn test_get_call_inputs_invalid_input_length() {
        let target = random_address();
        let selector = random_selector();

        let mut get_call_inputs = create_get_call_input(target, selector);
        get_call_inputs.input = CallInput::Bytes(random_bytes::<32>().into());

        let call_tracer = CallTracer::new();

        let result = test_with_inputs_and_tracer(&get_call_inputs, call_tracer);
        assert!(matches!(
            result,
            Err(GetCallInputsError::FailedToDecodeGetCallInputsCall(_))
        ));
    }

    #[test]
    fn test_get_call_inputs_multiple_results() {
        let target = random_address();
        let selector = random_selector();

        let get_call_inputs = create_get_call_input(target, selector);

        // Set up context with multiple call inputs
        let mock_call_inputs = vec![
            create_random_call_input::<32>(target, selector),
            create_random_call_input::<64>(target, selector),
        ];

        let call_tracer = create_call_tracer_with_inputs(mock_call_inputs.clone());

        let result = test_with_inputs_and_tracer(&get_call_inputs, call_tracer);
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded =
            <alloy_sol_types::sol_data::Array<PhEvmCallInputs>>::abi_decode(&encoded).unwrap();
        assert_eq!(decoded.len(), 2);

        // Verify both results are present
        assert_eq!(
            decoded[0].target_address,
            mock_call_inputs[0].0.target_address
        );
        assert_eq!(decoded[0].input, mock_call_inputs[0].1.slice(4..));
        assert_eq!(
            decoded[1].target_address,
            mock_call_inputs[1].0.target_address
        );
        assert_eq!(decoded[1].input, mock_call_inputs[1].1.slice(4..));
    }

    #[tokio::test]
    async fn test_get_call_inputs_create() {
        let result = run_precompile_test("TestGetCallInputsCreate").await;
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
        assert_eq!(result.assertions_executions.len(), 1);
    }

    #[tokio::test]
    async fn test_get_call_inputs() {
        let result = run_precompile_test("TestGetCallInputs").await;
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
        assert_eq!(result.assertions_executions.len(), 1);
    }

    #[tokio::test]
    async fn test_get_call_inputs_reverts() {
        let result = run_precompile_test("TestGetCallInputsReverts").await;
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert_eq!(result.assertions_executions.len(), 1);
        assert!(result_and_state.result.is_success());
    }

    #[tokio::test]
    async fn test_get_call_inputs_recursive() {
        let result = run_precompile_test("TestGetCallInputsRecursive").await;
        assert!(result.is_valid(), "{result:#?}");
        assert_eq!(result.assertions_executions.len(), 1);
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
