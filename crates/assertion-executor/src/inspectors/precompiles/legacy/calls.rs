use crate::{
    inspectors::{
        PhevmOutcome,
        phevm::PhEvmContext,
        precompiles::{
            BASE_COST,
            deduct_gas_and_check,
        },
        sol_primitives::PhEvm::CallInputs as PhEvmCallInputs,
        tracer::CallInputsWithId,
    },
    primitives::Bytes,
};

use alloy_primitives::{
    Address,
    FixedBytes,
    U256,
};
use revm::interpreter::CallScheme;

use alloy_sol_types::SolType;
use bumpalo::Bump;

#[derive(thiserror::Error, Debug)]
pub enum GetCallInputsError {
    #[error("Failed to decode getCallInputs call: {0:?}")]
    FailedToDecodeGetCallInputsCall(#[source] alloy_sol_types::Error),
    #[error(
        "Expected Bytes in CallInput input. This should be restricted to only CallInput::Bytes by the call tracer."
    )]
    ExpectedBytes,
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

const BASE_CALL_COST: u64 = 20;
const DYNAMIC_CALL_COST: u64 = 3;

/// Returns the call inputs of a transaction.
///
/// Gas for this fn is calculated as follows:
/// `gas = BASE_COST + (CALLS_PROCESSED * BASE_CALL_COST) + (ceil(CALLDATA_SIZE / 32) * DYNAMIC_CALL_COST)`
///
/// # Errors
///
/// Returns an error if decoding fails, the input type is unexpected, or gas is exhausted.
pub fn get_call_inputs(
    ph_context: &PhEvmContext,
    target: Address,
    selector: FixedBytes<4>,
    scheme_filter: Option<CallScheme>,
    gas: u64,
) -> Result<PhevmOutcome, GetCallInputsError> {
    let gas_limit = gas;
    let mut gas_left = gas;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(GetCallInputsError::OutOfGas(rax));
    }

    let mut call_inputs = ph_context
        .logs_and_traces
        .call_traces
        .get_call_inputs(target, selector);

    if let Some(scheme_filter) = scheme_filter {
        call_inputs.retain(|call_input| call_input.call_input.scheme == scheme_filter);
    }

    let encoded: Bytes = crate::arena::with_current_tx_arena(|arena| {
        let mut calldata_size: u64 = 0;
        let mut sol_call_inputs: Vec<PhEvmCallInputs, &Bump> = Vec::new_in(arena);
        for CallInputsWithId { call_input, id } in call_inputs {
            if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_CALL_COST, gas_limit) {
                return Err(GetCallInputsError::OutOfGas(rax));
            }

            let original_input_data = match &call_input.input {
                revm::interpreter::CallInput::Bytes(bytes) => bytes.clone(),
                revm::interpreter::CallInput::SharedBuffer(_) => {
                    return Err(GetCallInputsError::ExpectedBytes);
                }
            };
            let input_data_wo_selector = if original_input_data.len() >= 4 {
                original_input_data.slice(4..)
            } else {
                Bytes::new()
            };

            calldata_size += input_data_wo_selector.len() as u64;

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

        let calldata_words: u64 = calldata_size.div_ceil(32);
        let calldata_cost = calldata_words * DYNAMIC_CALL_COST;
        if let Some(rax) = deduct_gas_and_check(&mut gas_left, calldata_cost, gas_limit) {
            return Err(GetCallInputsError::OutOfGas(rax));
        }

        Ok::<_, GetCallInputsError>(
            <alloy_sol_types::sol_data::Array<PhEvmCallInputs>>::abi_encode(
                sol_call_inputs.as_slice(),
            )
            .into(),
        )
    })?;

    Ok(PhevmOutcome::new(encoded, gas_limit - gas_left))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::{
            MultiForkDb,
            fork_db::ForkDb,
            overlay::test_utils::MockDb,
        },
        inspectors::{
            TriggerRecorder,
            phevm::{
                LogsAndTraces,
                PhEvmContext,
            },
            sol_primitives::PhEvm::getCallInputsCall,
            tracer::CallTracer,
        },
        primitives::AssertionContract,
        store::{
            AssertionState,
            AssertionStore,
        },
        test_utils::{
            random_address,
            random_bytes,
            random_selector,
            random_u256,
            run_precompile_test,
        },
    };

    use alloy_sol_types::SolCall;

    fn test_with_inputs_and_tracer(
        call_inputs: &CallInputs,
        call_tracer: &CallTracer,
    ) -> Result<PhevmOutcome, GetCallInputsError> {
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: call_tracer,
        };
        let mock_db = MockDb::new();
        let pre_tx_db = ForkDb::new(mock_db);
        let mut multi_fork_db = MultiForkDb::new(pre_tx_db, &JournalInner::new());
        let context = revm::handler::MainnetContext::new(&mut multi_fork_db, SpecId::default());
        let tx_env = crate::primitives::TxEnv::default();
        let ph_context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter: Address::ZERO,
            console_logs: vec![],
            original_tx_env: &tx_env,
            assertion_spec: crate::inspectors::spec_recorder::AssertionSpec::Legacy,
            ofac_sanctions: std::sync::Arc::new(std::collections::HashSet::new()),
        };
        let input = call_inputs.input.bytes(&context);
        let inputs = getCallInputsCall::abi_decode(&input).unwrap();
        get_call_inputs(
            &ph_context,
            inputs.target,
            inputs.selector,
            None,
            call_inputs.gas_limit,
        )
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
            known_bytecode: None,
            target_address: Address::ZERO,
            caller: Address::ZERO,
            value: CallValue::Transfer(U256::ZERO),
            scheme: CallScheme::Call,
            is_static: false,
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
            known_bytecode: None,
            target_address: target,
            caller: random_address(),
            value: CallValue::Transfer(random_u256()),
            scheme: CallScheme::Call,
            is_static: false,
            return_memory_offset: 0..0,
        };
        (call_inputs, input_data.into())
    }

    fn empty_assertion_state() -> AssertionState {
        AssertionState {
            activation_block: 0,
            inactivation_block: None,
            assertion_contract: AssertionContract::default(),
            trigger_recorder: TriggerRecorder::default(),
            assertion_spec: crate::inspectors::spec_recorder::AssertionSpec::Legacy,
        }
    }

    fn empty_call_tracer() -> CallTracer {
        CallTracer::new(AssertionStore::new_ephemeral())
    }

    fn insert_adopter(store: &AssertionStore, adopter: Address) {
        // Presence is enough for CallTracer::has_assertions to mark calldata as relevant.
        store.insert(adopter, empty_assertion_state()).unwrap();
    }

    fn create_call_tracer_with_inputs<I>(call_inputs: I) -> CallTracer
    where
        I: IntoIterator<Item = (CallInputs, Bytes)>,
    {
        let assertion_store = AssertionStore::new_ephemeral();
        let mut call_tracer = CallTracer::new(assertion_store.clone());
        for (input, input_bytes) in call_inputs {
            insert_adopter(&assertion_store, input.target_address);
            call_tracer.record_call_start(input, &input_bytes, &mut JournalInner::new());
            call_tracer.result.clone().unwrap();

            call_tracer.record_call_end(&mut JournalInner::new(), false);
            call_tracer.result.clone().unwrap();
        }
        call_tracer
    }

    #[tokio::test]
    async fn test_get_call_inputs_success() {
        let target = random_address();
        let selector = random_selector();

        // Set up the context
        let mock_call_input = create_random_call_input::<32>(target, selector);

        let get_call_inputs = create_get_call_input(target, selector);
        let call_tracer = create_call_tracer_with_inputs(vec![mock_call_input.clone()]);

        let result = test_with_inputs_and_tracer(&get_call_inputs, &call_tracer);

        let encoded = result.unwrap().into_bytes();

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

    #[tokio::test]
    async fn test_get_call_inputs_empty_result() {
        let target = random_address();
        let selector = random_selector();

        let get_call_inputs = create_get_call_input(target, selector);

        // Create context with no matching call inputs (different target and selector)
        let call_tracer = empty_call_tracer();

        let result = test_with_inputs_and_tracer(&get_call_inputs, &call_tracer);
        assert!(result.is_ok());

        let encoded = result.unwrap().into_bytes();

        // Should return empty array
        let decoded = <alloy_sol_types::sol_data::Array<PhEvmCallInputs>>::abi_decode(&encoded);
        assert!(decoded.is_ok());
        let decoded_array = decoded.unwrap();
        assert_eq!(decoded_array.len(), 0);
    }

    #[tokio::test]
    async fn test_get_call_inputs_multiple_results() {
        let target = random_address();
        let selector = random_selector();

        let get_call_inputs = create_get_call_input(target, selector);

        // Set up context with multiple call inputs
        let mock_call_inputs = vec![
            create_random_call_input::<32>(target, selector),
            create_random_call_input::<64>(target, selector),
        ];

        let call_tracer = create_call_tracer_with_inputs(mock_call_inputs.clone());

        let result = test_with_inputs_and_tracer(&get_call_inputs, &call_tracer);
        assert!(result.is_ok());

        let encoded = result.unwrap().into_bytes();
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
    async fn test_get_call_inputs_gas_accounting() {
        let target = random_address();
        let selector = random_selector();

        let get_call_inputs = create_get_call_input(target, selector);

        let mock_call_inputs = vec![
            create_random_call_input::<0>(target, selector),
            create_random_call_input::<31>(target, selector),
            create_random_call_input::<32>(target, selector),
            create_random_call_input::<33>(target, selector),
        ];
        let call_tracer = create_call_tracer_with_inputs(mock_call_inputs.clone());

        let outcome = test_with_inputs_and_tracer(&get_call_inputs, &call_tracer).unwrap();

        let calls_processed = mock_call_inputs.len() as u64;
        let calldata_size: u64 = mock_call_inputs
            .iter()
            .map(|(_, bytes)| bytes.len().saturating_sub(4) as u64)
            .sum();
        let expected_gas = BASE_COST
            + (calls_processed * BASE_CALL_COST)
            + (calldata_size.div_ceil(32) * DYNAMIC_CALL_COST);

        assert_eq!(outcome.gas(), expected_gas);
    }

    #[tokio::test]
    async fn test_get_call_inputs_create() {
        let result = run_precompile_test("TestGetCallInputsCreate");
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
        assert_eq!(result.assertions_executions.len(), 1);
    }

    #[tokio::test]
    async fn test_get_call_inputs() {
        let result = run_precompile_test("TestGetCallInputs");
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
        assert_eq!(result.assertions_executions.len(), 1);
    }

    #[tokio::test]
    async fn test_get_call_inputs_reverts() {
        let result = run_precompile_test("TestGetCallInputsReverts");
        assert!(!result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert_eq!(result.assertions_executions.len(), 1);
        assert!(result_and_state.result.is_success());
    }

    #[tokio::test]
    async fn test_get_call_inputs_recursive() {
        let result = run_precompile_test("TestGetCallInputsRecursive");
        assert!(result.is_valid(), "{result:#?}");
        assert_eq!(result.assertions_executions.len(), 1);
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }

    #[tokio::test]
    async fn test_get_call_inputs_static() {
        let result = run_precompile_test("TestGetCallInputsStatic");
        assert!(result.is_valid(), "{result:#?}");
        assert_eq!(result.assertions_executions.len(), 1, "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }

    #[tokio::test]
    async fn test_get_call_inputs_proxy() {
        let result = run_precompile_test("TestGetCallInputsProxy");
        assert!(result.is_valid(), "{result:#?}");
        assert_eq!(result.assertions_executions.len(), 1, "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }

    #[tokio::test]
    async fn test_get_call_inputs_call_code() {
        let result = run_precompile_test("TestGetCallInputsCallCode");
        assert!(result.is_valid(), "{result:#?}");
        assert_eq!(result.assertions_executions.len(), 1, "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }

    #[tokio::test]
    async fn test_get_call_inputs_delegate() {
        let result = run_precompile_test("TestGetCallInputsDelegate");
        assert!(result.is_valid(), "{result:#?}");
        assert_eq!(result.assertions_executions.len(), 1, "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }

    #[tokio::test]
    async fn test_get_call_inputs_all_calls() {
        let result = run_precompile_test("TestGetCallInputsAllCalls");
        assert!(result.is_valid(), "{result:#?}");
        assert_eq!(result.assertions_executions.len(), 1, "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
