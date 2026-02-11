use crate::inspectors::{
    phevm::{
        PhEvmContext,
        PhevmOutcome,
    },
    precompiles::BASE_COST,
};

use alloy_sol_types::SolValue;
use std::convert::Infallible;

/// Returns the assertion adopter as a bytes array.
///
/// # Errors
///
/// This function returns `Infallible` and does not error.
pub fn get_assertion_adopter(context: &PhEvmContext) -> Result<PhevmOutcome, Infallible> {
    const AA_RET_COST: u64 = 9;
    Ok(PhevmOutcome::new(
        context.adopter.abi_encode().into(),
        BASE_COST + AA_RET_COST,
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        inspectors::{
            inspector_result_to_call_outcome,
            phevm::{
                LogsAndTraces,
                PhEvmContext,
            },
            tracer::CallTracer,
        },
        test_utils::{
            random_address,
            run_precompile_test,
        },
    };
    use alloy_primitives::Address;
    use alloy_sol_types::SolValue;

    fn with_adopter_context<F, R>(adopter: Address, f: F) -> R
    where
        F: FnOnce(&PhEvmContext) -> R,
    {
        let call_tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();

        let context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter,
            console_logs: vec![],
            original_tx_env: &tx_env,
        };
        f(&context)
    }

    fn test_get_assertion_adopter_helper(adopter: Address) {
        let result = with_adopter_context(adopter, get_assertion_adopter);
        assert!(result.is_ok());

        let outcome = result.unwrap();
        assert_eq!(outcome.gas(), 24);

        let encoded = outcome.bytes();
        assert!(!encoded.is_empty());

        // Verify we can decode the result back to the original address
        let decoded = Address::abi_decode(encoded);
        assert!(decoded.is_ok());
        assert_eq!(decoded.unwrap(), adopter);
    }

    #[test]
    fn test_get_assertion_adopter_zero_address() {
        test_get_assertion_adopter_helper(Address::ZERO);
    }

    #[test]
    fn test_get_assertion_adopter_random_address() {
        test_get_assertion_adopter_helper(random_address());
    }

    #[test]
    fn test_get_assertion_adopter_gas_decremented_by_inspector() {
        let adopter = random_address();
        let outcome = with_adopter_context(adopter, get_assertion_adopter).unwrap();
        let available_gas = 100;

        let call_outcome =
            inspector_result_to_call_outcome::<Infallible>(Ok(outcome), available_gas, 0..0);

        assert_eq!(call_outcome.result.gas.remaining(), available_gas - 24);

        let decoded = Address::abi_decode(&call_outcome.result.output);
        assert!(decoded.is_ok());
        assert_eq!(decoded.unwrap(), adopter);
    }

    #[tokio::test]
    async fn test_get_assertion_adopter_integration() {
        let result = run_precompile_test("TestGetAdopter");
        assert!(result.is_valid());
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
