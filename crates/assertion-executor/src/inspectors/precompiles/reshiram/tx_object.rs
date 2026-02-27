use crate::{
    inspectors::{
        PhEvmContext,
        phevm::PhevmOutcome,
        precompiles::deduct_gas_and_check,
        sol_primitives::PhEvm::TxObject,
    },
    primitives::{
        Bytes,
        TxKind,
    },
};

use alloy_primitives::Address;
use alloy_sol_types::SolValue;

use super::BASE_COST;

/// Base cost for the fixed-size fields in `TxObject` (102 bytes = 4 words)
const FIXED_FIELDS_COST: u64 = 12;
/// Cost per 32-byte word for dynamic data (input calldata)
const DYNAMIC_WORD_COST: u64 = 3;

/// Returns the `TxEnv` from the original assertion-triggering transaction.
///
/// Has a fixed cost of `BASE_COST` + 96 bytes (size of fixed values) + dynamic cost
/// of the original calldata.
#[must_use]
pub fn load_tx_object(context: &PhEvmContext, gas: u64) -> PhevmOutcome {
    let gas_limit = gas;
    let mut gas_left = gas;

    let tx_env = context.original_tx_env;

    let input_words = tx_env.data.len().div_ceil(32) as u64;
    let dynamic_cost = input_words * DYNAMIC_WORD_COST;

    if let Some(rax) = deduct_gas_and_check(
        &mut gas_left,
        BASE_COST + FIXED_FIELDS_COST + dynamic_cost,
        gas_limit,
    ) {
        return rax;
    }

    // Extract the 'to' address from TxKind
    let to: Address = match tx_env.kind {
        TxKind::Call(addr) => addr,
        TxKind::Create => Address::ZERO,
    };

    let object = TxObject {
        from: tx_env.caller,
        to,
        value: tx_env.value,
        chain_id: tx_env.chain_id.unwrap_or(0),
        gas_limit: tx_env.gas_limit,
        gas_price: tx_env.gas_price,
        input: tx_env.data.clone(),
    };

    let encoded: Bytes = object.abi_encode().into();

    PhevmOutcome::new(encoded, gas_limit - gas_left)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        inspectors::{
            phevm::{
                LogsAndTraces,
                PhEvmContext,
            },
            tracer::CallTracer,
        },
        primitives::{
            Address,
            TxEnv,
            TxKind,
        },
        store::AssertionStore,
        test_utils::{
            random_address,
            random_u256,
            run_precompile_test_with_spec,
        },
    };

    use alloy_primitives::Bytes;
    use alloy_sol_types::SolValue;

    fn create_ph_context<'a>(
        tx_env: &'a TxEnv,
        logs_and_traces: &'a LogsAndTraces<'a>,
    ) -> PhEvmContext<'a> {
        PhEvmContext {
            logs_and_traces,
            adopter: Address::ZERO,
            console_logs: vec![],
            original_tx_env: tx_env,
            assertion_spec: crate::inspectors::spec_recorder::AssertionSpec::Reshiram,
            ofac_sanctions: std::sync::Arc::new(std::collections::HashSet::new()),
        }
    }

    fn empty_logs_and_traces() -> (Vec<revm::primitives::Log>, CallTracer) {
        let call_tracer = CallTracer::new(AssertionStore::new_ephemeral());
        (vec![], call_tracer)
    }

    #[tokio::test]
    async fn test_load_tx_object_basic() {
        let caller = random_address();
        let to = random_address();
        let value = random_u256();
        let chain_id = 1u64;
        let gas_limit = 100_000u64;
        let gas_price = 20_000_000_000u128;
        let input_data = Bytes::from_static(&[0x12, 0x34, 0x56, 0x78]);

        let tx_env = TxEnv {
            caller,
            kind: TxKind::Call(to),
            value,
            chain_id: Some(chain_id),
            gas_limit,
            gas_price,
            data: input_data.clone(),
            ..Default::default()
        };

        let (logs, call_tracer) = empty_logs_and_traces();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &call_tracer,
        };
        let ph_context = create_ph_context(&tx_env, &logs_and_traces);

        let outcome = load_tx_object(&ph_context, 1_000_000);
        let decoded = <TxObject as SolValue>::abi_decode(outcome.bytes().as_ref()).unwrap();

        assert_eq!(decoded.from, caller);
        assert_eq!(decoded.to, to);
        assert_eq!(decoded.value, value);
        assert_eq!(decoded.chain_id, chain_id);
        assert_eq!(decoded.gas_limit, gas_limit);
        assert_eq!(decoded.gas_price, gas_price);
        assert_eq!(decoded.input, input_data);
    }

    #[tokio::test]
    async fn test_load_tx_object_create_tx() {
        let caller = random_address();
        let value = random_u256();
        let input_data = Bytes::from_static(&[0xaa, 0xbb, 0xcc]);

        let tx_env = TxEnv {
            caller,
            kind: TxKind::Create,
            value,
            chain_id: Some(42),
            gas_limit: 500_000,
            gas_price: 1_000_000_000,
            data: input_data.clone(),
            ..Default::default()
        };

        let (logs, call_tracer) = empty_logs_and_traces();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &call_tracer,
        };
        let ph_context = create_ph_context(&tx_env, &logs_and_traces);

        let outcome = load_tx_object(&ph_context, 1_000_000);
        let decoded = <TxObject as SolValue>::abi_decode(outcome.bytes().as_ref()).unwrap();

        assert_eq!(decoded.from, caller);
        assert_eq!(decoded.to, Address::ZERO);
        assert_eq!(decoded.value, value);
    }

    #[tokio::test]
    async fn test_load_tx_object_no_chain_id() {
        let tx_env = TxEnv {
            caller: random_address(),
            kind: TxKind::Call(random_address()),
            chain_id: None,
            ..Default::default()
        };

        let (logs, call_tracer) = empty_logs_and_traces();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &call_tracer,
        };
        let ph_context = create_ph_context(&tx_env, &logs_and_traces);

        let outcome = load_tx_object(&ph_context, 1_000_000);
        let decoded = <TxObject as SolValue>::abi_decode(outcome.bytes().as_ref()).unwrap();

        assert_eq!(decoded.chain_id, 0);
    }

    #[tokio::test]
    async fn test_load_tx_object_gas_accounting_empty_input() {
        let tx_env = TxEnv {
            caller: random_address(),
            kind: TxKind::Call(random_address()),
            data: Bytes::new(),
            ..Default::default()
        };

        let (logs, call_tracer) = empty_logs_and_traces();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &call_tracer,
        };
        let ph_context = create_ph_context(&tx_env, &logs_and_traces);

        let outcome = load_tx_object(&ph_context, 1_000_000);

        // BASE_COST + FIXED_FIELDS_COST + 0 dynamic cost
        assert_eq!(outcome.gas(), BASE_COST + FIXED_FIELDS_COST);
    }

    #[tokio::test]
    async fn test_load_tx_object_gas_accounting_with_input() {
        // 33 bytes of input data = 2 words (ceil(33/32))
        let input_data = Bytes::from(vec![0u8; 33]);
        let tx_env = TxEnv {
            caller: random_address(),
            kind: TxKind::Call(random_address()),
            data: input_data,
            ..Default::default()
        };

        let (logs, call_tracer) = empty_logs_and_traces();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &call_tracer,
        };
        let ph_context = create_ph_context(&tx_env, &logs_and_traces);

        let outcome = load_tx_object(&ph_context, 1_000_000);

        // BASE_COST + FIXED_FIELDS_COST + 2 words * DYNAMIC_WORD_COST
        let expected_gas = BASE_COST + FIXED_FIELDS_COST + (2 * DYNAMIC_WORD_COST);
        assert_eq!(outcome.gas(), expected_gas);
    }

    #[tokio::test]
    async fn test_load_tx_object_gas_accounting_exact_word() {
        // 32 bytes of input data = 1 word
        let input_data = Bytes::from(vec![0u8; 32]);
        let tx_env = TxEnv {
            caller: random_address(),
            kind: TxKind::Call(random_address()),
            data: input_data,
            ..Default::default()
        };

        let (logs, call_tracer) = empty_logs_and_traces();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &logs,
            call_traces: &call_tracer,
        };
        let ph_context = create_ph_context(&tx_env, &logs_and_traces);

        let outcome = load_tx_object(&ph_context, 1_000_000);

        // BASE_COST + FIXED_FIELDS_COST + 1 word * DYNAMIC_WORD_COST
        let expected_gas = BASE_COST + FIXED_FIELDS_COST + DYNAMIC_WORD_COST;
        assert_eq!(outcome.gas(), expected_gas);
    }

    #[tokio::test]
    async fn test_load_tx_object_integration() {
        let result = run_precompile_test_with_spec(
            "TestLoadTxObject",
            crate::inspectors::spec_recorder::AssertionSpec::Reshiram,
        );
        assert!(result.is_valid());
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
