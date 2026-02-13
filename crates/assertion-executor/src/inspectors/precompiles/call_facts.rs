//! Call-derived scalar predicates and aggregates.
//!
//! These precompiles are intentionally "query style": they let assertions express
//! call-shape invariants declaratively without iterating over raw `getCallInputs()`
//! output in Solidity.

use crate::inspectors::{
    phevm::{
        PhEvmContext,
        PhevmOutcome,
    },
    precompiles::{
        BASE_COST,
        deduct_gas_and_check,
    },
    sol_primitives::PhEvm,
    tracer::{
        CallRecord,
        CallTracer,
    },
};

use alloy_primitives::{
    Address,
    FixedBytes,
    U256,
};
use alloy_sol_types::{
    SolCall,
    SolValue,
};
use revm::interpreter::CallScheme;

#[derive(thiserror::Error, Debug)]
pub enum CallFactsError {
    #[error("Failed to decode call facts input: {0:?}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
    #[error("Call ID {call_id} is out of bounds (total calls: {total})")]
    CallIdOutOfBounds { call_id: U256, total: usize },
    #[error("argIndex {arg_index} cannot be represented as usize")]
    InvalidArgIndex { arg_index: U256 },
    #[error("argIndex {arg_index} overflows calldata offset computation")]
    ArgOffsetOverflow { arg_index: usize },
}

/// Per-visited-call filter cost used by call-fact precompiles.
///
/// This keeps query gas proportional to work performed while staying aligned with
/// the lightweight accounting model used across `PhEvm` query precompiles.
const PER_CALL_COST: u64 = 5;
/// Cost per returned ABI word for fixed-size responses.
const PER_RETURN_WORD_COST: u64 = 3;
const SCALAR_RETURN_WORDS: u64 = 1;
const TRIGGER_CONTEXT_RETURN_WORDS: u64 = 6;

/// Maps the CallFilter.callType field to an optional CallScheme filter.
/// 0 = any, 1 = CALL, 2 = STATICCALL, 3 = DELEGATECALL, 4 = CALLCODE
pub(crate) fn call_type_to_scheme(call_type: u8) -> Option<CallScheme> {
    match call_type {
        0 => None,
        1 => Some(CallScheme::Call),
        2 => Some(CallScheme::StaticCall),
        3 => Some(CallScheme::DelegateCall),
        4 => Some(CallScheme::CallCode),
        _ => None,
    }
}

pub(crate) fn candidate_call_indices(
    tracer: &CallTracer,
    target: Address,
    selector: FixedBytes<4>,
) -> Option<&[usize]> {
    tracer
        .target_and_selector_indices
        .get(&crate::inspectors::tracer::TargetAndSelector { target, selector })
        .map(Vec::as_slice)
}

pub(crate) fn call_matches_filter(
    record: &CallRecord,
    filter: &PhEvm::CallFilter,
    scheme_filter: Option<CallScheme>,
) -> bool {
    if let Some(scheme) = scheme_filter
        && record.inputs().scheme != scheme
    {
        return false;
    }

    let depth = record.depth();
    if filter.topLevelOnly && depth != 0 {
        return false;
    }
    if filter.minDepth > 0 && depth < filter.minDepth {
        return false;
    }
    if filter.maxDepth > 0 && depth > filter.maxDepth {
        return false;
    }

    true
}

#[inline]
fn charge_return_words(
    gas_left: &mut u64,
    gas_limit: u64,
    words: u64,
) -> Result<(), CallFactsError> {
    let return_cost = words.saturating_mul(PER_RETURN_WORD_COST);
    if let Some(rax) = deduct_gas_and_check(gas_left, return_cost, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }
    Ok(())
}

/// `anyCall(address target, bytes4 selector, CallFilter filter) -> bool`
///
/// Returns true if at least one call matching the filter exists.
/// Typical use: guard assertion branches without pulling full call arrays.
pub fn any_call(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }

    let call = PhEvm::anyCallCall::abi_decode(input_bytes).map_err(CallFactsError::DecodeError)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let call_records = tracer.call_records();
    let scheme_filter = call_type_to_scheme(call.filter.callType);
    let mut visited = 0u64;
    let mut found = false;

    if let Some(indices) = candidate_call_indices(tracer, call.target, call.selector) {
        for &idx in indices {
            visited = visited.saturating_add(1);
            if call_matches_filter(&call_records[idx], &call.filter, scheme_filter) {
                found = true;
                break;
            }
        }
    }

    let filter_cost = visited.saturating_mul(PER_CALL_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, filter_cost, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }
    charge_return_words(&mut gas_left, gas_limit, SCALAR_RETURN_WORDS)?;

    let encoded = found.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `countCalls(address target, bytes4 selector, CallFilter filter) -> uint256`
///
/// Returns the number of calls matching the filter.
pub fn count_calls(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }

    let call =
        PhEvm::countCallsCall::abi_decode(input_bytes).map_err(CallFactsError::DecodeError)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let call_records = tracer.call_records();
    let scheme_filter = call_type_to_scheme(call.filter.callType);
    let mut visited = 0u64;
    let mut matched = 0u64;

    if let Some(indices) = candidate_call_indices(tracer, call.target, call.selector) {
        for &idx in indices {
            visited = visited.saturating_add(1);
            if call_matches_filter(&call_records[idx], &call.filter, scheme_filter) {
                matched = matched.saturating_add(1);
            }
        }
    }

    let filter_cost = visited.saturating_mul(PER_CALL_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, filter_cost, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }
    charge_return_words(&mut gas_left, gas_limit, SCALAR_RETURN_WORDS)?;

    let count = U256::from(matched);
    let encoded = count.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `callerAt(uint256 callId) -> address`
///
/// Returns the caller of the call at the given index.
///
/// Intended for assertions that already have a call id from `getTriggerContext()`
/// or from trigger-time call matching in the executor.
pub fn caller_at(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }

    let call = PhEvm::callerAtCall::abi_decode(input_bytes).map_err(CallFactsError::DecodeError)?;

    let call_id = call.callId;
    let tracer = ph_context.logs_and_traces.call_traces;
    let total = tracer.call_records().len();

    let idx: usize = call_id
        .try_into()
        .map_err(|_| CallFactsError::CallIdOutOfBounds { call_id, total })?;

    let record = tracer
        .get_call_record(idx)
        .ok_or(CallFactsError::CallIdOutOfBounds { call_id, total })?;

    charge_return_words(&mut gas_left, gas_limit, SCALAR_RETURN_WORDS)?;

    let caller = record.inputs().caller;
    let encoded = caller.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `allCallsBy(address target, bytes4 selector, address allowedCaller, CallFilter filter) -> bool`
///
/// Returns true if no calls match, or all matching calls have the specified caller.
pub fn all_calls_by(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }

    let call =
        PhEvm::allCallsByCall::abi_decode(input_bytes).map_err(CallFactsError::DecodeError)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let call_records = tracer.call_records();
    let scheme_filter = call_type_to_scheme(call.filter.callType);
    let mut visited = 0u64;
    let mut result = true;

    if let Some(indices) = candidate_call_indices(tracer, call.target, call.selector) {
        for &idx in indices {
            visited = visited.saturating_add(1);
            let record = &call_records[idx];
            if !call_matches_filter(record, &call.filter, scheme_filter) {
                continue;
            }

            if record.inputs().caller != call.allowedCaller {
                result = false;
                break;
            }
        }
    }

    let filter_cost = visited.saturating_mul(PER_CALL_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, filter_cost, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }
    charge_return_words(&mut gas_left, gas_limit, SCALAR_RETURN_WORDS)?;

    let encoded = result.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `sumArgUint(address target, bytes4 selector, uint256 argIndex, CallFilter filter) -> uint256`
///
/// Sums the uint256 argument at `argIndex` across all matching calls.
/// Calldata shorter than the requested argument is zero-extended.
pub fn sum_arg_uint(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }

    let call =
        PhEvm::sumArgUintCall::abi_decode(input_bytes).map_err(CallFactsError::DecodeError)?;

    let tracer = ph_context.logs_and_traces.call_traces;
    let arg_index: usize = call.argIndex.try_into().map_err(|_| {
        CallFactsError::InvalidArgIndex {
            arg_index: call.argIndex,
        }
    })?;
    let byte_offset = arg_index
        .checked_mul(32)
        .ok_or(CallFactsError::ArgOffsetOverflow { arg_index })?;
    let data_start = 4usize
        .checked_add(byte_offset)
        .ok_or(CallFactsError::ArgOffsetOverflow { arg_index })?;
    let data_end = data_start
        .checked_add(32)
        .ok_or(CallFactsError::ArgOffsetOverflow { arg_index })?;

    let scheme_filter = call_type_to_scheme(call.filter.callType);
    let call_records = tracer.call_records();
    let mut visited = 0u64;
    let mut total = U256::ZERO;
    if let Some(indices) = candidate_call_indices(tracer, call.target, call.selector) {
        for &idx in indices {
            visited = visited.saturating_add(1);
            let record = &call_records[idx];
            if !call_matches_filter(record, &call.filter, scheme_filter) {
                continue;
            }
            let calldata = match &record.inputs().input {
                revm::interpreter::CallInput::Bytes(b) => b,
                _ => continue,
            };

            // Skip the 4-byte selector, then read 32 bytes at the arg offset.
            if calldata.len() < data_end {
                // Calldata too short for this argIndex - zero-extend.
                let mut padded = [0u8; 32];
                let available = calldata.len().saturating_sub(data_start);
                if available > 0 {
                    padded[..available]
                        .copy_from_slice(&calldata[data_start..data_start + available]);
                }
                total = total.wrapping_add(U256::from_be_bytes(padded));
            } else {
                let word: [u8; 32] = calldata[data_start..data_end]
                    .try_into()
                    .unwrap_or_default();
                total = total.wrapping_add(U256::from_be_bytes(word));
            }
        }
    }

    let filter_cost = visited.saturating_mul(PER_CALL_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, filter_cost, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }
    charge_return_words(&mut gas_left, gas_limit, SCALAR_RETURN_WORDS)?;

    let encoded = total.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `getTriggerContext() -> TriggerContext`
///
/// Returns the context of the call that triggered this assertion execution.
///
/// This is a convenience snapshot to avoid multiple `callerAt/targetAt/...` calls.
/// If no trigger call is set (e.g., storage/balance trigger), returns zero context.
pub fn get_trigger_context(
    ph_context: &PhEvmContext,
    gas: u64,
) -> Result<PhevmOutcome, CallFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }

    let tracer = ph_context.logs_and_traces.call_traces;

    let trigger_ctx = match ph_context.trigger_call_id {
        Some(call_id) => {
            if let Some(record) = tracer.get_call_record(call_id) {
                let inputs = record.inputs();
                let selector = record.target_and_selector().selector;

                PhEvm::TriggerContext {
                    callId: U256::from(call_id),
                    caller: inputs.caller,
                    target: inputs.target_address,
                    codeAddress: inputs.bytecode_address,
                    selector,
                    depth: record.depth(),
                }
            } else {
                // Call ID set but not found - return zeroed context
                PhEvm::TriggerContext {
                    callId: U256::ZERO,
                    caller: Address::ZERO,
                    target: Address::ZERO,
                    codeAddress: Address::ZERO,
                    selector: FixedBytes::ZERO,
                    depth: 0,
                }
            }
        }
        None => {
            PhEvm::TriggerContext {
                callId: U256::ZERO,
                caller: Address::ZERO,
                target: Address::ZERO,
                codeAddress: Address::ZERO,
                selector: FixedBytes::ZERO,
                depth: 0,
            }
        }
    };

    charge_return_words(&mut gas_left, gas_limit, TRIGGER_CONTEXT_RETURN_WORDS)?;

    let encoded = trigger_ctx.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::inspectors::{
        phevm::{
            LogsAndTraces,
            PhEvmContext,
        },
        tracer::CallTracer,
    };
    use alloy_primitives::{
        Bytes,
        address,
    };
    use revm::{
        context::JournalInner,
        interpreter::{
            CallInput,
            CallInputs,
            CallScheme,
            CallValue,
        },
    };

    fn make_call_inputs(
        target: Address,
        caller: Address,
        selector: FixedBytes<4>,
        scheme: CallScheme,
    ) -> (CallInputs, Bytes) {
        let input: Bytes = selector.into();
        (
            CallInputs {
                input: CallInput::Bytes(input.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: target,
                known_bytecode: None,
                target_address: target,
                caller,
                value: CallValue::default(),
                scheme,
                is_static: false,
            },
            input,
        )
    }

    fn make_ph_context<'a>(
        logs_and_traces: &'a LogsAndTraces<'a>,
        tx_env: &'a crate::primitives::TxEnv,
    ) -> PhEvmContext<'a> {
        PhEvmContext::new(logs_and_traces, Address::ZERO, tx_env)
    }

    fn default_filter() -> PhEvm::CallFilter {
        PhEvm::CallFilter {
            callType: 0,
            minDepth: 0,
            maxDepth: 0,
            topLevelOnly: false,
        }
    }

    #[test]
    fn test_any_call_no_calls() {
        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let target = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let input = PhEvm::anyCallCall {
            target,
            selector,
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = any_call(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(!decoded, "Should return false when no calls exist");
    }

    #[test]
    fn test_any_call_match() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::anyCallCall {
            target,
            selector,
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = any_call(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(decoded, "Should return true when matching call exists");
    }

    #[test]
    fn test_any_call_no_match() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let wrong_selector = FixedBytes::<4>::from([0xAA, 0xBB, 0xCC, 0xDD]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        // Query with wrong selector
        let input = PhEvm::anyCallCall {
            target,
            selector: wrong_selector,
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = any_call(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(!decoded, "Should return false for wrong selector");
    }

    #[test]
    fn test_count_calls_multiple() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        // Record 3 matching calls
        for _ in 0..3 {
            journal.depth = 0;
            let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
            tracer.record_call_start(inputs, &bytes, &mut journal);
            tracer.record_call_end(&mut journal, false);
        }

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::countCallsCall {
            target,
            selector,
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = count_calls(&context, &encoded, 1_000_000).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, U256::from(3));
    }

    #[test]
    fn test_call_filter_by_scheme() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        // Record a CALL
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Record a DELEGATECALL
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::DelegateCall);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        // Filter for DELEGATECALL only (callType=3)
        let input = PhEvm::countCallsCall {
            target,
            selector,
            filter: PhEvm::CallFilter {
                callType: 3,
                minDepth: 0,
                maxDepth: 0,
                topLevelOnly: false,
            },
        };
        let encoded = input.abi_encode();
        let result = count_calls(&context, &encoded, 1_000_000).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, U256::from(1), "Only DELEGATECALL should match");
    }

    #[test]
    fn test_call_filter_by_depth() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        // Root call (depth 0)
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);

        // Nested call (depth 1)
        journal.depth = 1;
        let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // End root
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        // Filter for minDepth=1 only
        let input = PhEvm::countCallsCall {
            target,
            selector,
            filter: PhEvm::CallFilter {
                callType: 0,
                minDepth: 1,
                maxDepth: 0,
                topLevelOnly: false,
            },
        };
        let encoded = input.abi_encode();
        let result = count_calls(&context, &encoded, 1_000_000).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        assert_eq!(
            decoded,
            U256::from(1),
            "Only nested call at depth 1 should match"
        );
    }

    #[test]
    fn test_caller_at_valid() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::callerAtCall {
            callId: U256::from(0),
        };
        let encoded = input.abi_encode();
        let result = caller_at(&context, &encoded, 1_000_000).unwrap();
        let decoded = Address::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, caller);
    }

    #[test]
    fn test_caller_at_invalid() {
        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::callerAtCall {
            callId: U256::from(99),
        };
        let encoded = input.abi_encode();
        let result = caller_at(&context, &encoded, 1_000_000);
        assert!(result.is_err(), "Should error for out-of-bounds call ID");
    }

    #[test]
    fn test_any_call_gas_accounting() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::anyCallCall {
            target,
            selector,
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = any_call(&context, &encoded, 1_000_000).unwrap();

        // BASE_COST + one visited call + one ABI return word.
        assert_eq!(
            result.gas(),
            BASE_COST + PER_CALL_COST + PER_RETURN_WORD_COST
        );
    }

    #[test]
    fn test_all_calls_by_all_match() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        for _ in 0..3 {
            journal.depth = 0;
            let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
            tracer.record_call_start(inputs, &bytes, &mut journal);
            tracer.record_call_end(&mut journal, false);
        }

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::allCallsByCall {
            target,
            selector,
            allowedCaller: caller,
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = all_calls_by(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(decoded, "All calls by same caller should return true");
    }

    #[test]
    fn test_all_calls_by_mixed() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller1 = address!("2222222222222222222222222222222222222222");
        let caller2 = address!("3333333333333333333333333333333333333333");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        // Call from caller1
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller1, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        // Call from caller2
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller2, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::allCallsByCall {
            target,
            selector,
            allowedCaller: caller1,
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = all_calls_by(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(!decoded, "Mixed callers should return false");
    }

    #[test]
    fn test_all_calls_by_no_calls() {
        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::allCallsByCall {
            target: address!("1111111111111111111111111111111111111111"),
            selector: FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]),
            allowedCaller: address!("2222222222222222222222222222222222222222"),
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = all_calls_by(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(
            decoded,
            "Vacuous truth: no matching calls should return true"
        );
    }

    #[test]
    fn test_sum_arg_uint_single() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        // Build calldata: selector + arg0 (value = 42)
        let mut calldata = selector.to_vec();
        calldata.extend_from_slice(&U256::from(42).to_be_bytes::<32>());

        journal.depth = 0;
        let input_bytes: Bytes = calldata.into();
        let inputs = CallInputs {
            input: CallInput::Bytes(input_bytes.clone()),
            return_memory_offset: 0..0,
            gas_limit: 0,
            bytecode_address: target,
            known_bytecode: None,
            target_address: target,
            caller,
            value: CallValue::default(),
            scheme: CallScheme::Call,
            is_static: false,
        };
        tracer.record_call_start(inputs, &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::sumArgUintCall {
            target,
            selector,
            argIndex: U256::from(0),
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = sum_arg_uint(&context, &encoded, 1_000_000).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, U256::from(42));
    }

    #[test]
    fn test_sum_arg_uint_multiple() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        let values = [U256::from(10), U256::from(20), U256::from(30)];
        for value in &values {
            // Build calldata: selector + arg0 (dummy) + arg1 (the value)
            let mut calldata = selector.to_vec();
            calldata.extend_from_slice(&U256::from(0).to_be_bytes::<32>()); // arg0
            calldata.extend_from_slice(&value.to_be_bytes::<32>()); // arg1

            journal.depth = 0;
            let input_bytes: Bytes = calldata.into();
            let inputs = CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: target,
                known_bytecode: None,
                target_address: target,
                caller,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            };
            tracer.record_call_start(inputs, &input_bytes, &mut journal);
            tracer.record_call_end(&mut journal, false);
        }

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::sumArgUintCall {
            target,
            selector,
            argIndex: U256::from(1), // arg1
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = sum_arg_uint(&context, &encoded, 1_000_000).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, U256::from(60)); // 10 + 20 + 30
    }

    #[test]
    fn test_sum_arg_uint_overflow() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        // Two calls with MAX values - should wrap
        for _ in 0..2 {
            let mut calldata = selector.to_vec();
            calldata.extend_from_slice(&U256::MAX.to_be_bytes::<32>());

            journal.depth = 0;
            let input_bytes: Bytes = calldata.into();
            let inputs = CallInputs {
                input: CallInput::Bytes(input_bytes.clone()),
                return_memory_offset: 0..0,
                gas_limit: 0,
                bytecode_address: target,
                known_bytecode: None,
                target_address: target,
                caller,
                value: CallValue::default(),
                scheme: CallScheme::Call,
                is_static: false,
            };
            tracer.record_call_start(inputs, &input_bytes, &mut journal);
            tracer.record_call_end(&mut journal, false);
        }

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::sumArgUintCall {
            target,
            selector,
            argIndex: U256::from(0),
            filter: default_filter(),
        };
        let encoded = input.abi_encode();
        let result = sum_arg_uint(&context, &encoded, 1_000_000).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        // MAX + MAX wraps to MAX - 1
        assert_eq!(decoded, U256::MAX.wrapping_add(U256::MAX));
    }

    #[test]
    fn test_sum_arg_uint_arg_offset_overflow_returns_error() {
        let target = address!("1111111111111111111111111111111111111111");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::sumArgUintCall {
            target,
            selector,
            argIndex: U256::from(usize::MAX),
            filter: default_filter(),
        };

        let encoded = input.abi_encode();
        let result = sum_arg_uint(&context, &encoded, 1_000_000);
        assert!(matches!(
            result,
            Err(CallFactsError::ArgOffsetOverflow { .. })
                | Err(CallFactsError::InvalidArgIndex { .. })
        ));
    }

    #[test]
    fn test_get_trigger_context_with_call() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        journal.depth = 0;
        let (inputs, bytes) = make_call_inputs(target, caller, selector, CallScheme::Call);
        tracer.record_call_start(inputs, &bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let mut context = make_ph_context(&logs_and_traces, &tx_env);
        context.trigger_call_id = Some(0);

        let result = get_trigger_context(&context, 1_000_000).unwrap();
        let decoded = PhEvm::TriggerContext::abi_decode(result.bytes()).unwrap();

        assert_eq!(decoded.callId, U256::ZERO);
        assert_eq!(decoded.caller, caller);
        assert_eq!(decoded.target, target);
        assert_eq!(decoded.codeAddress, target);
        assert_eq!(decoded.selector, selector);
        assert_eq!(decoded.depth, 0);
    }

    #[test]
    fn test_get_trigger_context_no_trigger() {
        let tracer = CallTracer::default();
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let result = get_trigger_context(&context, 1_000_000).unwrap();
        let decoded = PhEvm::TriggerContext::abi_decode(result.bytes()).unwrap();

        assert_eq!(decoded.callId, U256::ZERO);
        assert_eq!(decoded.caller, Address::ZERO);
        assert_eq!(decoded.target, Address::ZERO);
        assert_eq!(decoded.codeAddress, Address::ZERO);
        assert_eq!(decoded.selector, FixedBytes::<4>::ZERO);
        assert_eq!(decoded.depth, 0);
    }
}
