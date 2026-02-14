//! Call-derived scalar predicates and aggregates.
//!
//! These precompiles are intentionally "query style": they let assertions express
//! call-shape invariants declaratively without iterating over raw `getCallInputs()`
//! output in Solidity.

#![allow(clippy::missing_errors_doc)]

use crate::inspectors::{
    phevm::{
        PhEvmContext,
        PhevmOutcome,
    },
    precompiles::{
        BASE_COST,
        MAX_ARRAY_RESPONSE_ITEMS,
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
use std::collections::BTreeSet;

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
    #[error("response has {count} unique targets, exceeds max {max}")]
    TooManyResults { count: usize, max: usize },
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

#[inline]
fn charge_base_cost(gas_left: &mut u64, gas_limit: u64) -> Result<(), CallFactsError> {
    if let Some(rax) = deduct_gas_and_check(gas_left, BASE_COST, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }
    Ok(())
}

#[inline]
fn charge_filter_cost(
    gas_left: &mut u64,
    gas_limit: u64,
    visited: u64,
) -> Result<(), CallFactsError> {
    let filter_cost = visited.saturating_mul(PER_CALL_COST);
    if let Some(rax) = deduct_gas_and_check(gas_left, filter_cost, gas_limit) {
        return Err(CallFactsError::OutOfGas(rax));
    }
    Ok(())
}

#[inline]
fn zero_trigger_context() -> PhEvm::TriggerContext {
    PhEvm::TriggerContext {
        callId: U256::ZERO,
        caller: Address::ZERO,
        target: Address::ZERO,
        codeAddress: Address::ZERO,
        selector: FixedBytes::ZERO,
        depth: 0,
    }
}

#[inline]
fn default_success_filter() -> PhEvm::CallFilter {
    PhEvm::CallFilter {
        callType: 0,
        minDepth: 0,
        maxDepth: 0,
        topLevelOnly: false,
        successOnly: true,
    }
}

/// Maps the CallFilter.callType field to an optional `CallScheme` filter.
/// 0 = any, 1 = CALL, 2 = STATICCALL, 3 = DELEGATECALL, 4 = CALLCODE
pub(crate) fn call_type_to_scheme(call_type: u8) -> Option<CallScheme> {
    match call_type {
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
    // O(1) lookup built by CallTracer at record time.
    tracer
        .target_and_selector_indices
        .get(&crate::inspectors::tracer::TargetAndSelector { target, selector })
        .map(Vec::as_slice)
}

pub(crate) fn call_matches_filter(
    record: &CallRecord,
    depth: u32,
    filter: &PhEvm::CallFilter,
    scheme_filter: Option<CallScheme>,
) -> bool {
    if filter.successOnly {
        // `CallTracer` truncates reverted subtrees at record-time, so retained
        // call records are successful by construction.
    }

    if let Some(scheme) = scheme_filter
        && record.inputs().scheme != scheme
    {
        return false;
    }

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

#[inline]
fn for_each_matching_call(
    tracer: &CallTracer,
    target: Address,
    selector: FixedBytes<4>,
    filter: &PhEvm::CallFilter,
    mut f: impl FnMut(usize, &CallRecord) -> bool,
) -> u64 {
    // This helper centralizes the "index lookup -> depth/filter checks -> visit"
    // pattern used by all scalar call-fact precompiles.
    let mut visited = 0u64;
    let call_records = tracer.call_records();
    let scheme_filter = call_type_to_scheme(filter.callType);

    if let Some(indices) = candidate_call_indices(tracer, target, selector) {
        for &idx in indices {
            visited = visited.saturating_add(1);
            let Some(depth) = tracer.call_depth_at(idx) else {
                continue;
            };
            let record = &call_records[idx];
            if !call_matches_filter(record, depth, filter, scheme_filter) {
                continue;
            }
            // Visitor can short-circuit (e.g. anyCall/allCallsBy violation).
            if !f(idx, record) {
                break;
            }
        }
    }

    visited
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

    charge_base_cost(&mut gas_left, gas_limit)?;

    let selector: [u8; 4] = input_bytes
        .get(0..4)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_default();
    // Support both overloads:
    // - anyCall(target, selector, filter)
    // - anyCall(target, selector) [defaults to successOnly]
    let (target, call_selector, filter) = if selector == PhEvm::anyCall_1Call::SELECTOR {
        let call =
            PhEvm::anyCall_1Call::abi_decode(input_bytes).map_err(CallFactsError::DecodeError)?;
        (call.target, call.selector, default_success_filter())
    } else {
        let call =
            PhEvm::anyCall_0Call::abi_decode(input_bytes).map_err(CallFactsError::DecodeError)?;
        (call.target, call.selector, call.filter)
    };

    let tracer = ph_context.logs_and_traces.call_traces;
    let mut found = false;
    let visited = for_each_matching_call(tracer, target, call_selector, &filter, |_, _| {
        found = true;
        false
    });

    charge_filter_cost(&mut gas_left, gas_limit, visited)?;
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

    charge_base_cost(&mut gas_left, gas_limit)?;

    let selector: [u8; 4] = input_bytes
        .get(0..4)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_default();
    // Support overload with default `successOnly` filter.
    let (target, call_selector, filter) = if selector == PhEvm::countCalls_1Call::SELECTOR {
        let call = PhEvm::countCalls_1Call::abi_decode(input_bytes)
            .map_err(CallFactsError::DecodeError)?;
        (call.target, call.selector, default_success_filter())
    } else {
        let call = PhEvm::countCalls_0Call::abi_decode(input_bytes)
            .map_err(CallFactsError::DecodeError)?;
        (call.target, call.selector, call.filter)
    };

    let tracer = ph_context.logs_and_traces.call_traces;
    let mut matched = 0u64;
    let visited = for_each_matching_call(tracer, target, call_selector, &filter, |_, _| {
        matched = matched.saturating_add(1);
        true
    });

    charge_filter_cost(&mut gas_left, gas_limit, visited)?;
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

    charge_base_cost(&mut gas_left, gas_limit)?;

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

    charge_base_cost(&mut gas_left, gas_limit)?;

    let selector: [u8; 4] = input_bytes
        .get(0..4)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_default();
    // Support overload with default `successOnly` filter.
    let (target, call_selector, allowed_caller, filter) =
        if selector == PhEvm::allCallsBy_1Call::SELECTOR {
            let call = PhEvm::allCallsBy_1Call::abi_decode(input_bytes)
                .map_err(CallFactsError::DecodeError)?;
            (
                call.target,
                call.selector,
                call.allowedCaller,
                default_success_filter(),
            )
        } else {
            let call = PhEvm::allCallsBy_0Call::abi_decode(input_bytes)
                .map_err(CallFactsError::DecodeError)?;
            (call.target, call.selector, call.allowedCaller, call.filter)
        };

    let tracer = ph_context.logs_and_traces.call_traces;
    let mut result = true;
    let visited = for_each_matching_call(tracer, target, call_selector, &filter, |_, record| {
        if record.inputs().caller != allowed_caller {
            result = false;
            return false;
        }
        true
    });

    charge_filter_cost(&mut gas_left, gas_limit, visited)?;
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

    charge_base_cost(&mut gas_left, gas_limit)?;

    let selector: [u8; 4] = input_bytes
        .get(0..4)
        .and_then(|bytes| bytes.try_into().ok())
        .unwrap_or_default();
    // Support overload with default `successOnly` filter.
    let (target, call_selector, arg_index_raw, filter) =
        if selector == PhEvm::sumArgUint_1Call::SELECTOR {
            let call = PhEvm::sumArgUint_1Call::abi_decode(input_bytes)
                .map_err(CallFactsError::DecodeError)?;
            (
                call.target,
                call.selector,
                call.argIndex,
                default_success_filter(),
            )
        } else {
            let call = PhEvm::sumArgUint_0Call::abi_decode(input_bytes)
                .map_err(CallFactsError::DecodeError)?;
            (call.target, call.selector, call.argIndex, call.filter)
        };

    let tracer = ph_context.logs_and_traces.call_traces;
    let arg_index: usize = arg_index_raw.try_into().map_err(|_| {
        CallFactsError::InvalidArgIndex {
            arg_index: arg_index_raw,
        }
    })?;
    let byte_offset = arg_index
        .checked_mul(32)
        .ok_or(CallFactsError::ArgOffsetOverflow { arg_index })?;
    // 4-byte function selector prefix + ABI word offset.
    let data_start = 4usize
        .checked_add(byte_offset)
        .ok_or(CallFactsError::ArgOffsetOverflow { arg_index })?;
    let data_end = data_start
        .checked_add(32)
        .ok_or(CallFactsError::ArgOffsetOverflow { arg_index })?;

    let mut total = U256::ZERO;
    let visited = for_each_matching_call(tracer, target, call_selector, &filter, |_, record| {
        let revm::interpreter::CallInput::Bytes(calldata) = &record.inputs().input else {
            return true;
        };

        // Skip the 4-byte selector, then read 32 bytes at the arg offset.
        if calldata.len() < data_end {
            // Calldata too short for this argIndex - zero-extend.
            let mut padded = [0u8; 32];
            let available = calldata.len().saturating_sub(data_start);
            if available > 0 {
                padded[..available].copy_from_slice(&calldata[data_start..data_start + available]);
            }
            total = total.wrapping_add(U256::from_be_bytes(padded));
        } else {
            let word: [u8; 32] = calldata[data_start..data_end]
                .try_into()
                .unwrap_or_default();
            total = total.wrapping_add(U256::from_be_bytes(word));
        }
        true
    });

    charge_filter_cost(&mut gas_left, gas_limit, visited)?;
    charge_return_words(&mut gas_left, gas_limit, SCALAR_RETURN_WORDS)?;

    let encoded = total.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `getTouchedContracts(CallFilter filter) -> address[]`
///
/// Returns unique target addresses touched by calls that match the filter.
pub fn get_touched_contracts(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, CallFactsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    charge_base_cost(&mut gas_left, gas_limit)?;

    let call = PhEvm::getTouchedContractsCall::abi_decode(input_bytes)
        .map_err(CallFactsError::DecodeError)?;
    let filter = call.filter;
    let scheme_filter = call_type_to_scheme(filter.callType);
    let tracer = ph_context.logs_and_traces.call_traces;
    let mut visited = 0u64;
    let mut targets = BTreeSet::new();

    for (idx, record) in tracer.call_records().iter().enumerate() {
        visited = visited.saturating_add(1);
        let Some(depth) = tracer.call_depth_at(idx) else {
            continue;
        };
        if !call_matches_filter(record, depth, &filter, scheme_filter) {
            continue;
        }

        let target = record.inputs().target_address;
        // Bound array-returning APIs to avoid pathological response payloads.
        if !targets.contains(&target) && targets.len() >= MAX_ARRAY_RESPONSE_ITEMS {
            return Err(CallFactsError::TooManyResults {
                count: targets.len() + 1,
                max: MAX_ARRAY_RESPONSE_ITEMS,
            });
        }
        targets.insert(target);
    }

    charge_filter_cost(&mut gas_left, gas_limit, visited)?;

    let encoded = targets.into_iter().collect::<Vec<_>>().abi_encode();
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

    charge_base_cost(&mut gas_left, gas_limit)?;

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
                    depth: tracer.call_depth_at(call_id).unwrap_or(0),
                }
            } else {
                // Defensive fallback: executor set a call id that no longer exists.
                // We keep this non-throwing and return zero context for robustness.
                zero_trigger_context()
            }
        }
        // Non-call triggers (storage/balance) intentionally yield zero context.
        None => zero_trigger_context(),
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
            successOnly: false,
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

        let input = PhEvm::anyCall_0Call {
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

        let input = PhEvm::anyCall_0Call {
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
        let input = PhEvm::anyCall_0Call {
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

        let input = PhEvm::countCalls_0Call {
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
        let input = PhEvm::countCalls_0Call {
            target,
            selector,
            filter: PhEvm::CallFilter {
                callType: 3,
                minDepth: 0,
                maxDepth: 0,
                topLevelOnly: false,
                successOnly: false,
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
        let input = PhEvm::countCalls_0Call {
            target,
            selector,
            filter: PhEvm::CallFilter {
                callType: 0,
                minDepth: 1,
                maxDepth: 0,
                topLevelOnly: false,
                successOnly: false,
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

        let input = PhEvm::anyCall_0Call {
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

        let input = PhEvm::allCallsBy_0Call {
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

        let input = PhEvm::allCallsBy_0Call {
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

        let input = PhEvm::allCallsBy_0Call {
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

        let input = PhEvm::sumArgUint_0Call {
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

        let input = PhEvm::sumArgUint_0Call {
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

        let input = PhEvm::sumArgUint_0Call {
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

        let input = PhEvm::sumArgUint_0Call {
            target,
            selector,
            argIndex: U256::from(usize::MAX),
            filter: default_filter(),
        };

        let encoded = input.abi_encode();
        let result = sum_arg_uint(&context, &encoded, 1_000_000);
        assert!(matches!(
            result,
            Err(CallFactsError::ArgOffsetOverflow { .. } | CallFactsError::InvalidArgIndex { .. })
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

    #[test]
    fn test_any_call_default_overload_uses_no_filter_signature() {
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

        let input = PhEvm::anyCall_1Call { target, selector };
        let encoded = input.abi_encode();
        let result = any_call(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(decoded);
    }

    #[test]
    fn test_count_calls_default_overload_uses_no_filter_signature() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();
        for _ in 0..2 {
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

        let input = PhEvm::countCalls_1Call { target, selector };
        let encoded = input.abi_encode();
        let result = count_calls(&context, &encoded, 1_000_000).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, U256::from(2));
    }

    #[test]
    fn test_all_calls_by_default_overload_uses_no_filter_signature() {
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

        let input = PhEvm::allCallsBy_1Call {
            target,
            selector,
            allowedCaller: caller,
        };
        let encoded = input.abi_encode();
        let result = all_calls_by(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(decoded);
    }

    #[test]
    fn test_sum_arg_uint_default_overload_uses_no_filter_signature() {
        let target = address!("1111111111111111111111111111111111111111");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        // selector + arg0 = 7
        let mut calldata = selector.to_vec();
        calldata.extend_from_slice(&U256::from(7).to_be_bytes::<32>());
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

        journal.depth = 0;
        tracer.record_call_start(inputs, &input_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::sumArgUint_1Call {
            target,
            selector,
            argIndex: U256::ZERO,
        };
        let encoded = input.abi_encode();
        let result = sum_arg_uint(&context, &encoded, 1_000_000).unwrap();
        let decoded = U256::abi_decode(result.bytes()).unwrap();
        assert_eq!(decoded, U256::from(7));
    }

    #[test]
    fn test_get_touched_contracts_filters_by_depth() {
        let target_a = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let target_b = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let caller = address!("2222222222222222222222222222222222222222");
        let selector = FixedBytes::<4>::from([0xde, 0xad, 0xbe, 0xef]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        // Root call to A
        journal.depth = 0;
        let (root_inputs, root_bytes) =
            make_call_inputs(target_a, caller, selector, CallScheme::Call);
        tracer.record_call_start(root_inputs, &root_bytes, &mut journal);

        // Nested call to B
        journal.depth = 1;
        let (nested_inputs, nested_bytes) =
            make_call_inputs(target_b, caller, selector, CallScheme::Call);
        tracer.record_call_start(nested_inputs, &nested_bytes, &mut journal);
        tracer.record_call_end(&mut journal, false);

        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::getTouchedContractsCall {
            filter: PhEvm::CallFilter {
                callType: 0,
                minDepth: 1,
                maxDepth: 0,
                topLevelOnly: false,
                successOnly: false,
            },
        };
        let encoded = input.abi_encode();
        let result = get_touched_contracts(&context, &encoded, 1_000_000).unwrap();
        let decoded = <Vec<Address>>::abi_decode(result.bytes()).unwrap();

        assert_eq!(decoded, vec![target_b]);
    }
}
