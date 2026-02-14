//! Storage-write policy predicates.
//!
//! These precompiles intentionally expose boolean policy checks (`any`, `all`).
//! Value-oriented queries belong to `getSlotDiff`/`load`.

#![allow(clippy::missing_errors_doc)]

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
};

use alloy_primitives::U256;
use alloy_sol_types::{
    SolCall,
    SolValue,
};

#[derive(thiserror::Error, Debug)]
pub enum WritePolicyError {
    #[error("Failed to decode write policy input: {0:?}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

/// Per-journal-entry scan charge for write-policy predicates.
///
/// The shared storage index amortizes scan work across precompiles; this charge keeps
/// gas proportional to trace size and aligned with prior behavior.
const PER_ENTRY_COST: u64 = 5;

/// `anySlotWritten(address target, bytes32 slot) -> bool`
///
/// Checks the storage change index for any `StorageChanged` matching the target and slot.
/// Returns true if any matching storage change was found. O(1) via pre-built index.
///
/// This intentionally returns a predicate (not a value). Use `getSlotDiff`/`load`
/// when assertions need the actual slot contents.
pub fn any_slot_written(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, WritePolicyError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(WritePolicyError::OutOfGas(rax));
    }

    let call = PhEvm::anySlotWrittenCall::abi_decode(input_bytes)
        .map_err(WritePolicyError::DecodeError)?;

    let target = call.target;
    let slot: U256 = call.slot.into();

    let index = ph_context
        .logs_and_traces
        .call_traces
        .storage_change_index();

    // Charge gas proportional to journal size (same cost model)
    let journal_len = ph_context.logs_and_traces.call_traces.journal.journal.len() as u64;
    let scan_cost = journal_len.saturating_mul(PER_ENTRY_COST) / 100;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, scan_cost, gas_limit) {
        return Err(WritePolicyError::OutOfGas(rax));
    }

    let found = index.has_changes(&target, &slot);

    let encoded = found.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `allSlotWritesBy(address target, bytes32 slot, address allowedCaller) -> bool`
///
/// Uses the storage change index to find matching `StorageChanged` entries,
/// then attributes each write to the innermost active call and checks
/// that its caller matches `allowedCaller`.
///
/// Returns true if no writes occurred, or all writes were by the allowed caller (vacuous truth).
/// Name is predicate-oriented ("all ... by") rather than collection-oriented.
pub fn all_slot_writes_by(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, WritePolicyError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(WritePolicyError::OutOfGas(rax));
    }

    let call = PhEvm::allSlotWritesByCall::abi_decode(input_bytes)
        .map_err(WritePolicyError::DecodeError)?;

    let target = call.target;
    let slot: U256 = call.slot.into();
    let allowed_caller = call.allowedCaller;

    let tracer = ph_context.logs_and_traces.call_traces;
    let index = tracer.storage_change_index();
    let call_records = tracer.call_records();

    // Charge gas for journal scan
    let journal_len = tracer.journal.journal.len() as u64;
    let scan_cost = journal_len.saturating_mul(PER_ENTRY_COST) / 100;
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, scan_cost, gas_limit) {
        return Err(WritePolicyError::OutOfGas(rax));
    }

    // Use the index to get only matching storage changes, then attribute each to a call
    let result = index.changes_for_key(&target, &slot).is_none_or(|entries| {
        all_matching_writes_by_allowed_caller(entries, call_records, allowed_caller)
    });

    let encoded = result.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// Checks whether all matching writes are attributed to `allowed_caller`.
///
/// Runs in `O(call_records` log `call_records` + `matching_writes`) by sweeping sorted call
/// start/end checkpoints once while iterating matching writes in journal order.
fn all_matching_writes_by_allowed_caller(
    entries: &[crate::inspectors::tracer::StorageChangeEntry],
    call_records: &[crate::inspectors::tracer::CallRecord],
    allowed_caller: alloy_primitives::Address,
) -> bool {
    if entries.is_empty() {
        return true;
    }

    // Build deterministic traversal orders:
    // - start_order: calls by increasing pre-checkpoint (activation order)
    // - end_order: calls by increasing post-checkpoint (deactivation order)
    let mut start_order: Vec<usize> = call_records
        .iter()
        .enumerate()
        .filter_map(|(idx, record)| record.post_call_checkpoint().map(|_| idx))
        .collect();
    start_order.sort_by_key(|&idx| call_records[idx].pre_call_checkpoint().journal_i);

    let mut end_order = start_order.clone();
    end_order.sort_by_key(|&idx| {
        call_records[idx]
            .post_call_checkpoint()
            .map_or(usize::MAX, |checkpoint| checkpoint.journal_i)
    });

    // Stack of currently-active calls. By push-on-start and pop-on-end, the
    // tail is always the innermost call for current journal position.
    let mut active_calls: Vec<usize> = Vec::new();
    let mut start_i = 0usize;
    let mut end_i = 0usize;

    for entry in entries {
        let journal_idx = entry.journal_idx;

        // First remove calls whose post-checkpoint is <= current write position.
        // This guarantees inactive calls do not participate in attribution.
        while let Some(&call_idx) = end_order.get(end_i) {
            let Some(post_checkpoint) = call_records[call_idx].post_call_checkpoint() else {
                end_i += 1;
                continue;
            };
            if post_checkpoint.journal_i > journal_idx {
                break;
            }

            // Fast path: expected LIFO close.
            if active_calls.last().copied() == Some(call_idx) {
                active_calls.pop();
            } else if let Some(pos) = active_calls
                .iter()
                .rposition(|&active_idx| active_idx == call_idx)
            {
                // Defensive path for irregular checkpoint layouts.
                active_calls.remove(pos);
            }
            end_i += 1;
        }

        // Then activate calls whose pre-checkpoint is <= current write position and
        // whose post-checkpoint is still in the future.
        while let Some(&call_idx) = start_order.get(start_i) {
            let record = &call_records[call_idx];
            let pre = record.pre_call_checkpoint().journal_i;
            if pre > journal_idx {
                break;
            }

            if let Some(post_checkpoint) = record.post_call_checkpoint()
                && post_checkpoint.journal_i > journal_idx
            {
                active_calls.push(call_idx);
            }
            start_i += 1;
        }

        // If no active call covers this write, attribution is undefined: reject.
        let Some(&innermost_call_idx) = active_calls.last() else {
            return false;
        };

        // Enforce policy on the attributed writer.
        if call_records[innermost_call_idx].inputs().caller != allowed_caller {
            return false;
        }
    }

    true
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
        primitives::JournalEntry,
    };
    use alloy_primitives::{
        Address,
        B256,
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

    fn make_ph_context<'a>(
        logs_and_traces: &'a LogsAndTraces<'a>,
        tx_env: &'a crate::primitives::TxEnv,
    ) -> PhEvmContext<'a> {
        PhEvmContext::new(logs_and_traces, Address::ZERO, tx_env)
    }

    fn make_call_inputs(contract: Address, caller: Address, input_bytes: &Bytes) -> CallInputs {
        CallInputs {
            input: CallInput::Bytes(input_bytes.clone()),
            return_memory_offset: 0..0,
            gas_limit: 0,
            bytecode_address: contract,
            known_bytecode: None,
            target_address: contract,
            caller,
            value: CallValue::default(),
            scheme: CallScheme::Call,
            is_static: false,
        }
    }

    fn record_call_start(
        tracer: &mut CallTracer,
        journal: &mut JournalInner<JournalEntry>,
        contract: Address,
        selector: alloy_primitives::FixedBytes<4>,
        caller: Address,
        depth: usize,
    ) {
        journal.depth = depth;
        let input_bytes: Bytes = selector.into();
        let inputs = make_call_inputs(contract, caller, &input_bytes);
        tracer.record_call_start(inputs, &input_bytes, journal);
    }

    fn push_storage_change(
        tracer: &mut CallTracer,
        journal: &mut JournalInner<JournalEntry>,
        address: Address,
        slot: U256,
    ) {
        let entry = JournalEntry::StorageChanged {
            address,
            key: slot,
            had_value: U256::ZERO,
        };
        journal.journal.push(entry.clone());
        tracer.journal.journal.push(entry);
    }

    fn eval_all_slot_writes_by(
        tracer: &CallTracer,
        target: Address,
        slot: U256,
        allowed_caller: Address,
    ) -> bool {
        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::allSlotWritesByCall {
            target,
            slot: B256::from(slot),
            allowedCaller: allowed_caller,
        };
        let encoded = input.abi_encode();
        let result = all_slot_writes_by(&context, &encoded, 1_000_000).unwrap();
        bool::abi_decode(result.bytes()).unwrap()
    }

    #[test]
    fn test_any_slot_written_true() {
        let contract = address!("1111111111111111111111111111111111111111");
        let slot = U256::from(42);

        let mut tracer = CallTracer::default();
        tracer.journal.journal.push(JournalEntry::StorageChanged {
            address: contract,
            key: slot,
            had_value: U256::ZERO,
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::anySlotWrittenCall {
            target: contract,
            slot: B256::from(slot),
        };
        let encoded = input.abi_encode();
        let result = any_slot_written(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(decoded, "Should find matching storage change");
    }

    #[test]
    fn test_any_slot_written_false() {
        let contract = address!("1111111111111111111111111111111111111111");
        let slot = U256::from(42);
        let other_slot = U256::from(99);

        let mut tracer = CallTracer::default();
        tracer.journal.journal.push(JournalEntry::StorageChanged {
            address: contract,
            key: other_slot,
            had_value: U256::ZERO,
        });

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::anySlotWrittenCall {
            target: contract,
            slot: B256::from(slot),
        };
        let encoded = input.abi_encode();
        let result = any_slot_written(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(!decoded, "Should not find matching storage change");
    }

    #[test]
    fn test_all_slot_writes_by_all_match() {
        let contract = address!("1111111111111111111111111111111111111111");
        let allowed_caller = address!("2222222222222222222222222222222222222222");
        let slot = U256::from(42);
        let selector = alloy_primitives::FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        record_call_start(
            &mut tracer,
            &mut journal,
            contract,
            selector,
            allowed_caller,
            0,
        );
        push_storage_change(&mut tracer, &mut journal, contract, slot);

        tracer.record_call_end(&mut journal, false);

        assert!(
            eval_all_slot_writes_by(&tracer, contract, slot, allowed_caller),
            "All writes by allowed caller should return true"
        );
    }

    #[test]
    fn test_all_slot_writes_by_unauthorized() {
        let contract = address!("1111111111111111111111111111111111111111");
        let allowed_caller = address!("2222222222222222222222222222222222222222");
        let unauthorized = address!("3333333333333333333333333333333333333333");
        let slot = U256::from(42);
        let selector = alloy_primitives::FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        record_call_start(
            &mut tracer,
            &mut journal,
            contract,
            selector,
            unauthorized,
            0,
        );
        push_storage_change(&mut tracer, &mut journal, contract, slot);

        tracer.record_call_end(&mut journal, false);

        assert!(
            !eval_all_slot_writes_by(&tracer, contract, slot, allowed_caller),
            "Unauthorized caller should return false"
        );
    }

    #[test]
    fn test_all_slot_writes_by_nested_same_pre_attributed_to_innermost() {
        let contract = address!("1111111111111111111111111111111111111111");
        let allowed_caller = address!("2222222222222222222222222222222222222222");
        let unauthorized = address!("3333333333333333333333333333333333333333");
        let slot = U256::from(42);
        let selector_parent = alloy_primitives::FixedBytes::<4>::from([0x12, 0x34, 0x56, 0x78]);
        let selector_child = alloy_primitives::FixedBytes::<4>::from([0x90, 0xab, 0xcd, 0xef]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        // Parent call starts at journal index 0.
        record_call_start(
            &mut tracer,
            &mut journal,
            contract,
            selector_parent,
            allowed_caller,
            0,
        );

        // Child call starts immediately, so it shares the same pre checkpoint as parent.
        record_call_start(
            &mut tracer,
            &mut journal,
            contract,
            selector_child,
            unauthorized,
            1,
        );

        // Storage write is performed inside child context.
        push_storage_change(&mut tracer, &mut journal, contract, slot);

        // Close child then parent.
        tracer.record_call_end(&mut journal, false);
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        assert!(
            !eval_all_slot_writes_by(&tracer, contract, slot, allowed_caller),
            "Write must be attributed to innermost unauthorized caller"
        );
    }

    #[test]
    fn test_all_slot_writes_by_multiple_writes_mixed_callers() {
        let contract = address!("1111111111111111111111111111111111111111");
        let allowed_caller = address!("2222222222222222222222222222222222222222");
        let unauthorized = address!("3333333333333333333333333333333333333333");
        let slot = U256::from(42);
        let selector_a = alloy_primitives::FixedBytes::<4>::from([0x11, 0x11, 0x11, 0x11]);
        let selector_b = alloy_primitives::FixedBytes::<4>::from([0x22, 0x22, 0x22, 0x22]);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        record_call_start(
            &mut tracer,
            &mut journal,
            contract,
            selector_a,
            allowed_caller,
            0,
        );
        push_storage_change(&mut tracer, &mut journal, contract, slot);
        tracer.record_call_end(&mut journal, false);

        record_call_start(
            &mut tracer,
            &mut journal,
            contract,
            selector_b,
            unauthorized,
            0,
        );
        push_storage_change(&mut tracer, &mut journal, contract, slot);
        tracer.record_call_end(&mut journal, false);

        assert!(
            !eval_all_slot_writes_by(&tracer, contract, slot, allowed_caller),
            "Mixed callers writing same slot must fail policy"
        );
    }

    #[test]
    fn test_all_slot_writes_by_write_outside_any_call() {
        let contract = address!("1111111111111111111111111111111111111111");
        let allowed_caller = address!("2222222222222222222222222222222222222222");
        let slot = U256::from(42);

        let mut tracer = CallTracer::default();
        let mut journal = JournalInner::new();

        push_storage_change(&mut tracer, &mut journal, contract, slot);

        assert!(
            !eval_all_slot_writes_by(&tracer, contract, slot, allowed_caller),
            "Writes outside tracked calls must fail policy"
        );
    }
}
