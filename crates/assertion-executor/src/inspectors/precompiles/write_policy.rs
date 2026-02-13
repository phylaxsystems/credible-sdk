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

const PER_ENTRY_COST: u64 = 5;

/// `anySlotWritten(address target, bytes32 slot) -> bool`
///
/// Checks the storage change index for any `StorageChanged` matching the target and slot.
/// Returns true if any matching storage change was found. O(1) via pre-built index.
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

    let index = ph_context.logs_and_traces.call_traces.storage_change_index();

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

    let mut result = true;

    // Use the index to get only matching storage changes, then attribute each to a call
    if let Some(entries) = index.changes_for_key(&target, &slot) {
        for entry in entries {
            let caller_of_write =
                call_at_journal_position(call_records, entry.journal_idx);

            match caller_of_write {
                Some(record) => {
                    if record.inputs().caller != allowed_caller {
                        result = false;
                        break;
                    }
                }
                None => {
                    // Write occurred outside any tracked call (e.g., at top level tx)
                    result = false;
                    break;
                }
            }
        }
    }

    let encoded = result.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// Finds the innermost call whose checkpoint range contains the given journal index.
///
/// Iterates from latest to earliest call record. The latest record that contains
/// `journal_idx` is the innermost writer context for that journal position.
fn call_at_journal_position(
    call_records: &[crate::inspectors::tracer::CallRecord],
    journal_idx: usize,
) -> Option<&crate::inspectors::tracer::CallRecord> {
    for record in call_records.iter().rev() {
        let pre = record.pre_call_checkpoint().journal_i;
        if pre > journal_idx {
            continue;
        }

        // Check post_call_checkpoint if available
        if let Some(post) = record.post_call_checkpoint() {
            if post.journal_i <= journal_idx {
                continue;
            }
        }
        // First matching record in reverse start order is innermost.
        return Some(record);
    }

    None
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

        // Record a call from the allowed caller
        journal.depth = 0;
        let input_bytes: Bytes = selector.into();
        let inputs = CallInputs {
            input: CallInput::Bytes(input_bytes.clone()),
            return_memory_offset: 0..0,
            gas_limit: 0,
            bytecode_address: contract,
            known_bytecode: None,
            target_address: contract,
            caller: allowed_caller,
            value: CallValue::default(),
            scheme: CallScheme::Call,
            is_static: false,
        };
        tracer.record_call_start(inputs, &input_bytes, &mut journal);

        // The storage change happens during this call — push to BOTH journals
        // so checkpoints and scanning see consistent data.
        let entry = JournalEntry::StorageChanged {
            address: contract,
            key: slot,
            had_value: U256::ZERO,
        };
        journal.journal.push(entry.clone());
        tracer.journal.journal.push(entry);

        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::allSlotWritesByCall {
            target: contract,
            slot: B256::from(slot),
            allowedCaller: allowed_caller,
        };
        let encoded = input.abi_encode();
        let result = all_slot_writes_by(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(decoded, "All writes by allowed caller should return true");
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

        // Record a call from an unauthorized caller
        journal.depth = 0;
        let input_bytes: Bytes = selector.into();
        let inputs = CallInputs {
            input: CallInput::Bytes(input_bytes.clone()),
            return_memory_offset: 0..0,
            gas_limit: 0,
            bytecode_address: contract,
            known_bytecode: None,
            target_address: contract,
            caller: unauthorized,
            value: CallValue::default(),
            scheme: CallScheme::Call,
            is_static: false,
        };
        tracer.record_call_start(inputs, &input_bytes, &mut journal);

        // Storage change during unauthorized call — push to both journals
        let entry = JournalEntry::StorageChanged {
            address: contract,
            key: slot,
            had_value: U256::ZERO,
        };
        journal.journal.push(entry.clone());
        tracer.journal.journal.push(entry);

        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::allSlotWritesByCall {
            target: contract,
            slot: B256::from(slot),
            allowedCaller: allowed_caller,
        };
        let encoded = input.abi_encode();
        let result = all_slot_writes_by(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(!decoded, "Unauthorized caller should return false");
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
        journal.depth = 0;

        // Parent call starts at journal index 0.
        let parent_input: Bytes = selector_parent.into();
        let parent = CallInputs {
            input: CallInput::Bytes(parent_input.clone()),
            return_memory_offset: 0..0,
            gas_limit: 0,
            bytecode_address: contract,
            known_bytecode: None,
            target_address: contract,
            caller: allowed_caller,
            value: CallValue::default(),
            scheme: CallScheme::Call,
            is_static: false,
        };
        tracer.record_call_start(parent, &parent_input, &mut journal);

        // Child call starts immediately, so it shares the same pre checkpoint as parent.
        journal.depth = 1;
        let child_input: Bytes = selector_child.into();
        let child = CallInputs {
            input: CallInput::Bytes(child_input.clone()),
            return_memory_offset: 0..0,
            gas_limit: 0,
            bytecode_address: contract,
            known_bytecode: None,
            target_address: contract,
            caller: unauthorized,
            value: CallValue::default(),
            scheme: CallScheme::Call,
            is_static: false,
        };
        tracer.record_call_start(child, &child_input, &mut journal);

        // Storage write is performed inside child context.
        let entry = JournalEntry::StorageChanged {
            address: contract,
            key: slot,
            had_value: U256::ZERO,
        };
        journal.journal.push(entry.clone());
        tracer.journal.journal.push(entry);

        // Close child then parent.
        tracer.record_call_end(&mut journal, false);
        journal.depth = 0;
        tracer.record_call_end(&mut journal, false);

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();
        let context = make_ph_context(&logs_and_traces, &tx_env);

        let input = PhEvm::allSlotWritesByCall {
            target: contract,
            slot: B256::from(slot),
            allowedCaller: allowed_caller,
        };
        let encoded = input.abi_encode();
        let result = all_slot_writes_by(&context, &encoded, 1_000_000).unwrap();
        let decoded = bool::abi_decode(result.bytes()).unwrap();
        assert!(
            !decoded,
            "Write must be attributed to innermost unauthorized caller"
        );
    }
}
