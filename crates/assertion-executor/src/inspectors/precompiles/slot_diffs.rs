use crate::{
    inspectors::{
        phevm::{
            PhEvmContext,
            PhevmOutcome,
        },
        precompiles::{
            BASE_COST,
            deduct_gas_and_check,
        },
        sol_primitives::PhEvm,
    },
    primitives::{
        JournalEntry,
        U256,
    },
};

use alloy_primitives::{
    FixedBytes,
    keccak256,
};
use alloy_sol_types::{
    SolCall,
    SolValue,
};
use std::collections::HashMap;

#[derive(thiserror::Error, Debug)]
pub enum SlotDiffsError {
    #[error("Failed to decode slot diffs input: {0:?}")]
    DecodeError(#[source] alloy_sol_types::Error),
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

const PER_JOURNAL_ENTRY_COST: u64 = 16;

/// Compute the storage slot for a Solidity mapping key.
///
/// For `mapping(KeyType => ValueType)` at base slot `baseSlot`,
/// the storage position for key `k` is `keccak256(k ++ baseSlot) + fieldOffset`.
fn compute_mapping_slot(
    base_slot: FixedBytes<32>,
    key: FixedBytes<32>,
    field_offset: U256,
) -> U256 {
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(key.as_slice());
    input[32..].copy_from_slice(base_slot.as_slice());
    let hash = keccak256(input);
    let base = U256::from_be_bytes(hash.0);
    base.wrapping_add(field_offset)
}

/// Scan the journal for pre/post values of a specific slot.
///
/// Returns `(pre_value, post_value, changed)`:
/// - `pre_value`: the value before the first write (first `had_value` in journal)
/// - `post_value`: the current value from `journal.state`
/// - `changed`: whether pre != post
fn get_slot_pre_post(
    context: &PhEvmContext,
    target: alloy_primitives::Address,
    slot: U256,
) -> (U256, U256, bool) {
    let journal = context.post_tx_journal();

    // Find the first StorageChanged entry for this target+slot (= original value)
    let mut first_had_value: Option<U256> = None;
    for entry in &journal.journal {
        if let JournalEntry::StorageChanged {
            address,
            key,
            had_value,
        } = entry
        {
            if *address == target && *key == slot {
                first_had_value = Some(*had_value);
                break;
            }
        }
    }

    match first_had_value {
        Some(pre) => {
            let post = journal
                .state
                .get(&target)
                .and_then(|account| account.storage.get(&slot))
                .map(|s| s.present_value)
                .unwrap_or(pre);
            (pre, post, pre != post)
        }
        None => {
            // Not changed — return loaded value or zero
            let value = journal
                .state
                .get(&target)
                .and_then(|account| account.storage.get(&slot))
                .map(|s| s.present_value)
                .unwrap_or(U256::ZERO);
            (value, value, false)
        }
    }
}

/// `getChangedSlots(address target) -> bytes32[]`
///
/// Returns all storage slot keys that were modified during the transaction
/// for the given target address. Only includes slots where pre != post.
pub fn get_changed_slots(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, SlotDiffsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(SlotDiffsError::OutOfGas(rax));
    }

    let call = PhEvm::getChangedSlotsCall::abi_decode(input_bytes)
        .map_err(SlotDiffsError::DecodeError)?;

    let journal = ph_context.post_tx_journal();

    // Collect first had_value for each slot that was written
    let mut first_had_value: HashMap<U256, U256> = HashMap::new();
    for entry in &journal.journal {
        if let Some(rax) =
            deduct_gas_and_check(&mut gas_left, PER_JOURNAL_ENTRY_COST, gas_limit)
        {
            return Err(SlotDiffsError::OutOfGas(rax));
        }

        if let JournalEntry::StorageChanged {
            address,
            key,
            had_value,
        } = entry
        {
            if *address == call.target {
                first_had_value.entry(*key).or_insert(*had_value);
            }
        }
    }

    // Filter to truly changed slots (pre != post)
    let mut changed_slots: Vec<FixedBytes<32>> = Vec::new();
    if let Some(account) = journal.state.get(&call.target) {
        for (slot, pre_value) in &first_had_value {
            let post_value = account
                .storage
                .get(slot)
                .map(|s| s.present_value)
                .unwrap_or(*pre_value);
            if *pre_value != post_value {
                changed_slots.push(FixedBytes::<32>::from(slot.to_be_bytes::<32>()));
            }
        }
    }

    // Sort for deterministic output
    changed_slots.sort();

    let encoded = changed_slots.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `getSlotDiff(address target, bytes32 slot) -> (bytes32 pre, bytes32 post, bool changed)`
///
/// Returns the pre-tx and post-tx values of a storage slot.
pub fn get_slot_diff(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, SlotDiffsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(SlotDiffsError::OutOfGas(rax));
    }

    let call = PhEvm::getSlotDiffCall::abi_decode(input_bytes)
        .map_err(SlotDiffsError::DecodeError)?;

    let slot: U256 = call.slot.into();

    // Charge gas for journal scan
    let journal_len = ph_context.post_tx_journal().journal.len() as u64;
    let scan_cost = journal_len.saturating_mul(PER_JOURNAL_ENTRY_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, scan_cost, gas_limit) {
        return Err(SlotDiffsError::OutOfGas(rax));
    }

    let (pre, post, changed) = get_slot_pre_post(ph_context, call.target, slot);

    let pre_bytes = FixedBytes::<32>::from(pre.to_be_bytes::<32>());
    let post_bytes = FixedBytes::<32>::from(post.to_be_bytes::<32>());
    let encoded = (pre_bytes, post_bytes, changed).abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `didMappingKeyChange(address target, bytes32 baseSlot, bytes32 key, uint256 fieldOffset) -> bool`
///
/// Computes the Solidity mapping storage slot and checks if it was modified.
pub fn did_mapping_key_change(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, SlotDiffsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(SlotDiffsError::OutOfGas(rax));
    }

    let call = PhEvm::didMappingKeyChangeCall::abi_decode(input_bytes)
        .map_err(SlotDiffsError::DecodeError)?;

    let slot = compute_mapping_slot(call.baseSlot, call.key, call.fieldOffset);

    // Charge gas for journal scan
    let journal_len = ph_context.post_tx_journal().journal.len() as u64;
    let scan_cost = journal_len.saturating_mul(PER_JOURNAL_ENTRY_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, scan_cost, gas_limit) {
        return Err(SlotDiffsError::OutOfGas(rax));
    }

    let (_, _, changed) = get_slot_pre_post(ph_context, call.target, slot);

    let encoded = changed.abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

/// `mappingValueDiff(address target, bytes32 baseSlot, bytes32 key, uint256 fieldOffset) -> (bytes32 pre, bytes32 post, bool changed)`
///
/// Computes the Solidity mapping storage slot and returns pre/post values.
pub fn mapping_value_diff(
    ph_context: &PhEvmContext,
    input_bytes: &[u8],
    gas: u64,
) -> Result<PhevmOutcome, SlotDiffsError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(SlotDiffsError::OutOfGas(rax));
    }

    let call = PhEvm::mappingValueDiffCall::abi_decode(input_bytes)
        .map_err(SlotDiffsError::DecodeError)?;

    let slot = compute_mapping_slot(call.baseSlot, call.key, call.fieldOffset);

    // Charge gas for journal scan
    let journal_len = ph_context.post_tx_journal().journal.len() as u64;
    let scan_cost = journal_len.saturating_mul(PER_JOURNAL_ENTRY_COST);
    if let Some(rax) = deduct_gas_and_check(&mut gas_left, scan_cost, gas_limit) {
        return Err(SlotDiffsError::OutOfGas(rax));
    }

    let (pre, post, changed) = get_slot_pre_post(ph_context, call.target, slot);

    let pre_bytes = FixedBytes::<32>::from(pre.to_be_bytes::<32>());
    let post_bytes = FixedBytes::<32>::from(post.to_be_bytes::<32>());
    let encoded = (pre_bytes, post_bytes, changed).abi_encode();
    Ok(PhevmOutcome::new(encoded.into(), gas_limit - gas_left))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        db::overlay::test_utils::MockDb,
        inspectors::{
            phevm::{
                LogsAndTraces,
                PhEvmContext,
            },
            tracer::CallTracer,
        },
        primitives::JournalInner,
        test_utils::{
            random_address,
            random_u256,
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
    use revm::JournalEntry;

    const TEST_GAS: u64 = 1_000_000;

    fn with_journal_context<F, R>(journal: JournalInner<JournalEntry>, f: F) -> R
    where
        F: FnOnce(&PhEvmContext) -> R,
    {
        let mut call_tracer = CallTracer::default();
        call_tracer.journal = journal;

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };
        let tx_env = crate::primitives::TxEnv::default();

        let context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter: Address::ZERO,
            console_logs: vec![],
            original_tx_env: &tx_env,
            trigger_call_id: None,
        };
        f(&context)
    }

    fn slot_to_bytes32(slot: U256) -> FixedBytes<32> {
        FixedBytes::<32>::from(slot.to_be_bytes::<32>())
    }

    #[test]
    fn test_get_changed_slots_with_changes() {
        let address = random_address();
        let slot1 = random_u256();
        let slot2 = random_u256();

        let mut journal = JournalInner::new();
        let mut db = MockDb::new();

        db.insert_storage(address, slot1, U256::from(100));
        db.insert_storage(address, slot2, U256::from(200));
        journal.load_account(&mut db, address).unwrap();

        journal
            .sstore(&mut db, address, slot1, U256::from(150), false)
            .unwrap();
        journal
            .sstore(&mut db, address, slot2, U256::from(250), false)
            .unwrap();

        let input = PhEvm::getChangedSlotsCall { target: address };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            get_changed_slots(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let decoded =
            Vec::<FixedBytes<32>>::abi_decode(result.unwrap().bytes()).unwrap();
        assert_eq!(decoded.len(), 2);
        // Both changed slots should be present
        assert!(decoded.contains(&slot_to_bytes32(slot1)));
        assert!(decoded.contains(&slot_to_bytes32(slot2)));
    }

    #[test]
    fn test_get_changed_slots_no_changes() {
        let address = random_address();
        let journal = JournalInner::new();

        let input = PhEvm::getChangedSlotsCall { target: address };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            get_changed_slots(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let decoded =
            Vec::<FixedBytes<32>>::abi_decode(result.unwrap().bytes()).unwrap();
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn test_get_changed_slots_revert_to_original() {
        let address = random_address();
        let slot = random_u256();
        let original = U256::from(100);

        let mut journal = JournalInner::new();
        let mut db = MockDb::new();

        db.insert_storage(address, slot, original);
        journal.load_account(&mut db, address).unwrap();

        // Change then revert
        journal
            .sstore(&mut db, address, slot, U256::from(200), false)
            .unwrap();
        journal
            .sstore(&mut db, address, slot, original, false)
            .unwrap();

        let input = PhEvm::getChangedSlotsCall { target: address };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            get_changed_slots(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let decoded =
            Vec::<FixedBytes<32>>::abi_decode(result.unwrap().bytes()).unwrap();
        assert_eq!(
            decoded.len(),
            0,
            "Slot reverted to original should not appear as changed"
        );
    }

    #[test]
    fn test_get_slot_diff_changed() {
        let address = random_address();
        let slot = random_u256();
        let original = U256::from(100);
        let new_value = U256::from(200);

        let mut journal = JournalInner::new();
        let mut db = MockDb::new();

        db.insert_storage(address, slot, original);
        journal.load_account(&mut db, address).unwrap();
        journal
            .sstore(&mut db, address, slot, new_value, false)
            .unwrap();

        let input = PhEvm::getSlotDiffCall {
            target: address,
            slot: slot_to_bytes32(slot),
        };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            get_slot_diff(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let (pre, post, changed) =
            <(FixedBytes<32>, FixedBytes<32>, bool)>::abi_decode(
                result.unwrap().bytes(),
            )
            .unwrap();
        assert_eq!(U256::from_be_bytes(pre.0), original);
        assert_eq!(U256::from_be_bytes(post.0), new_value);
        assert!(changed);
    }

    #[test]
    fn test_get_slot_diff_not_changed() {
        let address = random_address();
        let slot = random_u256();
        let journal = JournalInner::new();

        let input = PhEvm::getSlotDiffCall {
            target: address,
            slot: slot_to_bytes32(slot),
        };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            get_slot_diff(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let (pre, post, changed) =
            <(FixedBytes<32>, FixedBytes<32>, bool)>::abi_decode(
                result.unwrap().bytes(),
            )
            .unwrap();
        assert_eq!(pre, post);
        assert!(!changed);
    }

    #[test]
    fn test_get_slot_diff_multiple_writes() {
        let address = random_address();
        let slot = random_u256();
        let original = U256::from(10);

        let mut journal = JournalInner::new();
        let mut db = MockDb::new();

        db.insert_storage(address, slot, original);
        journal.load_account(&mut db, address).unwrap();

        // Multiple writes: 10 → 20 → 30 → 40
        journal
            .sstore(&mut db, address, slot, U256::from(20), false)
            .unwrap();
        journal
            .sstore(&mut db, address, slot, U256::from(30), false)
            .unwrap();
        journal
            .sstore(&mut db, address, slot, U256::from(40), false)
            .unwrap();

        let input = PhEvm::getSlotDiffCall {
            target: address,
            slot: slot_to_bytes32(slot),
        };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            get_slot_diff(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let (pre, post, changed) =
            <(FixedBytes<32>, FixedBytes<32>, bool)>::abi_decode(
                result.unwrap().bytes(),
            )
            .unwrap();
        assert_eq!(U256::from_be_bytes(pre.0), original);
        assert_eq!(U256::from_be_bytes(post.0), U256::from(40));
        assert!(changed);
    }

    #[test]
    fn test_compute_mapping_slot_basic() {
        let base_slot = FixedBytes::<32>::ZERO;
        let mut key_bytes = [0u8; 32];
        key_bytes[31] = 1;
        let key = FixedBytes::<32>::from(key_bytes);

        let slot = compute_mapping_slot(base_slot, key, U256::ZERO);

        // Manual computation
        let mut input = [0u8; 64];
        input[..32].copy_from_slice(key.as_slice());
        input[32..].copy_from_slice(base_slot.as_slice());
        let expected = U256::from_be_bytes(keccak256(input).0);
        assert_eq!(slot, expected);
    }

    #[test]
    fn test_compute_mapping_slot_with_offset() {
        let base_slot = FixedBytes::<32>::ZERO;
        let mut key_bytes = [0u8; 32];
        key_bytes[31] = 1;
        let key = FixedBytes::<32>::from(key_bytes);
        let offset = U256::from(3);

        let slot = compute_mapping_slot(base_slot, key, offset);

        let mut input = [0u8; 64];
        input[..32].copy_from_slice(key.as_slice());
        input[32..].copy_from_slice(base_slot.as_slice());
        let expected = U256::from_be_bytes(keccak256(input).0).wrapping_add(offset);
        assert_eq!(slot, expected);
    }

    #[test]
    fn test_did_mapping_key_change_true() {
        let address = random_address();
        let base_slot = FixedBytes::<32>::ZERO;
        let mut key_bytes = [0u8; 32];
        key_bytes[31] = 42;
        let key = FixedBytes::<32>::from(key_bytes);
        let field_offset = U256::ZERO;

        let computed_slot = compute_mapping_slot(base_slot, key, field_offset);

        let mut journal = JournalInner::new();
        let mut db = MockDb::new();

        db.insert_storage(address, computed_slot, U256::from(100));
        journal.load_account(&mut db, address).unwrap();
        journal
            .sstore(&mut db, address, computed_slot, U256::from(200), false)
            .unwrap();

        let input = PhEvm::didMappingKeyChangeCall {
            target: address,
            baseSlot: base_slot,
            key,
            fieldOffset: field_offset,
        };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            did_mapping_key_change(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let decoded = bool::abi_decode(result.unwrap().bytes()).unwrap();
        assert!(decoded, "Mapping key should be reported as changed");
    }

    #[test]
    fn test_did_mapping_key_change_false() {
        let address = random_address();
        let base_slot = FixedBytes::<32>::ZERO;
        let mut key_bytes = [0u8; 32];
        key_bytes[31] = 42;
        let key = FixedBytes::<32>::from(key_bytes);
        let field_offset = U256::ZERO;

        let journal = JournalInner::new();

        let input = PhEvm::didMappingKeyChangeCall {
            target: address,
            baseSlot: base_slot,
            key,
            fieldOffset: field_offset,
        };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            did_mapping_key_change(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let decoded = bool::abi_decode(result.unwrap().bytes()).unwrap();
        assert!(!decoded, "Mapping key should not be reported as changed");
    }

    #[test]
    fn test_mapping_value_diff() {
        let address = random_address();
        let base_slot = FixedBytes::<32>::ZERO;
        let mut key_bytes = [0u8; 32];
        key_bytes[31] = 42;
        let key = FixedBytes::<32>::from(key_bytes);
        let field_offset = U256::ZERO;

        let computed_slot = compute_mapping_slot(base_slot, key, field_offset);

        let original = U256::from(500);
        let new_value = U256::from(750);

        let mut journal = JournalInner::new();
        let mut db = MockDb::new();

        db.insert_storage(address, computed_slot, original);
        journal.load_account(&mut db, address).unwrap();
        journal
            .sstore(&mut db, address, computed_slot, new_value, false)
            .unwrap();

        let input = PhEvm::mappingValueDiffCall {
            target: address,
            baseSlot: base_slot,
            key,
            fieldOffset: field_offset,
        };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            mapping_value_diff(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let (pre, post, changed) =
            <(FixedBytes<32>, FixedBytes<32>, bool)>::abi_decode(
                result.unwrap().bytes(),
            )
            .unwrap();
        assert_eq!(U256::from_be_bytes(pre.0), original);
        assert_eq!(U256::from_be_bytes(post.0), new_value);
        assert!(changed);
    }

    #[test]
    fn test_mapping_value_diff_with_field_offset() {
        let address = random_address();
        let base_slot = FixedBytes::<32>::ZERO;
        let mut key_bytes = [0u8; 32];
        key_bytes[31] = 7;
        let key = FixedBytes::<32>::from(key_bytes);
        let field_offset = U256::from(2); // Access 3rd field in the struct

        let computed_slot = compute_mapping_slot(base_slot, key, field_offset);

        let original = U256::from(1000);
        let new_value = U256::from(2000);

        let mut journal = JournalInner::new();
        let mut db = MockDb::new();

        db.insert_storage(address, computed_slot, original);
        journal.load_account(&mut db, address).unwrap();
        journal
            .sstore(&mut db, address, computed_slot, new_value, false)
            .unwrap();

        let input = PhEvm::mappingValueDiffCall {
            target: address,
            baseSlot: base_slot,
            key,
            fieldOffset: field_offset,
        };
        let encoded = input.abi_encode();

        let result = with_journal_context(journal, |context| {
            mapping_value_diff(context, &encoded, TEST_GAS)
        });
        assert!(result.is_ok());

        let (pre, post, changed) =
            <(FixedBytes<32>, FixedBytes<32>, bool)>::abi_decode(
                result.unwrap().bytes(),
            )
            .unwrap();
        assert_eq!(U256::from_be_bytes(pre.0), original);
        assert_eq!(U256::from_be_bytes(post.0), new_value);
        assert!(changed);
    }
}
