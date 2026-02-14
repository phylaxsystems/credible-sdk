use crate::{
    inspectors::{
        phevm::{
            PhEvmContext,
            PhevmOutcome,
        },
        precompiles::deduct_gas_and_check,
        sol_primitives::PhEvm,
    },
    primitives::{
        Address,
        Bytes,
        JournalEntry,
        JournalInner,
        U256,
    },
};

use alloy_sol_types::{
    SolCall,
    SolType,
    sol_data::{
        Array,
        Uint,
    },
};
use bumpalo::Bump;

use super::BASE_COST;

#[derive(Debug, thiserror::Error)]
pub enum GetStateChangesError {
    #[error("Error decoding call inputs")]
    CallDecodeError(#[source] alloy_sol_types::Error),
    #[error("Journal Missing from Context. For some reason it was not captured.")]
    JournalMissing,
    #[error("Slot not found in journal, but differences were found.")]
    SlotNotFound,
    #[error("Account not found in journal, but differences were found.")]
    AccountNotFound,
    #[error("Out of gas")]
    OutOfGas(PhevmOutcome),
}

/// Function for getting state changes for the `PhEvm` precompile.
/// This returns a result type, which can be used to determine the success of the precompile call and include error messaging.
///
/// # Errors
///
/// Returns an error if decoding fails, the journal is missing, or gas is exhausted.
pub fn get_state_changes(
    input_bytes: &[u8],
    context: &PhEvmContext,
    gas: u64,
) -> Result<PhevmOutcome, GetStateChangesError> {
    let gas_limit = gas;
    let mut gas_left = gas;

    if let Some(rax) = deduct_gas_and_check(&mut gas_left, BASE_COST, gas_limit) {
        return Err(GetStateChangesError::OutOfGas(rax));
    }

    let event = PhEvm::getStateChangesCall::abi_decode(input_bytes)
        .map_err(GetStateChangesError::CallDecodeError)?;

    let index = context.logs_and_traces.call_traces.storage_change_index();
    let journal = &context.logs_and_traces.call_traces.journal;

    let dif_bytes: Bytes = crate::arena::with_current_tx_arena(|arena| {
        let mut differences: Vec<U256, &Bump> = Vec::new_in(arena);
        get_differences(
            journal,
            index,
            event.contractAddress,
            event.slot.into(),
            &mut gas_left,
            gas_limit,
            &mut differences,
        )?;

        Ok::<_, GetStateChangesError>(Array::<Uint<256>>::abi_encode(differences.as_slice()).into())
    })?;

    Ok(PhevmOutcome::new(dif_bytes, gas_limit - gas_left))
}

/// Returns an array of different values for an account and slot, using the pre-built index.
fn get_differences<A: std::alloc::Allocator>(
    journal: &JournalInner<JournalEntry>,
    index: &crate::inspectors::tracer::StorageChangeIndex,
    contract_address: Address,
    slot: U256,
    gas_left: &mut u64,
    gas_limit: u64,
    differences: &mut Vec<U256, A>,
) -> Result<(), GetStateChangesError> {
    const JOURNAL_PROCESSING_COST: u64 = 16;
    const MEMORY_COST: u64 = 3;
    const PUSH_LAST: u64 = 6;

    // Charge for journal scan proportional to journal size (same cost model)
    let journal_len = journal.journal.len() as u64;
    if let Some(rax) = deduct_gas_and_check(
        gas_left,
        journal_len.saturating_mul(JOURNAL_PROCESSING_COST),
        gas_limit,
    ) {
        return Err(GetStateChangesError::OutOfGas(rax));
    }

    // Use the index to find matching entries directly
    if let Some(entries) = index.changes_for_key(&contract_address, &slot) {
        for entry in entries {
            if let Some(rax) = deduct_gas_and_check(gas_left, MEMORY_COST, gas_limit) {
                return Err(GetStateChangesError::OutOfGas(rax));
            }

            differences.push(entry.had_value);
        }
    }

    // If any differences were found in the journal, check the state to get the current value.
    // The account should always exist in state if differences were found in the journal.
    if !differences.is_empty() {
        if let Some(rax) = deduct_gas_and_check(gas_left, PUSH_LAST, gas_limit) {
            return Err(GetStateChangesError::OutOfGas(rax));
        }

        let journal_account = journal
            .state
            .get(&contract_address)
            .ok_or(GetStateChangesError::AccountNotFound)?;

        let current_slot_value = journal_account
            .storage
            .get(&slot)
            .ok_or(GetStateChangesError::SlotNotFound)?
            .present_value;

        differences.push(current_slot_value);
    }

    Ok(())
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
            sol_primitives::PhEvm,
            tracer::CallTracer,
        },
        primitives::{
            Account,
            AccountInfo,
            AccountStatus,
        },
        test_utils::{
            random_address,
            random_bytes,
            random_u256,
            run_precompile_test,
        },
    };
    use alloy_primitives::{
        Address,
        Bytes,
        FixedBytes,
        U256,
    };
    use alloy_sol_types::{
        SolCall,
        SolValue,
    };
    use revm::JournalEntry;

    const TEST_GAS: u64 = 1_000_000;

    fn expected_gas_left_after_get_differences(
        starting_gas_left: u64,
        journal: &JournalInner<JournalEntry>,
        contract_address: Address,
        slot: U256,
    ) -> u64 {
        const JOURNAL_PROCESSING_COST: u64 = 16;
        const MEMORY_COST: u64 = 3;
        const PUSH_LAST: u64 = 6;

        let journal_entries = journal.journal.len() as u64;
        let matching_storage_changes = journal
            .journal
            .iter()
            .filter(|entry| {
                match entry {
                    JournalEntry::StorageChanged { address, key, .. } => {
                        *address == contract_address && *key == slot
                    }
                    _ => false,
                }
            })
            .count() as u64;

        let push_last_cost = if matching_storage_changes > 0 {
            PUSH_LAST
        } else {
            0
        };

        starting_gas_left
            - (journal_entries * JOURNAL_PROCESSING_COST)
            - (matching_storage_changes * MEMORY_COST)
            - push_last_cost
    }

    fn create_call_inputs_for_state_changes(contract_address: Address, slot: U256) -> Bytes {
        PhEvm::getStateChangesCall {
            contractAddress: contract_address,
            slot: slot.into(),
        }
        .abi_encode()
        .into()
    }

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

    fn create_journal_with_changes(
        address: Address,
        slot: U256,
        current_value: U256,
        value_updates: Vec<U256>,
        db: &mut MockDb,
    ) -> JournalInner<JournalEntry> {
        let mut journal = JournalInner::new();

        db.insert_storage(address, slot, current_value);
        journal.load_account(db, address).unwrap();

        // Add journal entries for storage changes
        for value_update in value_updates {
            journal
                .sstore(db, address, slot, value_update, false)
                .unwrap();
        }

        journal
    }

    #[test]
    fn test_get_state_changes_success() {
        let contract_address = random_address();
        let slot = random_u256();
        let value_updates = vec![U256::from(200), U256::from(300)];
        let current_value = U256::from(100);

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);
        let mut db = MockDb::new();
        let journal = create_journal_with_changes(
            contract_address,
            slot,
            current_value,
            value_updates.clone(),
            &mut db,
        );
        let expected_gas_left = expected_gas_left_after_get_differences(
            TEST_GAS - BASE_COST,
            &journal,
            contract_address,
            slot,
        );
        let expected_gas_spent = TEST_GAS - expected_gas_left;
        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, TEST_GAS)
        });
        assert!(result.is_ok());

        let encoded = result.unwrap();
        assert_eq!(encoded.gas(), expected_gas_spent);
        let decoded = Vec::<U256>::abi_decode(encoded.bytes().as_ref());
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 3); // 2 old values + 1 current value
        assert_eq!(differences[0], current_value);
        assert_eq!(differences[1], value_updates[0]);
        assert_eq!(differences[2], value_updates[1]);
    }

    #[test]
    fn test_get_state_changes_no_changes() {
        let contract_address = random_address();
        let slot = random_u256();

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        // Create empty journaled state with no changes
        let journal = JournalInner::new();
        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, TEST_GAS)
        });
        assert!(result.is_ok());

        let encoded = result.unwrap();
        assert_eq!(encoded.gas(), BASE_COST);
        let decoded = Vec::<U256>::abi_decode(encoded.bytes().as_ref());
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 0); // No changes found
    }

    #[test]
    fn test_get_state_changes_invalid_input() {
        let invalid_input = Bytes::from(random_bytes::<10>()); // Too short for proper ABI decoding

        let journal = JournalInner::new();
        let result = with_journal_context(journal, |context| {
            get_state_changes(&invalid_input, context, TEST_GAS)
        });
        assert!(result.is_err());

        match result.unwrap_err() {
            GetStateChangesError::CallDecodeError(_) => {}
            other => panic!("Expected CallDecodeError, got {other:?}"),
        }
    }

    #[test]
    fn test_get_state_changes_account_not_found() {
        let contract_address = random_address();
        let slot = random_u256();

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        // Create journaled state with journal entries but no account in state
        let mut journal = JournalInner::new();

        journal.journal.push(JournalEntry::StorageChanged {
            address: contract_address,
            key: slot,
            had_value: U256::from(100),
        });

        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, TEST_GAS)
        });
        assert!(result.is_err());

        match result.unwrap_err() {
            GetStateChangesError::AccountNotFound => {}
            other => panic!("Expected AccountNotFound, got {other:?}"),
        }
    }

    #[test]
    fn test_get_state_changes_slot_not_found() {
        let contract_address = random_address();
        let slot = random_u256();

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        // Create journaled state with journal entries and account but no slot in storage
        let mut journal = JournalInner::new();
        journal.journal.push(JournalEntry::StorageChanged {
            address: contract_address,
            key: slot,
            had_value: U256::from(100),
        });
        let mut db = MockDb::new();

        // Add account but without the requested slot
        let account = Account {
            info: AccountInfo::default(),
            transaction_id: 0,
            storage: std::collections::HashMap::default(), // Empty storage - slot not found
            status: AccountStatus::LoadedAsNotExisting,
        };
        journal.state.insert(contract_address, account);

        journal.load_account(&mut db, contract_address).unwrap();

        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, TEST_GAS)
        });
        assert!(result.is_err());

        match result.unwrap_err() {
            GetStateChangesError::SlotNotFound => {}
            other => panic!("Expected SlotNotFound, got {other:?}"),
        }
    }

    #[test]
    fn test_get_state_changes_multiple_changes_same_slot() {
        let contract_address = random_address();
        let slot = random_u256();
        let value_updates = vec![
            U256::from(20),
            U256::from(30),
            U256::from(40),
            U256::from(50),
        ];
        let current_value = U256::from(10);

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);
        let mut db = MockDb::new();
        let journal = create_journal_with_changes(
            contract_address,
            slot,
            current_value,
            value_updates.clone(),
            &mut db,
        );
        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, TEST_GAS)
        });
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(encoded.bytes().as_ref());
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 5); // 4 updated values + 1 current value

        assert_eq!(differences[0], current_value);

        for (i, &value_update) in value_updates.iter().enumerate() {
            assert_eq!(differences[i + 1], value_update);
        }
    }

    #[test]
    fn test_get_state_changes_different_addresses() {
        let contract_address = random_address();
        let other_address = random_address();
        let slot = random_u256();

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        // Create journaled state with changes to different address (should not match)
        let mut journal = JournalInner::new();
        let mut db = MockDb::new();
        journal.load_account(&mut db, other_address).unwrap();
        journal
            .sstore(&mut db, other_address, slot, U256::from(50), false)
            .unwrap();

        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, TEST_GAS)
        });
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(encoded.bytes().as_ref());
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 0); // No matching changes
    }

    #[test]
    fn test_get_differences_function() {
        use crate::inspectors::tracer::StorageChangeIndex;

        let contract_address = random_address();
        let slot = random_u256();
        let value_updates = vec![U256::from(2), U256::from(3)];
        let current_value = U256::from(1);

        let mut db = MockDb::new();
        let journal = create_journal_with_changes(
            contract_address,
            slot,
            current_value,
            value_updates.clone(),
            &mut db,
        );

        let index = StorageChangeIndex::build_from_journal(&journal);
        let mut gas_left = TEST_GAS;
        let mut differences = Vec::<U256>::new();
        let result = get_differences(
            &journal,
            &index,
            contract_address,
            slot,
            &mut gas_left,
            TEST_GAS,
            &mut differences,
        );
        assert!(result.is_ok());

        result.unwrap();
        assert_eq!(differences.len(), 3);
        assert_eq!(differences[0], current_value);
        assert_eq!(differences[1], value_updates[0]);
        assert_eq!(differences[2], value_updates[1]);
        assert_eq!(
            gas_left,
            expected_gas_left_after_get_differences(TEST_GAS, &journal, contract_address, slot)
        );
    }

    #[test]
    fn test_abi_encoding_roundtrip() {
        let contract_address = random_address();
        let slot = random_u256();

        let call = PhEvm::getStateChangesCall {
            contractAddress: contract_address,
            slot: slot.into(),
        };
        let encoded = call.abi_encode();
        let decoded = PhEvm::getStateChangesCall::abi_decode(&encoded).unwrap();

        assert_eq!(decoded.contractAddress, contract_address);
        assert_eq!(decoded.slot, FixedBytes::<32>::from(slot));
    }

    #[test]
    fn test_get_state_changes_oog_base_cost() {
        let contract_address = random_address();
        let slot = random_u256();
        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        let journal = JournalInner::new();
        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, BASE_COST - 1)
        });
        let outcome = match result {
            Ok(outcome) | Err(GetStateChangesError::OutOfGas(outcome)) => outcome,
            Err(other) => panic!("Expected Ok or OutOfGas, got {other:?}"),
        };

        assert_eq!(outcome.gas(), BASE_COST - 1);
        assert!(outcome.bytes().is_empty());
    }

    #[test]
    fn test_get_state_changes_oog_journal_processing() {
        let contract_address = random_address();
        let slot = random_u256();
        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        let mut journal = JournalInner::new();
        journal.journal.push(JournalEntry::StorageChanged {
            address: contract_address,
            key: slot,
            had_value: U256::from(1),
        });

        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, BASE_COST + 15)
        });
        let outcome = match result {
            Err(GetStateChangesError::OutOfGas(outcome)) => outcome,
            Ok(_) => panic!("Expected OutOfGas error, got Ok"),
            Err(other) => panic!("Expected OutOfGas error, got {other:?}"),
        };

        assert_eq!(outcome.gas(), BASE_COST + 15);
        assert!(outcome.bytes().is_empty());
    }

    #[test]
    fn test_get_state_changes_oog_memory_cost() {
        let contract_address = random_address();
        let slot = random_u256();
        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        let mut journal = JournalInner::new();
        journal.journal.push(JournalEntry::StorageChanged {
            address: contract_address,
            key: slot,
            had_value: U256::from(1),
        });

        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, BASE_COST + 16 + 2)
        });
        let outcome = match result {
            Err(GetStateChangesError::OutOfGas(outcome)) => outcome,
            Ok(_) => panic!("Expected OutOfGas error, got Ok"),
            Err(other) => panic!("Expected OutOfGas error, got {other:?}"),
        };

        assert_eq!(outcome.gas(), BASE_COST + 16 + 2);
        assert!(outcome.bytes().is_empty());
    }

    #[test]
    fn test_get_state_changes_oog_push_last() {
        let contract_address = random_address();
        let slot = random_u256();
        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        let mut journal = JournalInner::new();
        journal.journal.push(JournalEntry::StorageChanged {
            address: contract_address,
            key: slot,
            had_value: U256::from(1),
        });

        let result = with_journal_context(journal, |context| {
            get_state_changes(&call_inputs, context, BASE_COST + 16 + 3 + 5)
        });
        let outcome = match result {
            Err(GetStateChangesError::OutOfGas(outcome)) => outcome,
            Ok(_) => panic!("Expected OutOfGas error, got Ok"),
            Err(other) => panic!("Expected OutOfGas error, got {other:?}"),
        };

        assert_eq!(outcome.gas(), BASE_COST + 16 + 3 + 5);
        assert!(outcome.bytes().is_empty());
    }

    #[tokio::test]
    async fn test_get_state_changes_integration() {
        let result = run_precompile_test("TestGetStateChanges");
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
