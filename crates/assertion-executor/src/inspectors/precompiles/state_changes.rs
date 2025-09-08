use crate::{
    inspectors::{
        phevm::PhEvmContext,
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
    SolValue,
};

#[derive(Debug, thiserror::Error)]
pub enum GetStateChangesError {
    #[error("Error decoding call inputs")]
    CallDecodeError(#[from] alloy_sol_types::Error),
    #[error("Journal Missing from Context. For some reason it was not captured.")]
    JournalMissing,
    #[error("Slot not found in journal, but differences were found.")]
    SlotNotFound,
    #[error("Account not found in journal, but differences were found.")]
    AccountNotFound,
}

/// Function for getting state changes for the `PhEvm` precompile.
/// This returns a result type, which can be used to determine the success of the precompile call and include error messaging.
pub fn get_state_changes(
    input_bytes: &[u8],
    context: &PhEvmContext,
) -> Result<Bytes, GetStateChangesError> {
    let event = PhEvm::getStateChangesCall::abi_decode(input_bytes)?;

    let differences = get_differences(
        &context.logs_and_traces.call_traces.journal,
        event.contractAddress,
        event.slot.into(),
    )?;

    Ok(Vec::<U256>::abi_encode(&differences).into())
}

/// Returns an array of different values for an account and slot, from the `JournaledState` passed.
fn get_differences(
    journal: &JournalInner<JournalEntry>,
    contract_address: Address,
    slot: U256,
) -> Result<Vec<U256>, GetStateChangesError> {
    let mut differences = Vec::new();

    for entry in &journal.journal {
        if let JournalEntry::StorageChanged {
            address,
            had_value,
            key,
        } = entry
            && *address == contract_address
            && key == &slot
        {
            differences.push(*had_value);
        }
    }

    // If any differences were found in the journal, check the state to get the current value.
    // The account should always exist in state if differences were found in the journal.
    if !differences.is_empty() {
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

    Ok(differences)
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
        let mut call_tracer = CallTracer::new();
        call_tracer.journal = journal;

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };

        let context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter: Address::ZERO,
            console_logs: vec![],
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
            journal.sstore(db, address, slot, value_update).unwrap();
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
        let result =
            with_journal_context(journal, |context| get_state_changes(&call_inputs, context));
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(&encoded);
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
        let result =
            with_journal_context(journal, |context| get_state_changes(&call_inputs, context));
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(&encoded);
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 0); // No changes found
    }

    #[test]
    fn test_get_state_changes_invalid_input() {
        let invalid_input = Bytes::from(random_bytes::<10>()); // Too short for proper ABI decoding

        let journal = JournalInner::new();
        let result = with_journal_context(journal, |context| {
            get_state_changes(&invalid_input, context)
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

        let result =
            with_journal_context(journal, |context| get_state_changes(&call_inputs, context));
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
            storage: std::collections::HashMap::default(), // Empty storage - slot not found
            status: AccountStatus::Loaded,
        };
        journal.state.insert(contract_address, account);

        journal.load_account(&mut db, contract_address).unwrap();

        let result =
            with_journal_context(journal, |context| get_state_changes(&call_inputs, context));
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
        let result =
            with_journal_context(journal, |context| get_state_changes(&call_inputs, context));
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(&encoded);
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
            .sstore(&mut db, other_address, slot, U256::from(50))
            .unwrap();

        let result =
            with_journal_context(journal, |context| get_state_changes(&call_inputs, context));
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(&encoded);
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 0); // No matching changes
    }

    #[test]
    fn test_get_differences_function() {
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

        let result = get_differences(&journal, contract_address, slot);
        assert!(result.is_ok());

        let differences = result.unwrap();
        assert_eq!(differences.len(), 3);
        assert_eq!(differences[0], current_value);
        assert_eq!(differences[1], value_updates[0]);
        assert_eq!(differences[2], value_updates[1]);
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
    fn test_get_state_changes_integration() {
        let result = run_precompile_test("TestGetStateChanges");
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
