use crate::{
    inspectors::phevm::PhEvmContext,
    inspectors::sol_primitives::PhEvm,
    primitives::{
        Address,
        Bytes,
        JournaledState,
        U256,
    },
};

use alloy_sol_types::{
    SolCall,
    SolValue,
};

use revm::{
    JournalEntry,
    interpreter::CallInputs,
};

#[derive(Debug, thiserror::Error)]
pub enum GetStateChangesError {
    #[error("Error decoding call inputs")]
    CallDecodeError(#[from] alloy_sol_types::Error),
    #[error("Journaled State Missing from Context. For some reason it was not captured.")]
    JournaledStateMissing,
    #[error("Slot not found in journaled state, but differences were found.")]
    SlotNotFound,
    #[error("Account not found in journaled state, but differences were found.")]
    AccountNotFound,
}

/// Function for getting state changes for the PhEvm precompile.
/// This returns a result type, which can be used to determine the success of the precompile call and include error messaging.
pub fn get_state_changes(
    inputs: &CallInputs,
    context: &PhEvmContext,
) -> Result<Bytes, GetStateChangesError> {
    let event = PhEvm::getStateChangesCall::abi_decode(&inputs.input, true)?;
    let journaled_state = context
        .logs_and_traces
        .call_traces
        .journaled_state
        .as_ref()
        .ok_or(GetStateChangesError::JournaledStateMissing)?;

    let differences = get_differences(journaled_state, event.contractAddress, event.slot.into())?;

    Ok(Vec::<U256>::abi_encode(&differences).into())
}

/// Returns an array of different values for an account and slot, from the JournaledState passed.
fn get_differences(
    journaled_state: &JournaledState,
    contract_address: Address,
    slot: U256,
) -> Result<Vec<U256>, GetStateChangesError> {
    let mut differences = Vec::new();

    for entry in journaled_state.journal.iter().flatten() {
        if let JournalEntry::StorageChanged {
            address,
            had_value,
            key,
        } = entry
            && *address == **contract_address
            && *key == slot
        {
            differences.push(*had_value);
        }
    }

    // If any differences were found in the journal, check the state to get the current value.
    // The account should always exist in state if differences were found in the journal.
    if !differences.is_empty() {
        let journaled_state_account = journaled_state
            .state
            .get(&contract_address)
            .ok_or(GetStateChangesError::AccountNotFound)?;

        let current_slot_value = journaled_state_account
            .storage
            .get(&slot)
            .ok_or(GetStateChangesError::SlotNotFound)?
            .present_value;

        differences.push(current_slot_value);
    };

    Ok(differences)
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
            sol_primitives::PhEvm,
            tracer::CallTracer,
        },
        primitives::{
            Account,
            JournaledState,
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
    use revm::{
        JournalEntry,
        interpreter::{
            CallInputs,
            CallScheme,
            CallValue,
        },
    };
    use std::collections::HashSet;

    fn create_call_inputs_for_state_changes(contract_address: Address, slot: U256) -> CallInputs {
        let call = PhEvm::getStateChangesCall {
            contractAddress: contract_address,
            slot: slot.into(),
        };
        let encoded = call.abi_encode();

        CallInputs {
            input: Bytes::from(encoded),
            gas_limit: 1_000_000,
            bytecode_address: Address::ZERO,
            target_address: contract_address,
            caller: Address::ZERO,
            value: CallValue::Transfer(U256::ZERO),
            scheme: CallScheme::Call,
            is_static: false,
            is_eof: false,
            return_memory_offset: 0..0,
        }
    }

    fn with_journaled_state_context<F, R>(journaled_state: Option<JournaledState>, f: F) -> R
    where
        F: FnOnce(&PhEvmContext) -> R,
    {
        let mut call_tracer = CallTracer::new();
        call_tracer.journaled_state = journaled_state;

        let logs_and_traces = LogsAndTraces {
            tx_logs: &[],
            call_traces: &call_tracer,
        };

        let context = PhEvmContext {
            logs_and_traces: &logs_and_traces,
            adopter: Address::ZERO,
        };
        f(&context)
    }

    fn create_journaled_state_with_changes(
        address: Address,
        slot: U256,
        old_values: Vec<U256>,
        current_value: U256,
    ) -> JournaledState {
        let mut journaled_state =
            JournaledState::new(revm::primitives::SpecId::LATEST, HashSet::default());

        // Add journal entries for storage changes
        let mut journal_entries = Vec::new();
        for old_value in old_values {
            journal_entries.push(JournalEntry::StorageChanged {
                address,
                key: slot,
                had_value: old_value,
            });
        }
        journaled_state.journal = vec![journal_entries];

        // Add the account to state with current storage value
        let mut storage = std::collections::HashMap::default();
        storage.insert(slot, revm::primitives::EvmStorageSlot::new(current_value));

        let account = Account {
            info: revm::primitives::AccountInfo::default(),
            storage,
            status: revm::primitives::AccountStatus::Loaded,
        };

        journaled_state.state.insert(address, account);

        journaled_state
    }

    #[test]
    fn test_get_state_changes_success() {
        let contract_address = random_address();
        let slot = random_u256();
        let old_values = vec![U256::from(100), U256::from(200)];
        let current_value = U256::from(300);

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);
        let journaled_state = create_journaled_state_with_changes(
            contract_address,
            slot,
            old_values.clone(),
            current_value,
        );
        let result = with_journaled_state_context(Some(journaled_state), |context| {
            get_state_changes(&call_inputs, context)
        });
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(&encoded, false);
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 3); // 2 old values + 1 current value
        assert_eq!(differences[0], U256::from(100));
        assert_eq!(differences[1], U256::from(200));
        assert_eq!(differences[2], current_value);
    }

    #[test]
    fn test_get_state_changes_no_changes() {
        let contract_address = random_address();
        let slot = random_u256();

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        // Create empty journaled state with no changes
        let journaled_state =
            JournaledState::new(revm::primitives::SpecId::LATEST, HashSet::default());
        let result = with_journaled_state_context(Some(journaled_state), |context| {
            get_state_changes(&call_inputs, context)
        });
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(&encoded, false);
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 0); // No changes found
    }

    #[test]
    fn test_get_state_changes_missing_journaled_state() {
        let contract_address = random_address();
        let slot = random_u256();

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);
        let result =
            with_journaled_state_context(None, |context| get_state_changes(&call_inputs, context));
        assert!(result.is_err());

        match result.unwrap_err() {
            GetStateChangesError::JournaledStateMissing => {}
            other => panic!("Expected JournaledStateMissing, got {other:?}"),
        }
    }

    #[test]
    fn test_get_state_changes_invalid_input() {
        let invalid_input = Bytes::from(random_bytes::<10>()); // Too short for proper ABI decoding
        let call_inputs = CallInputs {
            input: invalid_input,
            gas_limit: 1_000_000,
            bytecode_address: Address::ZERO,
            target_address: Address::ZERO,
            caller: Address::ZERO,
            value: CallValue::Transfer(U256::ZERO),
            scheme: CallScheme::Call,
            is_static: false,
            is_eof: false,
            return_memory_offset: 0..0,
        };

        let journaled_state =
            JournaledState::new(revm::primitives::SpecId::LATEST, HashSet::default());
        let result = with_journaled_state_context(Some(journaled_state), |context| {
            get_state_changes(&call_inputs, context)
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
        let mut journaled_state =
            JournaledState::new(revm::primitives::SpecId::LATEST, HashSet::default());

        // Add journal entry for storage change
        let journal_entries = vec![JournalEntry::StorageChanged {
            address: contract_address,
            key: slot,
            had_value: U256::from(100),
        }];
        journaled_state.journal = vec![journal_entries];
        // Note: We don't add the account to state, which should cause AccountNotFound error

        let result = with_journaled_state_context(Some(journaled_state), |context| {
            get_state_changes(&call_inputs, context)
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
        let mut journaled_state =
            JournaledState::new(revm::primitives::SpecId::LATEST, HashSet::default());

        // Add journal entry for storage change
        let journal_entries = vec![JournalEntry::StorageChanged {
            address: contract_address,
            key: slot,
            had_value: U256::from(100),
        }];
        journaled_state.journal = vec![journal_entries];

        // Add account but without the requested slot
        let account = Account {
            info: revm::primitives::AccountInfo::default(),
            storage: std::collections::HashMap::default(), // Empty storage - slot not found
            status: revm::primitives::AccountStatus::Loaded,
        };
        journaled_state.state.insert(contract_address, account);

        let result = with_journaled_state_context(Some(journaled_state), |context| {
            get_state_changes(&call_inputs, context)
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
        let old_values = vec![
            U256::from(10),
            U256::from(20),
            U256::from(30),
            U256::from(40),
        ];
        let current_value = U256::from(50);

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);
        let journaled_state = create_journaled_state_with_changes(
            contract_address,
            slot,
            old_values.clone(),
            current_value,
        );
        let result = with_journaled_state_context(Some(journaled_state), |context| {
            get_state_changes(&call_inputs, context)
        });
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(&encoded, false);
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 5); // 4 old values + 1 current value

        for (i, &old_value) in old_values.iter().enumerate() {
            assert_eq!(differences[i], old_value);
        }
        assert_eq!(differences[4], current_value);
    }

    #[test]
    fn test_get_state_changes_different_addresses() {
        let contract_address = random_address();
        let other_address = random_address();
        let slot = random_u256();

        let call_inputs = create_call_inputs_for_state_changes(contract_address, slot);

        // Create journaled state with changes to different address (should not match)
        let mut journaled_state =
            JournaledState::new(revm::primitives::SpecId::LATEST, HashSet::default());

        // Add journal entry for different address
        let journal_entries = vec![JournalEntry::StorageChanged {
            address: other_address, // Different address
            key: slot,
            had_value: U256::from(50),
        }];
        journaled_state.journal = vec![journal_entries];

        let result = with_journaled_state_context(Some(journaled_state), |context| {
            get_state_changes(&call_inputs, context)
        });
        assert!(result.is_ok());

        let encoded = result.unwrap();
        let decoded = Vec::<U256>::abi_decode(&encoded, false);
        assert!(decoded.is_ok());

        let differences = decoded.unwrap();
        assert_eq!(differences.len(), 0); // No matching changes
    }

    #[test]
    fn test_get_differences_function() {
        let contract_address = random_address();
        let slot = random_u256();
        let old_values = vec![U256::from(1), U256::from(2)];
        let current_value = U256::from(3);

        let journaled_state = create_journaled_state_with_changes(
            contract_address,
            slot,
            old_values.clone(),
            current_value,
        );

        let result = get_differences(&journaled_state, contract_address, slot);
        assert!(result.is_ok());

        let differences = result.unwrap();
        assert_eq!(differences.len(), 3);
        assert_eq!(differences[0], U256::from(1));
        assert_eq!(differences[1], U256::from(2));
        assert_eq!(differences[2], current_value);
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
        let decoded = PhEvm::getStateChangesCall::abi_decode(&encoded, true).unwrap();

        assert_eq!(decoded.contractAddress, contract_address);
        assert_eq!(decoded.slot, FixedBytes::<32>::from(slot));
    }

    #[tokio::test]
    async fn test_get_state_changes_integration() {
        let result = run_precompile_test("TestGetStateChanges").await;
        assert!(result.is_valid(), "{result:#?}");
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
