use crate::{
    db::{
        DatabaseRef,
        multi_fork_db::MultiForkDb,
    },
    inspectors::sol_primitives::PhEvm::loadCall,
    primitives::{
        Address,
        Bytes,
    },
};
use revm::{
    context::{
        ContextTr,
        Journal,
        JournalTr,
    },
    interpreter::{
        CallInputs,
        Host,
    },
};

use alloy_sol_types::{
    SolCall,
    SolValue,
};

#[derive(Debug, thiserror::Error)]
#[error("Error loading external slot: {0}")]
pub struct LoadExternalSlotError<ExtDb: DatabaseRef>(pub ExtDb::Error);

/// Returns a storage slot for a given address. Will return `0x0` if slot empty.
pub fn load_external_slot<'db, ExtDb: DatabaseRef + 'db, CTX>(
    context: &mut CTX,
    call_inputs: &CallInputs,
) -> Result<Bytes, LoadExternalSlotError<ExtDb>>
where
    CTX:
        ContextTr<Db = &'db mut MultiForkDb<ExtDb>, Journal = Journal<&'db mut MultiForkDb<ExtDb>>>,
{
    let Ok(call) = loadCall::abi_decode(&call_inputs.input.bytes(context)) else {
        return Ok(Bytes::default());
    };
    let address: Address = call.target;

    // Load the account before reading the storage.
    // This prevents a bug with revm's State<Db> where it panics if reading the storage before
    // loading the account.
    context
        .journal()
        .load_account(address)
        .map_err(LoadExternalSlotError)?;

    let value_opt = context.sload(address, call.slot.into());
    let slot_value = value_opt.unwrap_or_default().data;

    Ok(SolValue::abi_encode(&slot_value).into())
}

#[cfg(test)]
mod test {
    use crate::{
        db::{
            fork_db::ForkDb,
            overlay::test_utils::MockDb,
        },
        inspectors::sol_primitives::PhEvm::loadCall,
        primitives::{
            AccountInfo,
            Bytecode,
            FixedBytes,
        },
        test_utils::{
            random_address,
            random_u256,
            run_precompile_test,
        },
    };
    use alloy_evm::eth::EthEvmContext;
    use alloy_primitives::{
        Address,
        U256,
    };
    use alloy_sol_types::SolCall;
    use revm::{
        context::JournalInner,
        interpreter::{
            CallInput,
            CallInputs,
            CallScheme,
            CallValue,
        },
        primitives::{
            KECCAK_EMPTY,
            hardfork::SpecId,
        },
    };

    use super::*;

    fn create_call_inputs_for_load(target: Address, slot: U256) -> CallInputs {
        let call = loadCall {
            target,
            slot: slot.into(),
        };
        let encoded = call.abi_encode();

        CallInputs {
            input: CallInput::Bytes(encoded.into()),
            gas_limit: 1_000_000,
            bytecode_address: Address::ZERO,
            target_address: target,
            caller: Address::ZERO,
            value: CallValue::Transfer(U256::ZERO),
            scheme: CallScheme::Call,
            is_static: false,
            is_eof: false,
            return_memory_offset: 0..0,
        }
    }

    fn create_mock_db_with_storage(address: Address, slot: U256, value: U256) -> MockDb {
        let mut mock_db = MockDb::new();
        mock_db.insert_storage(address, slot, value);
        mock_db.insert_account(
            address,
            AccountInfo {
                balance: U256::ZERO,
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: Some(Bytecode::default()),
            },
        );
        mock_db
    }

    #[test]
    fn test_load_call_encoding_roundtrip() {
        let target = random_address();
        let slot = random_u256();
        let slot_bytes: alloy_primitives::FixedBytes<32> = slot.into();

        let call = loadCall {
            target,
            slot: slot_bytes,
        };
        let encoded = call.abi_encode();
        let decoded = loadCall::abi_decode(&encoded).unwrap();

        assert_eq!(decoded.target, target);
        assert_eq!(decoded.slot, slot_bytes);
    }

    #[test]
    fn test_load_call_existing_account_and_storage_value() {
        // Test that function returns properly encoded storage value
        let target = random_address();
        let slot = random_u256();
        let expected_value = random_u256();

        // Create valid call inputs
        let call_inputs = create_call_inputs_for_load(target, slot);

        // Create context with storage value
        let mock_db = create_mock_db_with_storage(target, slot, expected_value);
        let mut multi_fork = MultiForkDb::new(ForkDb::new(mock_db), &JournalInner::new());

        let mut context = EthEvmContext::new(&mut multi_fork, SpecId::default());

        let result = load_external_slot(&mut context, &call_inputs);
        let decoded = loadCall::abi_decode_returns(&result.unwrap()).unwrap();
        assert_eq!(decoded.0, FixedBytes::from(expected_value.to_be_bytes()));
    }

    #[test]
    fn test_load_call_existing_account_no_storage_value() {
        // Test with an existing account but no storage value for the requested slot
        let target = random_address();
        let slot = random_u256();

        // Create valid call inputs
        let call_inputs = create_call_inputs_for_load(target, slot);

        // Create context with account but no storage for this slot
        let mock_db = create_mock_db_with_storage(target, random_u256(), random_u256());
        let mut multi_fork = MultiForkDb::new(ForkDb::new(mock_db), &JournalInner::new());
        let mut context = EthEvmContext::new(&mut multi_fork, SpecId::default());

        let result = load_external_slot(&mut context, &call_inputs);
        let decoded = loadCall::abi_decode_returns(&result.unwrap()).unwrap();
        assert_eq!(decoded.0, FixedBytes::ZERO);
    }

    #[test]
    fn test_load_call_non_existing_account() {
        // Test with a non-existing account
        let target = random_address();
        let slot = random_u256();

        // Create valid call inputs
        let call_inputs = create_call_inputs_for_load(target, slot);

        // Create context with no accounts
        let mock_db = create_mock_db_with_storage(Address::ZERO, random_u256(), random_u256());
        let mut multi_fork = MultiForkDb::new(ForkDb::new(mock_db), &JournalInner::new());
        let mut context = EthEvmContext::new(&mut multi_fork, SpecId::default());

        let result = load_external_slot(&mut context, &call_inputs);

        // parse the result
        let decoded = loadCall::abi_decode_returns(&result.unwrap()).unwrap();
        assert_eq!(decoded.0, FixedBytes::ZERO);
    }

    #[test]
    fn test_load_integration() {
        let result = run_precompile_test("TestLoad");
        assert!(result.is_valid());
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
