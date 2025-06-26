use crate::{
    db::MultiForkDb,
    inspectors::sol_primitives::PhEvm::loadCall,
    primitives::{
        Address,
        Bytes,
    },
};
use revm::{
    DatabaseRef,
    InnerEvmContext,
    interpreter::CallInputs,
};

use alloy_sol_types::{
    SolCall,
    SolValue,
};
use std::convert::Infallible;

/// Returns a storage slot for a given address. Will return `0x0` if slot empty.
pub fn load_external_slot(
    context: &InnerEvmContext<&mut MultiForkDb<impl DatabaseRef>>,
    call_inputs: &CallInputs,
) -> Result<Bytes, Infallible> {
    let call = match loadCall::abi_decode(&call_inputs.input, true) {
        Ok(call) => call,
        Err(_) => return Ok(Bytes::default()),
    };
    let address: Address = call.target;

    // Load the account before reading the storage.
    // This prevents a bug with revm's State<Db> where it panics if reading the storage before
    // loading the account.
    let _ = context.db.active_db.basic_ref(address);

    let slot = call.slot;

    let slot_value = match context.db.active_db.storage_ref(address, slot.into()) {
        Ok(rax) => rax,
        Err(_) => return Ok(Bytes::default()),
    };

    Ok(SolValue::abi_encode(&slot_value).into())
}

#[cfg(test)]
mod test {
    use crate::{
        db::overlay::test_utils::MockDb,
        inspectors::sol_primitives::PhEvm::loadCall,
        primitives::{
            Bytecode,
            FixedBytes,
        },
        test_utils::{
            random_address,
            random_u256,
            run_precompile_test,
        },
    };
    use alloy_primitives::{
        Address,
        Bytes,
        U256,
    };
    use alloy_sol_types::SolCall;
    use revm::{
        InnerEvmContext,
        interpreter::{
            CallInputs,
            CallScheme,
            CallValue,
        },
        primitives::{
            AccountInfo,
            KECCAK_EMPTY,
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
            input: Bytes::from(encoded),
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
        let decoded = loadCall::abi_decode(&encoded, true).unwrap();

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
        let mut multi_fork = MultiForkDb::new(MockDb::new(), mock_db);
        let context = InnerEvmContext::new(&mut multi_fork);

        let result = load_external_slot(&context, &call_inputs);
        let decoded = loadCall::abi_decode_returns(&result.unwrap(), true).unwrap();
        assert_eq!(
            decoded.data.0,
            FixedBytes::from(expected_value.to_be_bytes())
        );
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
        let mut multi_fork = MultiForkDb::new(MockDb::new(), mock_db);
        let context = InnerEvmContext::new(&mut multi_fork);

        let result = load_external_slot(&context, &call_inputs);
        let decoded = loadCall::abi_decode_returns(&result.unwrap(), true).unwrap();
        assert_eq!(decoded.data.0, FixedBytes::ZERO);
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

        let mut multi_fork = MultiForkDb::new(MockDb::new(), mock_db);
        let context = InnerEvmContext::new(&mut multi_fork);

        let result = load_external_slot(&context, &call_inputs);

        // parse the result
        let decoded = loadCall::abi_decode_returns(&result.unwrap(), true).unwrap();
        assert_eq!(decoded.data.0, FixedBytes::ZERO);
    }

    #[tokio::test]
    async fn test_load_integration() {
        let result = run_precompile_test("TestLoad").await;
        assert!(result.is_valid());
        let result_and_state = result.result_and_state;
        assert!(result_and_state.result.is_success());
    }
}
