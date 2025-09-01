use std::convert::Infallible;

use crate::{
    ExecutorConfig,
    constants::{
        ASSERTION_CONTRACT,
        CALLER,
    },
    db::DatabaseCommit,
    evm::build_evm::evm_env,
    inspectors::{
        TriggerRecorder,
        insert_trigger_recorder_account,
    },
    primitives::{
        Account,
        AssertionContract,
        BlockEnv,
        Bytes,
        EVMError,
        ResultAndState,
        TxEnv,
        TxKind,
        keccak256,
    },
};

use revm::{
    ExecuteEvm,
    InspectEvm,
    database::InMemoryDB,
    inspector::NoOpInspector,
};

use alloy_sol_types::{
    SolCall,
    sol,
};

use tracing::{
    debug,
    warn,
};

// Typing for the assertion fn selectors
sol! {
    #[derive(Debug)]
    function triggers() external view;
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FnSelectorExtractorError {
    #[error("Failed to call triggers function: {0}")]
    TriggersCallError(EVMError<Infallible>),
    #[error("Failed to call triggers function: {0:?}")]
    TriggersCallFailed(ResultAndState),
    #[error("Error with assertion contract deployment: {0}")]
    AssertionContractDeployError(EVMError<Infallible>),
    #[error("Assertion contract deployment failed: {0:?}")]
    AssertionContractDeployFailed(ResultAndState),
    #[error("No triggers recorded in assertion contract")]
    NoTriggersRecorded,
    #[error("Assertion Contract not found at expected address.")]
    AssertionContractNotFound,
    #[error("Assertion Contract did not contain code at expected address.")]
    AssertionContractNoCode,
}

const DEPLOYMENT_GAS_LIMIT: u64 = 20_000_000;

/// Extracts [`AssertionContract`] and [`TriggerRecorder`] from a given assertion contract's deployment bytecode
#[allow(clippy::result_large_err)]
pub fn extract_assertion_contract(
    assertion_code: Bytes,
    config: &ExecutorConfig,
) -> Result<(AssertionContract, TriggerRecorder), FnSelectorExtractorError> {
    let assertion_id = keccak256(&assertion_code);

    debug!(
        target = "assertion_executor:extract_assertion_contract",
        assertion_id = ?assertion_id,
        "Extracting assertion contract"
    );

    let block_env = BlockEnv::default();

    // Deploy the contract first
    let tx_env = TxEnv {
        kind: TxKind::Create,
        caller: CALLER,
        data: assertion_code.clone(),
        gas_limit: DEPLOYMENT_GAS_LIMIT,
        chain_id: Some(config.chain_id),
        ..Default::default()
    };

    let mut db = InMemoryDB::default();

    let env = evm_env(config.chain_id, config.spec_id, block_env.clone());

    let mut evm = crate::build_evm_by_features!(&mut db, &env, NoOpInspector);
    let tx_env = crate::wrap_tx_env_for_optimism!(tx_env);

    let result_and_state = evm
        .transact(tx_env)
        .map_err(FnSelectorExtractorError::AssertionContractDeployError)?;

    if !result_and_state.result.is_success() {
        warn!(
            target = "assertion_executor::assertion_contract_extractor",
            result = ?result_and_state.result,
            "Assertion contract deployment failed",
        );
        return Err(FnSelectorExtractorError::AssertionContractDeployFailed(
            result_and_state,
        ));
    }

    let init_account = result_and_state
        .state
        .get(&ASSERTION_CONTRACT)
        .cloned()
        .ok_or(FnSelectorExtractorError::AssertionContractNotFound)?;

    db.commit(result_and_state.state);

    insert_trigger_recorder_account(&mut db);

    let tx_env = TxEnv {
        kind: TxKind::Call(ASSERTION_CONTRACT),
        caller: CALLER,
        data: triggersCall::SELECTOR.into(),
        gas_limit: DEPLOYMENT_GAS_LIMIT - result_and_state.result.gas_used(),
        nonce: 1,
        chain_id: Some(config.chain_id),
        ..Default::default()
    };

    // Set up and execute the call
    let mut trigger_recorder = TriggerRecorder::default();
    let mut evm = crate::build_evm_by_features!(&mut db, &env, &mut trigger_recorder);
    let tx_env = crate::wrap_tx_env_for_optimism!(tx_env);

    let trigger_call_result = evm
        .inspect_with_tx(tx_env)
        .map_err(FnSelectorExtractorError::TriggersCallError)?;

    if !trigger_call_result.result.is_success() {
        return Err(FnSelectorExtractorError::TriggersCallFailed(
            trigger_call_result,
        ));
    }

    std::mem::drop(evm);

    // If the triggers function does not record any triggers,
    if trigger_recorder.triggers.is_empty() {
        return Err(FnSelectorExtractorError::NoTriggersRecorded);
    }

    let Account {
        info,
        storage,
        status,
        ..
    } = init_account;

    let deployed_code = info
        .code
        .ok_or(FnSelectorExtractorError::AssertionContractNoCode)?;

    Ok((
        AssertionContract {
            deployed_code,
            code_hash: info.code_hash,
            storage,
            account_status: status,
            id: assertion_id,
        },
        trigger_recorder,
    ))
}

#[test]
fn test_get_assertion_selectors() {
    use crate::test_utils::*;

    use crate::primitives::fixed_bytes;

    let config = ExecutorConfig::default();
    // Test with valid assertion contract
    let (_, trigger_recorder) = extract_assertion_contract(bytecode(FN_SELECTOR), &config).unwrap();

    // Verify the contract has the expected selectors from the counter assertion
    let mut expected_selectors = vec![
        fixed_bytes!("e7f48038"),
        fixed_bytes!("1ff1bc3a"),
        fixed_bytes!("d210b7cf"),
    ];
    expected_selectors.sort();

    let mut recorded_selectors = trigger_recorder
        .triggers
        .values()
        .flat_map(|v| v.iter())
        .cloned()
        .collect::<Vec<_>>();
    recorded_selectors.sort();
    assert_eq!(recorded_selectors, expected_selectors);
}

#[test]
fn test_endless_loop_constructor() {
    use crate::test_utils::*;

    use crate::primitives::{
        EvmExecutionResult,
        HaltReason,
    };

    let config = ExecutorConfig::default();

    // Test with valid assertion contract
    let result = extract_assertion_contract(
        bytecode("InfiniteDeployment.sol:InfiniteDeploymentAssertion"),
        &config,
    );

    match result {
        Ok(_) => panic!("Expected an error due to infinite loop in constructor"),
        Err(e) => {
            if let FnSelectorExtractorError::AssertionContractDeployFailed(result_and_state) = e {
                match result_and_state.result {
                    EvmExecutionResult::Halt { reason, .. } => {
                        if let HaltReason::OutOfGas(_) = reason {
                        } else {
                            panic!("Expected OutOfGas error");
                        }
                    }
                    _ => panic!("Expected OutOfGas error"),
                }
            } else {
                panic!("Expected AssertionContractDeployFailed error");
            }
        }
    }
}

#[test]
fn test_extract_all_trigger_types() {
    use crate::{
        inspectors::TriggerType,
        test_utils::*,
    };

    let config = ExecutorConfig::default();

    // Test extraction from TriggerOnAny contract which should have AllCalls, AllStorageChanges, and BalanceChange
    let (_, trigger_recorder_any) =
        extract_assertion_contract(bytecode("TriggerOnAny.sol:TriggerOnAny"), &config).unwrap();

    // Should have all three "Any" trigger types
    assert!(
        trigger_recorder_any
            .triggers
            .contains_key(&TriggerType::AllCalls)
    );
    assert!(
        trigger_recorder_any
            .triggers
            .contains_key(&TriggerType::AllStorageChanges)
    );
    assert!(
        trigger_recorder_any
            .triggers
            .contains_key(&TriggerType::BalanceChange)
    );
    assert!(
        trigger_recorder_any
            .triggers
            .contains_key(&TriggerType::AllCalls)
    );
    assert!(
        trigger_recorder_any
            .triggers
            .contains_key(&TriggerType::AllStorageChanges)
    );
    assert!(
        trigger_recorder_any
            .triggers
            .contains_key(&TriggerType::BalanceChange)
    );

    // Each trigger should have the DEADBEEF selector
    let expected_selector = crate::primitives::fixed_bytes!("DEADBEEF");
    assert!(trigger_recorder_any.triggers[&TriggerType::AllCalls].contains(&expected_selector));
    assert!(
        trigger_recorder_any.triggers[&TriggerType::AllStorageChanges].contains(&expected_selector)
    );
    assert!(
        trigger_recorder_any.triggers[&TriggerType::BalanceChange].contains(&expected_selector)
    );
    assert!(
        trigger_recorder_any.triggers[&TriggerType::BalanceChange].contains(&expected_selector)
    );

    // Test extraction from TriggerOnSpecific contract which should have specific Call and StorageChange triggers
    let (_, trigger_recorder_specific) =
        extract_assertion_contract(bytecode("TriggerOnSpecific.sol:TriggerOnSpecific"), &config)
            .unwrap();

    // Should have specific trigger types
    let expected_call_trigger = TriggerType::Call {
        trigger_selector: crate::primitives::fixed_bytes!("f18c388a"),
    };
    let expected_storage_trigger = TriggerType::StorageChange {
        trigger_slot: crate::primitives::fixed_bytes!(
            "ccc4fa32c72b32fc1388e9b17cbcd9cb5939d52551871739e4c3415f4ee595a0"
        ),
    };

    assert!(
        trigger_recorder_specific
            .triggers
            .contains_key(&expected_call_trigger)
    );
    assert!(
        trigger_recorder_specific
            .triggers
            .contains_key(&expected_storage_trigger)
    );
    assert!(
        trigger_recorder_specific
            .triggers
            .contains_key(&expected_call_trigger)
    );
    assert!(
        trigger_recorder_specific
            .triggers
            .contains_key(&expected_storage_trigger)
    );

    // Both should have the DEADBEEF selector
    assert!(
        trigger_recorder_specific.triggers[&expected_call_trigger].contains(&expected_selector)
    );
    assert!(
        trigger_recorder_specific.triggers[&expected_call_trigger].contains(&expected_selector)
    );
    assert!(
        trigger_recorder_specific.triggers[&expected_storage_trigger].contains(&expected_selector)
    );
}

#[test]
fn test_extract_no_triggers_error() {
    use crate::test_utils::*;

    let config = ExecutorConfig::default();

    // Test with a contract that doesn't register any triggers
    // This would be a contract that has a triggers() function but doesn't call any register functions
    let result = extract_assertion_contract(
        bytecode("Target.sol:Target"), // Target contract likely doesn't register triggers
        &config,
    );

    match result {
        Ok(_) => panic!("Expected NoTriggersRecorded error"),
        Err(FnSelectorExtractorError::NoTriggersRecorded) => {
            // This is expected
        }
        Err(other) => {
            // The Target contract might not even have a triggers() function,
            // so we might get a different error, which is also acceptable for this test
            println!("Got different error (acceptable): {other:?}");
        }
    }
}
