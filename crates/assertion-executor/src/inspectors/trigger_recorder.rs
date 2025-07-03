use crate::{
    inspectors::{inspector_result_to_call_outcome, sol_primitives::ITriggerRecorder},
    primitives::{Address, Bytecode, Bytes, FixedBytes, address, bytes},
};
use alloy_evm::eth::EthEvmContext;
use op_revm::OpContext;
use revm::{
    Inspector,
    database::{Database, InMemoryDB},
    interpreter::{CallInputs, CallOutcome, Gas},
};

use alloy_sol_types::SolCall;

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// Trigger recorder address
/// address(uint160(uint256(keccak256("TriggerRecorder"))))
pub const TRIGGER_RECORDER: Address = address!("55BB9AD8Dc1EE06D47279fC2B23Cd755B7f2d326");

/// Trigger type represents different types of triggers that assertions can be registered for.
///
/// Call { trigger_selector: FixedBytes<4> } - Triggers on specific function calls matching the 4-byte selector
/// AllCalls - Triggers on any function call to the contract
/// BalanceChange - Triggers when the contract's ETH balance changes
/// StorageChange { trigger_slot: FixedBytes<32> } - Triggers when a specific storage slot is modified
/// AllStorageChanges - Triggers on any storage modification in the contract
///
/// These triggers are used to determine when an assertion should be executed.
/// The trigger recorder keeps track of which triggers are registered for each contract.
#[derive(Clone, Debug, PartialEq, Hash, Eq, Serialize, Deserialize)]
pub enum TriggerType {
    Call { trigger_selector: FixedBytes<4> },
    AllCalls,
    BalanceChange,
    StorageChange { trigger_slot: FixedBytes<32> },
    AllStorageChanges,
}

/// TriggerRecorder is an inspector for recording calls made to register triggers at the trigger
/// recorder address.
/// The recorder triggers are used to determine when to run an assertion.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Clone)]
pub struct TriggerRecorder {
    pub triggers: HashMap<TriggerType, HashSet<FixedBytes<4>>>,
}

#[derive(thiserror::Error, Debug)]
pub enum RecordError {
    #[error("Failed to decode call inputs")]
    CallDecodeError(#[from] alloy_sol_types::Error),
    #[error("Fn selector not found")]
    FnSelectorNotFound,
}

impl TriggerRecorder {
    /// Records a trigger call made to the trigger recorder address.
    fn record_trigger(&mut self, input_bytes: &[u8]) -> Result<Bytes, RecordError> {
        match input_bytes
            .get(0..4)
            .unwrap_or_default()
            .try_into()
            .unwrap_or_default()
        {
            ITriggerRecorder::registerCallTrigger_0Call::SELECTOR => {
                let fn_selector =
                    ITriggerRecorder::registerCallTrigger_0Call::abi_decode(input_bytes)?
                        .fnSelector;
                self.add_trigger(TriggerType::AllCalls, fn_selector);
            }

            ITriggerRecorder::registerCallTrigger_1Call::SELECTOR => {
                let call = ITriggerRecorder::registerCallTrigger_1Call::abi_decode(input_bytes)?;
                self.add_trigger(
                    TriggerType::Call {
                        trigger_selector: call.triggerSelector,
                    },
                    call.fnSelector,
                );
            }

            ITriggerRecorder::registerStorageChangeTrigger_0Call::SELECTOR => {
                let fn_selector =
                    ITriggerRecorder::registerStorageChangeTrigger_0Call::abi_decode(input_bytes)?
                        .fnSelector;
                self.add_trigger(TriggerType::AllStorageChanges, fn_selector);
            }

            ITriggerRecorder::registerStorageChangeTrigger_1Call::SELECTOR => {
                let call =
                    ITriggerRecorder::registerStorageChangeTrigger_1Call::abi_decode(input_bytes)?;
                self.add_trigger(
                    TriggerType::StorageChange {
                        trigger_slot: call.slot,
                    },
                    call.fnSelector,
                );
            }

            ITriggerRecorder::registerBalanceChangeTriggerCall::SELECTOR => {
                let fn_selector =
                    ITriggerRecorder::registerBalanceChangeTriggerCall::abi_decode(input_bytes)?
                        .fnSelector;
                self.add_trigger(TriggerType::BalanceChange, fn_selector);
            }

            _ => return Err(RecordError::FnSelectorNotFound),
        }
        Ok(Bytes::new())
    }

    /// Adds an assertion to the trigger recorder's respective trigger type.
    fn add_trigger(&mut self, trigger_type: TriggerType, fn_selector: FixedBytes<4>) {
        self.triggers
            .entry(trigger_type)
            .or_default()
            .insert(fn_selector);
    }
}

/// Macro to implement Inspector trait for multiple context types.
/// This avoids duplicating the implementation and provides better maintainability.
macro_rules! impl_trigger_recorder_inspector {
    ($($context_type:ty),* $(,)?) => {
        $(
            impl<DB: Database> Inspector<$context_type> for TriggerRecorder {
                fn call(
                    &mut self,
                    context: &mut $context_type,
                    inputs: &mut CallInputs,
                ) -> Option<CallOutcome> {
                    if inputs.target_address == TRIGGER_RECORDER {
                        let input_bytes = inputs.input.bytes(context);
                        let record_result = self.record_trigger(&input_bytes);
                        let gas = Gas::new(inputs.gas_limit);
                        return Some(inspector_result_to_call_outcome(
                            record_result,
                            gas,
                            inputs.return_memory_offset.clone(),
                        ));
                    }
                    None
                }
            }
        )*
    };
}

// Implement Inspector for both context types using the macro
impl_trigger_recorder_inspector!(EthEvmContext<DB>, OpContext<DB>,);

/// Insert the trigger recorder account into the database.
pub fn insert_trigger_recorder_account(db: &mut InMemoryDB) {
    db.insert_account_info(
        TRIGGER_RECORDER,
        crate::primitives::AccountInfo {
            code: Some(Bytecode::new_raw(bytes!("45"))),
            ..Default::default()
        },
    );
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::{
        evm::build_evm::{build_optimism_evm, evm_env},
        primitives::{Bytecode, TxEnv, TxKind, fixed_bytes},
        store::triggersCall,
        test_utils::deployed_bytecode,
    };
    use op_revm::OpTransaction;
    use revm::{InspectEvm, context::BlockEnv, database::InMemoryDB, primitives::hardfork::SpecId};

    fn run_trigger_recorder_test(artifact: &str) -> TriggerRecorder {
        let assertion_contract = Address::random();
        let deployed_code = deployed_bytecode(&format!("{artifact}.sol:{artifact}"));

        let mut db = InMemoryDB::default();
        db.insert_account_info(
            assertion_contract,
            crate::primitives::AccountInfo {
                code: Some(Bytecode::new_raw(deployed_code)),
                ..Default::default()
            },
        );

        insert_trigger_recorder_account(&mut db);

        let tx_env = TxEnv {
            kind: TxKind::Call(assertion_contract),
            data: triggersCall::SELECTOR.into(),
            ..Default::default()
        };

        let tx_env = OpTransaction::new(tx_env);

        let mut trigger_recorder = TriggerRecorder::default();

        let env = evm_env(1, SpecId::default(), BlockEnv::default());
        let mut evm = build_optimism_evm(&mut db, &env, &mut trigger_recorder);

        let result = evm.inspect_with_tx(tx_env).unwrap();
        std::mem::drop(evm);

        assert!(
            result.result.is_success(),
            "Failed to transact: {result:#?}",
        );

        trigger_recorder
    }

    #[test]
    fn record_trigger_on_any() {
        let triggers: HashMap<TriggerType, HashSet<FixedBytes<4>>> = HashMap::from([
            (
                TriggerType::AllCalls,
                vec![fixed_bytes!("DEADBEEF")].into_iter().collect(),
            ),
            (
                TriggerType::AllStorageChanges,
                vec![fixed_bytes!("DEADBEEF")].into_iter().collect(),
            ),
            (
                TriggerType::BalanceChange,
                vec![fixed_bytes!("DEADBEEF")].into_iter().collect(),
            ),
        ]);

        assert_eq!(
            run_trigger_recorder_test("TriggerOnAny"),
            TriggerRecorder { triggers }
        );
    }

    #[test]
    fn record_trigger_on_specific() {
        let triggers = HashMap::from([
            (
                TriggerType::Call {
                    trigger_selector: fixed_bytes!("f18c388a"),
                },
                vec![fixed_bytes!("DEADBEEF")].into_iter().collect(),
            ),
            (
                TriggerType::StorageChange {
                    trigger_slot: fixed_bytes!(
                        "ccc4fa32c72b32fc1388e9b17cbcd9cb5939d52551871739e4c3415f4ee595a0"
                    ),
                },
                vec![fixed_bytes!("DEADBEEF")].into_iter().collect(),
            ),
        ]);
        assert_eq!(
            run_trigger_recorder_test("TriggerOnSpecific"),
            TriggerRecorder { triggers }
        );
    }

    #[test]
    fn test_all_trigger_types_manual() {
        let mut recorder = TriggerRecorder::default();

        let selector1 = fixed_bytes!("12345678");
        let selector2 = fixed_bytes!("87654321");
        let selector3 = fixed_bytes!("ABCDEFAB");
        let selector4 = fixed_bytes!("FEDCBAED");
        let selector5 = fixed_bytes!("11111111");

        // Test all trigger types
        recorder.add_trigger(TriggerType::AllCalls, selector1);
        recorder.add_trigger(
            TriggerType::Call {
                trigger_selector: fixed_bytes!("AAAAAAAA"),
            },
            selector2,
        );
        recorder.add_trigger(TriggerType::AllStorageChanges, selector3);
        recorder.add_trigger(
            TriggerType::StorageChange {
                trigger_slot: fixed_bytes!(
                    "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
                ),
            },
            selector4,
        );
        recorder.add_trigger(TriggerType::BalanceChange, selector5);

        // Verify all triggers were recorded
        assert_eq!(recorder.triggers.len(), 5);

        assert!(recorder.triggers[&TriggerType::AllCalls].contains(&selector1));
        assert!(
            recorder.triggers[&TriggerType::Call {
                trigger_selector: fixed_bytes!("AAAAAAAA")
            }]
                .contains(&selector2)
        );
        assert!(
            recorder.triggers[&TriggerType::Call {
                trigger_selector: fixed_bytes!("AAAAAAAA")
            }]
                .contains(&selector2)
        );
        assert!(recorder.triggers[&TriggerType::AllStorageChanges].contains(&selector3));
        assert!(
            recorder.triggers[&TriggerType::StorageChange {
                trigger_slot: fixed_bytes!(
                    "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
                )
            }]
                .contains(&selector4)
        );
        assert!(
            recorder.triggers[&TriggerType::StorageChange {
                trigger_slot: fixed_bytes!(
                    "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
                )
            }]
                .contains(&selector4)
        );
        assert!(recorder.triggers[&TriggerType::BalanceChange].contains(&selector5));
    }

    #[test]
    fn test_multiple_selectors_same_trigger() {
        let mut recorder = TriggerRecorder::default();

        let selector1 = fixed_bytes!("11111111");
        let selector2 = fixed_bytes!("22222222");
        let selector3 = fixed_bytes!("33333333");

        // Add multiple selectors to the same trigger type
        recorder.add_trigger(TriggerType::AllCalls, selector1);
        recorder.add_trigger(TriggerType::AllCalls, selector2);
        recorder.add_trigger(TriggerType::AllCalls, selector3);

        // Should have one trigger type with three selectors
        assert_eq!(recorder.triggers.len(), 1);
        assert_eq!(recorder.triggers[&TriggerType::AllCalls].len(), 3);

        assert!(recorder.triggers[&TriggerType::AllCalls].contains(&selector1));
        assert!(recorder.triggers[&TriggerType::AllCalls].contains(&selector2));
        assert!(recorder.triggers[&TriggerType::AllCalls].contains(&selector3));
    }

    #[test]
    fn test_record_trigger_invalid_selector() {
        let mut recorder = TriggerRecorder::default();

        let input = Bytes::from([0xFF, 0xFF, 0xFF, 0xFF]);

        let result = recorder.record_trigger(&input);
        assert!(matches!(result, Err(RecordError::FnSelectorNotFound)));
    }

    #[test]
    fn test_record_trigger_decode_error() {
        let mut recorder = TriggerRecorder::default();

        // Create call inputs with valid selector but invalid data
        let mut invalid_data = ITriggerRecorder::registerCallTrigger_0Call::SELECTOR.to_vec();
        invalid_data.extend_from_slice(&[0xFF; 10]); // Add invalid data

        let result = recorder.record_trigger(&invalid_data);
        assert!(matches!(result, Err(RecordError::CallDecodeError(_))));
    }
}
