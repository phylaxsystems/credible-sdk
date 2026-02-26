//! `assertion-verification`
//!
//! Reusable library containing core logic for verifying if assertions can be put in the store.
//!
//! This validates:
//! 1. contract deployment succeeds
//! 2. `triggers()` executes successfully and records at least one trigger
//! 3. resulting assertion state can be inserted into an ephemeral assertion store

use alloy_sol_types::{
    Panic,
    Revert,
    SolError,
};
use assertion_executor::{
    ExecutorConfig,
    inspectors::{
        TriggerRecorder,
        TriggerType,
    },
    primitives::{
        Address,
        Bytes,
        EvmExecutionResult,
        ResultAndState,
        hex,
    },
    store::{
        AssertionState,
        AssertionStore,
        AssertionStoreError,
        FnSelectorExtractorError,
        extract_assertion_contract,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Success,
    DeploymentFailure,
    NoTriggers,
    MissingAssertionSpec,
    InvalidAssertionSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationResult {
    pub status: VerificationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggers: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl VerificationResult {
    fn success(triggers: BTreeMap<String, String>) -> Self {
        Self {
            status: VerificationStatus::Success,
            triggers: Some(triggers),
            error: None,
        }
    }

    fn deployment_failure(error: impl Into<String>) -> Self {
        Self {
            status: VerificationStatus::DeploymentFailure,
            triggers: None,
            error: Some(error.into()),
        }
    }

    fn no_triggers() -> Self {
        Self {
            status: VerificationStatus::NoTriggers,
            triggers: None,
            error: None,
        }
    }
}

/// Verify if an assertion bytecode can be deployed and inserted into the assertion store.
///
/// This validates:
/// 1. contract deployment succeeds
/// 2. `triggers()` executes successfully and records at least one trigger
/// 3. resulting assertion state can be inserted into an ephemeral assertion store
///
/// Note: The service will execute deployment of the assertion contract.
/// So if your contract has constructor params, you must pass full deployment payload:
/// `creation_bytecode ++ abi.encode(constructor_args)`.
pub fn verify_assertion(bytecode: &Bytes, executor_config: &ExecutorConfig) -> VerificationResult {
    let (assertion_contract, trigger_recorder) =
        match extract_assertion_contract(bytecode, executor_config) {
            Ok(extracted) => extracted,
            Err(error) => return map_extraction_error(error),
        };
    let registered_triggers = format_registered_triggers(&trigger_recorder);

    let state = AssertionState {
        activation_block: 0,
        inactivation_block: None,
        assertion_contract,
        trigger_recorder,
    };

    let insert_result = if tokio::runtime::Handle::try_current().is_ok() {
        insert_assertion_state(state)
    } else {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                return VerificationResult::deployment_failure(format!(
                    "failed to create tokio runtime for insertion validation: {error}"
                ));
            }
        };

        let _guard = runtime.enter();
        insert_assertion_state(state)
    };

    match insert_result {
        Ok(()) => VerificationResult::success(registered_triggers),
        Err(error) => {
            VerificationResult::deployment_failure(format!(
                "failed to insert assertion into store: {error}"
            ))
        }
    }
}

fn insert_assertion_state(state: AssertionState) -> Result<(), AssertionStoreError> {
    let store = AssertionStore::new_ephemeral();
    store.insert(Address::ZERO, state).map(|_| ())
}

fn format_registered_triggers(trigger_recorder: &TriggerRecorder) -> BTreeMap<String, String> {
    let mut trigger_map: BTreeMap<String, Vec<TriggerDisplay>> = BTreeMap::new();

    for (trigger_type, fn_selectors) in &trigger_recorder.triggers {
        let trigger_display = map_trigger_display(trigger_type);
        for fn_selector in fn_selectors {
            let selector = format!("0x{}", hex::encode(fn_selector.as_slice()));
            trigger_map
                .entry(selector)
                .or_default()
                .push(trigger_display);
        }
    }

    trigger_map
        .into_iter()
        .map(|(selector, mut trigger_displays)| {
            trigger_displays.sort_by_key(|trigger_display| trigger_display.order);
            trigger_displays.dedup_by_key(|trigger_display| trigger_display.name);
            let formatted_triggers = trigger_displays
                .into_iter()
                .map(|trigger_display| trigger_display.name)
                .collect::<Vec<_>>()
                .join(" | ");
            (selector, formatted_triggers)
        })
        .collect()
}

#[derive(Clone, Copy)]
struct TriggerDisplay {
    order: u8,
    name: &'static str,
}

fn map_trigger_display(trigger_type: &TriggerType) -> TriggerDisplay {
    match trigger_type {
        TriggerType::Call { .. } => {
            TriggerDisplay {
                order: 0,
                name: "call",
            }
        }
        TriggerType::AllCalls => {
            TriggerDisplay {
                order: 1,
                name: "allCall",
            }
        }
        TriggerType::BalanceChange => {
            TriggerDisplay {
                order: 2,
                name: "balanceChange",
            }
        }
        TriggerType::StorageChange { .. } => {
            TriggerDisplay {
                order: 3,
                name: "storageChange",
            }
        }
        TriggerType::AllStorageChanges => {
            TriggerDisplay {
                order: 4,
                name: "allStorageChange",
            }
        }
    }
}

fn map_extraction_error(error: FnSelectorExtractorError) -> VerificationResult {
    match error {
        FnSelectorExtractorError::NoTriggersRecorded => VerificationResult::no_triggers(),
        FnSelectorExtractorError::AssertionContractDeployFailed(result_and_state)
        | FnSelectorExtractorError::TriggersCallFailed(result_and_state) => {
            let message = extract_execution_failure_message(&result_and_state)
                .unwrap_or_else(|| "assertion execution failed".to_string());
            VerificationResult::deployment_failure(message)
        }
        FnSelectorExtractorError::AssertionContractDeployError(_)
        | FnSelectorExtractorError::TriggersCallError(_)
        | FnSelectorExtractorError::AssertionContractNotFound
        | FnSelectorExtractorError::AssertionContractNoCode => {
            VerificationResult::deployment_failure(error.to_string())
        }
    }
}

fn extract_execution_failure_message(result_and_state: &ResultAndState) -> Option<String> {
    match &result_and_state.result {
        EvmExecutionResult::Revert { output, .. } => {
            if let Some(revert_reason) = decode_revert_reason(output) {
                Some(revert_reason)
            } else {
                Some(format!("execution reverted: 0x{}", hex::encode(output)))
            }
        }
        EvmExecutionResult::Halt { reason, .. } => Some(format!("execution halted: {reason:?}")),
        EvmExecutionResult::Success { .. } => None,
    }
}

fn decode_revert_reason(output: &Bytes) -> Option<String> {
    if let Ok(revert) = Revert::abi_decode(output.as_ref()) {
        return Some(revert.reason);
    }

    if let Ok(panic) = Panic::abi_decode(output.as_ref()) {
        let panic_code = u64::try_from(panic.code).ok()?;
        return Some(format!("panic code 0x{panic_code:02x}"));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertion_executor::primitives::hex;
    use serde_json::Value;
    use std::path::Path;

    fn bytecode_from_artifact(path: &str) -> Bytes {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let artifact_path = root.join(path);
        let file = std::fs::File::open(&artifact_path)
            .unwrap_or_else(|err| panic!("failed to open {}: {err}", artifact_path.display()));
        let json: Value = serde_json::from_reader(file)
            .unwrap_or_else(|err| panic!("failed to parse {}: {err}", artifact_path.display()));

        let bytecode = json["bytecode"]["object"]
            .as_str()
            .expect("artifact bytecode object should be a string");
        hex::decode(bytecode)
            .expect("artifact bytecode should be valid hex")
            .into()
    }

    #[tokio::test]
    async fn verify_assertion_success() {
        let bytecode = bytecode_from_artifact(
            "../../testdata/mock-protocol/out/SimpleCounterAssertion.sol/SimpleCounterAssertion.json",
        );
        let result = verify_assertion(&bytecode, &ExecutorConfig::default());
        assert_eq!(result.status, VerificationStatus::Success);
        assert!(
            result
                .triggers
                .as_ref()
                .is_some_and(|triggers| !triggers.is_empty())
        );
        assert_eq!(result.error, None);
    }

    #[tokio::test]
    async fn verify_assertion_no_triggers() {
        // Deployment code for a contract whose runtime bytecode is `STOP`.
        // The `triggers()` call succeeds but records no trigger.
        let bytecode: Bytes = hex::decode("6001600c60003960016000f300").unwrap().into();
        let result = verify_assertion(&bytecode, &ExecutorConfig::default());
        assert_eq!(result.status, VerificationStatus::NoTriggers);
        assert_eq!(result.triggers, None);
        assert_eq!(result.error, None);
    }

    #[tokio::test]
    async fn verify_assertion_deployment_failure() {
        // Constructor bytecode that always reverts.
        let bytecode: Bytes = hex::decode("60006000fd").unwrap().into();
        let result = verify_assertion(&bytecode, &ExecutorConfig::default());
        assert_eq!(result.status, VerificationStatus::DeploymentFailure);
        assert_eq!(result.triggers, None);
        assert!(result.error.is_some());
    }

    #[test]
    fn decode_revert_reason_error_string() {
        let revert_data = Revert::from("boom").abi_encode();
        let output: Bytes = revert_data.into();

        assert_eq!(decode_revert_reason(&output), Some("boom".to_string()));
    }

    #[test]
    fn decode_revert_reason_panic_code() {
        let revert_data = Panic::from(0x11u64).abi_encode();
        let output: Bytes = revert_data.into();

        assert_eq!(
            decode_revert_reason(&output),
            Some("panic code 0x11".to_string())
        );
    }
}
