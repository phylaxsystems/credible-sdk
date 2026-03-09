//! Persisted assertion state model.
//!
//! WARNING: This struct is serialized to sled via bincode.
//! Any field change requires a new migration in `store::migration`.

use crate::{
    ExecutorConfig,
    inspectors::{
        TriggerRecorder,
        spec_recorder::AssertionSpec,
    },
    primitives::{
        AssertionContract,
        B256,
        Bytes,
    },
    store::assertion_contract_extractor::{
        ExtractedContract,
        FnSelectorExtractorError,
        extract_assertion_contract,
    },
};

use serde::{
    Deserialize,
    Serialize,
};

/// Represents the persisted state of a single assertion for a given adopter.
///
/// WARNING: This struct is persisted to sled via bincode. Any field change requires a new
/// migration in `store::migration`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct AssertionState {
    pub activation_block: u64,
    pub inactivation_block: Option<u64>,
    pub assertion_contract: AssertionContract,
    pub trigger_recorder: TriggerRecorder,
    pub assertion_spec: AssertionSpec,
}

impl AssertionState {
    /// Creates a new active assertion state.
    /// Will be active across all blocks.
    ///
    /// # Errors
    ///
    /// Returns an error if the assertion contract cannot be deployed or its triggers cannot be
    /// extracted.
    pub fn new_active(
        bytecode: &Bytes,
        executor_config: &ExecutorConfig,
    ) -> Result<Self, FnSelectorExtractorError> {
        let ExtractedContract {
            assertion_contract,
            trigger_recorder,
            assertion_spec,
        } = extract_assertion_contract(bytecode, executor_config)?;
        Ok(Self {
            activation_block: 0,
            inactivation_block: None,
            assertion_contract,
            trigger_recorder,
            assertion_spec,
        })
    }

    #[cfg(any(test, feature = "test"))]
    /// # Panics
    ///
    /// Panics if the assertion contract cannot be initialized.
    pub fn new_test(bytecode: &Bytes) -> Self {
        Self::new_active(bytecode, &ExecutorConfig::default()).unwrap()
    }

    /// Override the assertion spec on this state.
    #[must_use]
    pub fn with_spec(mut self, spec: AssertionSpec) -> Self {
        self.assertion_spec = spec;
        self
    }

    /// Getter for the `assertion_contract_id`
    pub fn assertion_contract_id(&self) -> B256 {
        self.assertion_contract.id
    }
}
