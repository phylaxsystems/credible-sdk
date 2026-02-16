mod assertion_contract_extractor;
pub use assertion_contract_extractor::{
    FnSelectorExtractorError,
    extract_assertion_contract,
    triggersCall,
};

pub use assertion_da_client::{
    DaClient,
    DaClientError,
};

mod assertion_store;
pub use assertion_store::{
    AssertionState,
    AssertionStore,
    AssertionStoreError,
    AssertionsForExecution,
    PruneConfig,
};

mod indexer;
pub use indexer::{
    BlockTag,
    Indexer,
    IndexerCfg,
    IndexerError,
};

use crate::{
    inspectors::TriggerRecorder,
    primitives::{
        Address,
        AssertionContract,
        B256,
    },
};

use serde::{
    Deserialize,
    Serialize,
};

/// Information about an assertion modification event.
/// These events are indexed from the state oracle contract logs.
/// The `Add` variant also has had the assertion bytecode extracted from the DA layer, and the
/// `AssertionContract` has been extracted from the bytecode.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum PendingModification {
    Add {
        assertion_adopter: Address,
        assertion_contract: AssertionContract,
        trigger_recorder: TriggerRecorder,
        activation_block: u64,
        log_index: u64,
    },
    Remove {
        assertion_adopter: Address,
        assertion_contract_id: B256,
        inactivation_block: u64,
        log_index: u64,
    },
}

impl PendingModification {
    pub fn assertion_adopter(&self) -> Address {
        match self {
            PendingModification::Add {
                assertion_adopter, ..
            }
            | PendingModification::Remove {
                assertion_adopter, ..
            } => *assertion_adopter,
        }
    }
    pub fn assertion_contract_id(&self) -> B256 {
        match self {
            PendingModification::Add {
                assertion_contract, ..
            } => assertion_contract.id,
            PendingModification::Remove {
                assertion_contract_id,
                ..
            } => *assertion_contract_id,
        }
    }
}
