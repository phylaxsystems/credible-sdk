use crate::{
    primitives::SpecId,
    store::AssertionStore,
    AssertionExecutor,
};

#[cfg(feature = "optimism")]
use crate::primitives::Bytes;
#[cfg(feature = "optimism")]
use revm::primitives::OptimismFields;

/// This function creates fields necessary to statisfy tx execution against
/// the optimism spec of the EVM.
///
/// When executing against the regular EVM the `OptimismFields` of the `TxEnv`
/// do not need to be satisfied. When using the Op spec we need to fill certain
/// fields to allow optimism to account the gas for posting the calldata.
#[cfg(feature = "optimism")]
pub fn create_optimism_fields() -> OptimismFields {
    // the `enveloped_tx` field can be default because we arent posting
    // data from the executor anywhere and is therefor not relevant.
    //
    // TODO: add a feature flag to do proper accounting.
    OptimismFields {
        enveloped_tx: Some(Bytes::default()),
        ..Default::default()
    }
}

/// Contains the configuration for the assertion executor.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub spec_id: SpecId,
    pub chain_id: u64,
    pub assertion_gas_limit: u64,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        ExecutorConfig {
            spec_id: SpecId::LATEST,
            chain_id: 1,
            assertion_gas_limit: 3_000_000,
        }
    }
}

impl ExecutorConfig {
    /// Set the assertion gas limit for the assertion executor
    pub fn with_assertion_gas_limit(mut self, gas_limit: u64) -> Self {
        self.assertion_gas_limit = gas_limit;
        self
    }

    /// Set the evm [`SpecId`] for the assertion executor
    pub fn with_spec_id(mut self, spec_id: SpecId) -> Self {
        self.spec_id = spec_id;
        self
    }

    /// Set the chain id for the assertion executor
    pub fn with_chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = chain_id;
        self
    }

    /// Build the assertion executor
    pub fn build<DB>(self, db: DB, store: AssertionStore) -> AssertionExecutor<DB> {
        AssertionExecutor {
            db,
            store,
            config: self,
        }
    }
}
