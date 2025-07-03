use crate::{AssertionExecutor, primitives::SpecId, store::AssertionStore};

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
            spec_id: SpecId::default(),
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
    pub fn build(self, store: AssertionStore) -> AssertionExecutor {
        AssertionExecutor {
            store,
            config: self,
        }
    }
}
