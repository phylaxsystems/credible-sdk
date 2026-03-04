use crate::server::models::replay::{
    Assertion,
    ReplayRequest,
};
use alloy::primitives::B256;
use assertion_executor::{
    ExecutorConfig,
    store::{
        AssertionStore,
        AssertionStoreError,
        ExtractedContract,
        FnSelectorExtractorError,
        PendingModification,
        extract_assertion_contract,
    },
};
use std::collections::HashSet;
use thiserror::Error;
use tracing::info;

pub(super) async fn bootstrap_assertion_store(
    request: &ReplayRequest,
    start_block: u64,
    executor_config: &ExecutorConfig,
) -> Result<(AssertionStore, HashSet<B256>), BootstrapError> {
    let store = AssertionStore::new_ephemeral();
    let mut watched_assertion_ids = HashSet::new();

    let assertions = build_assertion_modifications(
        &request.assertions,
        executor_config,
        start_block,
        &mut watched_assertion_ids,
    )?;
    if !assertions.is_empty() {
        store.apply_pending_modifications(assertions)?;
    }

    info!(
        watched_assertions = watched_assertion_ids.len(),
        assertions = request.assertions.len(),
        "bootstrapped assertion store for replay"
    );

    Ok((store, watched_assertion_ids))
}

fn build_assertion_modifications(
    assertions: &[Assertion],
    executor_config: &ExecutorConfig,
    replay_start_block: u64,
    watched_assertion_ids: &mut HashSet<B256>,
) -> Result<Vec<PendingModification>, BootstrapError> {
    let synthetic_activation_block = replay_start_block.saturating_sub(1);
    let mut modifications = Vec::with_capacity(assertions.len());
    for assertion in assertions {
        let ExtractedContract {
            assertion_contract,
            trigger_recorder,
            assertion_spec,
        } = extract_assertion_contract(&assertion.deployment_bytecode, executor_config).map_err(
            |source| {
                BootstrapError::AssertionExtraction {
                    adopter: assertion.adopter,
                    source,
                }
            },
        )?;

        let expected = assertion.id.0;
        if expected != assertion_contract.id {
            return Err(BootstrapError::AssertionIdMismatch {
                expected,
                computed: assertion_contract.id,
            });
        }

        watched_assertion_ids.insert(expected);
        modifications.push(PendingModification::Add {
            assertion_adopter: assertion.adopter,
            assertion_contract,
            trigger_recorder,
            assertion_spec,
            activation_block: synthetic_activation_block,
            log_index: u64::MAX,
        });
    }

    Ok(modifications)
}

#[derive(Debug, Error)]
pub(crate) enum BootstrapError {
    #[error("failed to extract synthetic assertion for adopter {adopter}")]
    AssertionExtraction {
        adopter: alloy::primitives::Address,
        #[source]
        source: FnSelectorExtractorError,
    },
    #[error("assertion id mismatch")]
    AssertionIdMismatch { expected: B256, computed: B256 },
    #[error("failed to apply assertion modifications to in-memory store")]
    Store(#[from] AssertionStoreError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{
        Address,
        Bytes,
    };

    #[test]
    fn assertion_id_mismatch_is_rejected() {
        let assertions = vec![Assertion {
            adopter: Address::repeat_byte(0x11),
            deployment_bytecode: Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0x55]),
            id: crate::server::models::replay::AssertionId(B256::repeat_byte(0xaa)),
        }];
        let mut watched_assertion_ids = HashSet::new();

        let result = build_assertion_modifications(
            &assertions,
            &ExecutorConfig::default(),
            100,
            &mut watched_assertion_ids,
        );
        assert!(matches!(
            result,
            Err(BootstrapError::AssertionExtraction { .. }
                | BootstrapError::AssertionIdMismatch { .. })
        ));
    }
}
