use alloy_consensus::BlockHeader;
use alloy_provider::{
    Provider,
    RootProvider,
};
use alloy_rpc_types::BlockNumHash;

use alloy_network::BlockResponse;

use alloy_network_primitives::HeaderResponse;

use alloy_transport::TransportError;
use tracing::instrument;

use crate::primitives::UpdateBlock;

#[derive(Debug, thiserror::Error)]
pub enum CheckIfReorgedError {
    #[error("Parent block not found")]
    ParentBlockNotFound,
    #[error("TransportError: {0}")]
    TransportError(#[source] TransportError),
}

/// Checks if the new block is part of the same chain as the last indexed block.
///
/// # Errors
///
/// Returns an error if the parent block cannot be found or the provider fails.
#[instrument(skip(provider))]
pub async fn check_if_reorged(
    provider: &RootProvider,
    update_block: &UpdateBlock,
    last_indexed_block: BlockNumHash,
) -> Result<bool, CheckIfReorgedError> {
    // If the new block number is the same as the last indexed block number, then
    // block is part of the same chain if the hashes are the same.
    if update_block.block_number == last_indexed_block.number {
        return Ok(update_block.block_hash != last_indexed_block.hash);
    }

    // Traverse parent blocks from the new block until the parent blocks number is equal to
    // the last indexed block number.
    let mut cursor_hash = update_block.parent_hash;
    loop {
        let cursor = provider
            .get_block_by_hash(cursor_hash)
            .await
            .map_err(CheckIfReorgedError::TransportError)?
            .ok_or(CheckIfReorgedError::ParentBlockNotFound)?;

        let cursor_header = cursor.header();

        cursor_hash = cursor_header.parent_hash();

        // Continue if the parent block
        if cursor_header.number() != last_indexed_block.number {
            continue;
        }

        return Ok(cursor_header.hash() != last_indexed_block.hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        anvil_provider,
        mine_block,
    };

    use alloy_provider::ext::AnvilApi;

    #[tokio::test]
    async fn test_check_if_reorged_no_reorg() {
        let (provider, _anvil) = anvil_provider().await;

        // Mine initial block
        let block0 = provider
            .get_block_by_number(0.into())
            .await
            .unwrap()
            .unwrap();

        // Mine next block
        let block1 = mine_block(&provider).await;

        // Check if reorged - should be false since blocks are in sequence
        let is_reorged = check_if_reorged(
            &provider,
            &UpdateBlock {
                block_number: block1.number,
                block_hash: block1.hash,
                parent_hash: block1.parent_hash,
            },
            BlockNumHash {
                number: 0,
                hash: block0.header.hash,
            },
        )
        .await
        .unwrap();

        assert!(!is_reorged);
    }

    #[tokio::test]
    async fn test_check_if_reorged_with_reorg() {
        let (provider, _anvil) = anvil_provider().await;

        let block = mine_block(&provider).await;

        // Simulate reorg by modifying anvil state
        let reorg_options = alloy_rpc_types_anvil::ReorgOptions {
            depth: 1,
            tx_block_pairs: vec![],
        };
        provider.anvil_reorg(reorg_options).await.unwrap();

        // Mine new block on different fork
        let new_block = mine_block(&provider).await;

        // Check if reorged - should be true since we created a new fork
        let is_reorged = check_if_reorged(
            &provider,
            &UpdateBlock {
                block_number: new_block.number,
                block_hash: new_block.hash,
                parent_hash: new_block.parent_hash,
            },
            BlockNumHash {
                number: 1,
                hash: block.hash,
            },
        )
        .await
        .unwrap();

        assert!(is_reorged);
    }
}
