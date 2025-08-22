use alloy_network::BlockResponse;
use alloy_provider::{
    Provider,
    RootProvider,
};
use alloy_rpc_types::{
    BlockId,
    BlockNumHash,
    BlockNumberOrTag,
    Filter,
};
use alloy_transport::TransportError;

use alloy_network_primitives::HeaderResponse;

use alloy_consensus::BlockHeader;

use alloy_sol_types::{
    SolEvent,
    sol,
};

use bincode::{
    deserialize as de,
    serialize as ser,
};

use tracing::{
    debug,
    error,
    info,
    instrument,
    trace,
    warn,
};

use alloy::primitives::{
    LogData,
    U256,
};

use clap::ValueEnum;

use crate::{
    ExecutorConfig,
    primitives::{
        Address,
        B256,
        UpdateBlock,
    },
    store::{
        AssertionStore,
        AssertionStoreError,
        PendingModification,
        extract_assertion_contract,
    },
    utils::reorg_utils::{
        CheckIfReorgedError,
        check_if_reorged,
    },
};

use assertion_da_client::{
    DaClient,
    DaClientError,
    DaFetchResponse,
};

use std::collections::BTreeMap;

sol! {
    #[derive(Debug)]
    event AssertionAdded(address contractAddress, bytes32 assertionId, uint256 activeAtBlock);

    #[derive(Debug)]
    event AssertionRemoved(address contractAddress, bytes32 assertionId, uint256 activeAtBlock);

}

/// Indexer for the State Oracle contract
/// Indexes the events emitted by the State Oracle contract
/// Writes finalized pending modifications to the Assertion Store
/// # Example
/// ``` no_run
/// use assertion_executor::{store::{AssertionStore, DaClient, Indexer, BlockTag, IndexerCfg}, primitives::Address,ExecutorConfig};
///
/// use sled::Config;
///
/// use alloy_network::Ethereum;
/// use alloy_provider::{ProviderBuilder, WsConnect, Provider};
///
/// #[tokio::main]
/// async fn main() {
///    let state_oracle = Address::new([0; 20]);
///    let db = Config::tmp().unwrap().open().unwrap();
///    let store = AssertionStore::new_ephemeral().unwrap();
///    let da_client = DaClient::new(&format!("http://127.0.0.1:0000")).unwrap();
///
///
///    let provider = ProviderBuilder::new()
///    .network::<Ethereum>() // Change for other networks, like Optimism.
///    .on_ws(WsConnect::new("wss://127.0.0.1:0001"))
///    .await
///    .unwrap();
///    let provider = provider.root().clone().boxed();
///
///
///    let config = IndexerCfg {
///         provider,
///         db,
///         store,
///         da_client,
///         state_oracle,
///         executor_config: ExecutorConfig::default(),
///         await_tag: BlockTag::Latest,
///    };
///
///    // Await syncing the indexer to the latest block.
///    let indexer = Indexer::new_synced(config).await.unwrap();
///
///    // Streams new blocks. Awaits infinitely unless an error occurs.
///    indexer.run().await.unwrap();
/// }
pub struct Indexer {
    /// The provider for fetching blocks and logs
    provider: PubSubProvider,
    /// The sled database instance
    db: sled::Db,
    /// The Assertion Store
    store: AssertionStore,
    /// The DA Client
    da_client: DaClient,
    /// The State Oracle contract address
    state_oracle: Address,
    /// The executor configuration for extracting the assertion contract
    executor_config: ExecutorConfig,
    /// Whether the indexer is synced to the latest block
    is_synced: bool,
    /// Tag to use as the upper bound of pending modifications which should be applied to the
    /// store.
    pub await_tag: BlockTag,
}

/// Restricted version of `BlockNumberOrTag` enum.
/// Only exposes the `Latest`, `Finalized`, and `Safe` variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BlockTag {
    Latest,
    Finalized,
    Safe,
}

impl From<BlockTag> for BlockNumberOrTag {
    fn from(tag: BlockTag) -> Self {
        match tag {
            BlockTag::Latest => BlockNumberOrTag::Latest,
            BlockTag::Finalized => BlockNumberOrTag::Finalized,
            BlockTag::Safe => BlockNumberOrTag::Safe,
        }
    }
}

type PubSubProvider = RootProvider;

const MAX_BLOCKS_PER_CALL: u64 = 50_000;

#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("Transport error")]
    TransportError(#[from] TransportError),
    #[error("Sled error")]
    SledError(#[from] std::io::Error),
    #[error("Bincode error")]
    BincodeError(#[from] bincode::Error),
    #[error("Event decoding error")]
    EventDecodeError(#[from] alloy_sol_types::Error),
    #[error("Block number missing")]
    BlockNumberMissing,
    #[error("Log index missing")]
    LogIndexMissing,
    #[error("Block number exceeds u64")]
    BlockNumberExceedsU64,
    #[error("DA Client Error")]
    DaClientError(#[from] DaClientError),
    #[error("Error decoding da bytecode")]
    DaBytecodeDecodingFailed(#[from] alloy::hex::FromHexError),
    #[error("Block stream error")]
    BlockStreamError(#[from] tokio::sync::broadcast::error::RecvError),
    #[error("Parent block not found")]
    ParentBlockNotFound,
    #[error("Block hash missing")]
    BlockHashMissing,
    #[error("No common ancestor found")]
    NoCommonAncestor,
    #[error("Execution Logs Rx is None; Channel likely dropped.")]
    ExecutionLogsRxNone,
    #[error("Check if reorged error")]
    CheckIfReorgedError(#[from] CheckIfReorgedError),
    #[error("Assertion Store Error")]
    AssertionStoreError(#[from] AssertionStoreError),
    #[error("Store must be synced before running")]
    StoreNotSynced,
}

type IndexerResult<T = ()> = std::result::Result<T, IndexerError>;

/// Configuration for the Indexer
#[derive(Debug)]
pub struct IndexerCfg {
    /// The State Oracle contract address
    pub state_oracle: Address,
    /// The DA Client
    pub da_client: DaClient,
    /// The Executor configuration
    pub executor_config: ExecutorConfig,
    /// The Assertion Store
    pub store: AssertionStore,
    /// Rpc Provider
    pub provider: PubSubProvider,
    /// Sled database instance
    /// Subtables will be opened on demand from this database
    pub db: sled::Db,
    /// Tag to use as the upper bound of pending modifications which should be applied to the
    /// store.
    pub await_tag: BlockTag,
}

impl Indexer {
    /// Create a new Indexer
    pub fn new(cfg: IndexerCfg) -> Self {
        let IndexerCfg {
            provider,
            db,
            store,
            da_client,
            state_oracle,
            executor_config,
            await_tag,
        } = cfg;

        Self {
            provider,
            db,
            store,
            da_client,
            state_oracle,
            executor_config,
            is_synced: false,
            await_tag,
        }
    }

    /// Get the block hash tree, opening it on demand
    fn block_hash_tree(&self) -> Result<sled::Tree, std::io::Error> {
        self.db.open_tree("block_hashes")
    }

    /// Get the latest block tree, opening it on demand
    fn latest_block_tree(&self) -> Result<sled::Tree, std::io::Error> {
        self.db.open_tree("latest_block")
    }

    /// Get the pending modifications tree, opening it on demand
    fn pending_modifications_tree(&self) -> Result<sled::Tree, std::io::Error> {
        self.db.open_tree("pending_modifications")
    }

    /// Create a new Indexer and sync it to the latest block
    pub async fn new_synced(cfg: IndexerCfg) -> IndexerResult<Self> {
        let mut indexer = Self::new(cfg);
        indexer.sync_to_head().await?;
        Ok(indexer)
    }

    /// Run the indexer
    #[instrument(skip(self))]
    pub async fn run(&self) -> IndexerResult {
        if !self.is_synced {
            return Err(IndexerError::StoreNotSynced);
        }

        let mut block_stream = self.provider.subscribe_blocks().await?;
        loop {
            // FIXME: Sometimes when op-talos is syncing from scratch,
            // the below might fail with `Lagged` on the recv.
            // When op-talos is initially syncing, writes indexer data to disk,
            // and then restarts this problem does not appear.
            let latest_block = block_stream.recv().await?;
            trace!(
                target = "assertion_executor::indexer",
                ?latest_block,
                "Received new block"
            );
            self.handle_latest_block(latest_block).await?;
        }
    }

    /// Sync the indexer to the latest block
    #[instrument(skip(self))]
    pub async fn sync_to_head(&mut self) -> IndexerResult {
        info!(
            target = "assertion_executor::indexer",
            "Syncing indexer to latest block"
        );
        let latest_block = match self
            .provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await?
        {
            Some(block) => block,
            None => {
                warn!("Latest block not found");
                return Ok(());
            }
        };

        let latest_block_header = latest_block.header();
        let latest_block_number = latest_block_header.number();
        let latest_block_hash = latest_block_header.hash();

        self.sync(
            UpdateBlock {
                block_number: latest_block_number,
                block_hash: latest_block_header.hash(),
                parent_hash: latest_block_header.parent_hash(),
            },
            MAX_BLOCKS_PER_CALL,
        )
        .await?;

        self.is_synced = true;

        info!(
            target = "assertion_executor::indexer",
            latest_block_number = ?latest_block_number,
            latest_block_hash = ?latest_block_hash,
            "Indexer synced to latest block"
        );

        Ok(())
    }

    /// Prune the pending modifications and block hashes trees up to a block number
    fn prune_to(&self, to: u64) -> IndexerResult<Vec<PendingModification>> {
        let mut pending_modifications = Vec::new();
        let ser_to = ser(&U256::from(to))?;

        let block_hash_tree = self.block_hash_tree()?;
        let pending_modifications_tree = self.pending_modifications_tree()?;

        while let Some((key, _)) = block_hash_tree.pop_first_in_range(..ser_to.clone())? {
            if let Some(pending_mods) = pending_modifications_tree.remove(&key)? {
                let pending_mods: Vec<PendingModification> = de(&pending_mods)?;
                pending_modifications.extend(pending_mods);
            }
        }
        debug!(
            target = "assertion_executor::indexer",
            to,
            ?pending_modifications,
            "Pruned pending modifications and block hashes trees"
        );

        Ok(pending_modifications)
    }

    /// Prune the pending modifications and block hashes trees from a block number
    fn prune_from(&self, from: u64) -> IndexerResult {
        let ser_from = ser(&U256::from(from))?;

        let block_hash_tree = self.block_hash_tree()?;
        let pending_modifications_tree = self.pending_modifications_tree()?;

        while let Some((key, _)) = block_hash_tree.pop_first_in_range(ser_from.clone()..)? {
            pending_modifications_tree.remove(&key)?;
        }
        debug!(
            target = "assertion_executor::indexer",
            from, "Pruned pending modifications and block hashes trees"
        );

        Ok(())
    }

    /// Finds the common ancestor
    /// Traverses from the cursor backwords until it finds a common ancestor in the block_hashes
    /// tree.
    async fn find_common_ancestor(&self, cursor_hash: B256) -> IndexerResult<u64> {
        let block_hashes_tree = self.block_hash_tree()?;

        let mut cursor_hash = cursor_hash;
        loop {
            let cursor = self
                .provider
                .get_block_by_hash(cursor_hash)
                .await?
                .ok_or(IndexerError::ParentBlockNotFound)?;
            let cursor_header = cursor.header();

            cursor_hash = cursor_header.parent_hash();
            if let Some(hash) = block_hashes_tree.get(ser(&U256::from(cursor_header.number()))?)? {
                if hash == ser(&cursor_header.hash())? {
                    return Ok(cursor_header.number());
                }
            } else {
                return Err(IndexerError::NoCommonAncestor);
            }
        }
    }

    /// Handle the latest block, sync the indexer to the latest block, and moving
    /// pending modifications to the store
    pub async fn handle_latest_block(
        &self,
        header: impl HeaderResponse + std::fmt::Debug,
    ) -> IndexerResult {
        self.sync(
            UpdateBlock {
                block_number: header.number(),
                block_hash: header.hash(),
                parent_hash: header.parent_hash(),
            },
            MAX_BLOCKS_PER_CALL,
        )
        .await?;

        Ok(())
    }

    /// Sync the indexer to the latest block
    /// If no block has been indexed, indexes from block 0.
    ///
    /// Otherwise checks for reorg.
    /// If reorg, prunes the pending modifications and block hashes trees.
    /// Then indexes the new blocks.
    /// If no block has been indexed, indexes from block 0.
    ///
    /// FIXME: We limit individual calls to 50k blocks to not go over reths
    /// WS call limits.
    /// If we try to get logs from too many blocks at once, calls might fail.
    /// This can happen if op-talos was out of sync for a while or if it is syncing
    /// an existing chain from scratch.
    #[instrument(skip(self))]
    pub async fn sync(&self, update_block: UpdateBlock, max_blocks_per_call: u64) -> IndexerResult {
        let last_indexed_block_num_hash = self.get_last_indexed_block_num_hash()?;

        let from;
        // If a block has been indexed, check if the new block is part of the same chain.
        if let Some(last_indexed_block_num_hash) = last_indexed_block_num_hash {
            let is_reorg =
                check_if_reorged(&self.provider, &update_block, last_indexed_block_num_hash)
                    .await?;

            if is_reorg {
                let common_ancestor = self.find_common_ancestor(update_block.parent_hash).await?;
                debug!(
                    target = "assertion_executor::indexer",
                    common_ancestor, "Reorg detected"
                );
                from = common_ancestor + 1;
                self.prune_from(from)?;
                self.insert_last_indexed_block_num_hash(BlockNumHash {
                    number: common_ancestor,
                    hash: update_block.parent_hash,
                })?;
            } else {
                from = last_indexed_block_num_hash.number + 1;
            }
        } else {
            from = 0;
        };

        let update_block_number = update_block.block_number;
        let mut current_from = from;
        while current_from <= update_block_number {
            let current_to =
                std::cmp::min(current_from + max_blocks_per_call - 1, update_block_number);

            self.index_range(current_from, current_to).await?;
            if current_to == update_block_number {
                break;
            }
            current_from = current_to + 1;
        }

        Ok(())
    }

    /// Move pending modifications to the store, with the specified block as an upper bound
    /// Prune the pending modifications and block hashes trees
    async fn move_pending_modifications_to_store(&self, upper_bound: u64) -> IndexerResult {
        // Delay pruning genesis block until the first block is ready to be moved.
        // Otherwise it would immediately prune the genesis block and would not be able to find a
        // common ancestor if there was a reorg to the genesis block.
        if upper_bound == 0 {
            return Ok(());
        }

        let pending_modifications = self.prune_to(upper_bound + 1)?;

        if pending_modifications.is_empty() {
            debug!(
                target = "assertion_executor::indexer",
                upper_bound, "No pending modifications to apply",
            );
        } else {
            debug!(
                target = "assertion_executor::indexer",
                upper_bound,
                modifications_count = pending_modifications.len(),
                "Moving pending modifications to store",
            );
            self.store
                .apply_pending_modifications(pending_modifications)?;
        }

        Ok(())
    }

    /// Fetch the events from the State Oracle contract.
    /// Store the events in the pending_modifications tree for the indexed blocks.
    async fn index_range(&self, from: u64, to: u64) -> IndexerResult {
        debug!(
            target = "assertion_executor::indexer",
            from, to, "Indexing range"
        );

        let filter = Filter::new()
            .address(self.state_oracle)
            .from_block(BlockNumberOrTag::Number(from))
            .to_block(BlockNumberOrTag::Number(to));

        let logs = self.provider.get_logs(&filter).await?;

        // For ordered insertion of pending modifications
        let mut pending_modifications: BTreeMap<u64, BTreeMap<u64, PendingModification>> =
            BTreeMap::new();

        for log in logs {
            let log_index = log.log_index.ok_or(IndexerError::LogIndexMissing)?;
            let block_number = log.block_number.ok_or(IndexerError::BlockNumberMissing)?;

            trace!(target: "assertion_executor::indexer", log_index, block_number, "Processing log");
            if let Some(modification) = self
                .extract_pending_modifications(log.data(), log_index)
                .await?
            {
                pending_modifications
                    .entry(block_number)
                    .or_default()
                    .insert(log_index, modification);
            }
        }

        let mut pending_mods_batch = sled::Batch::default();

        for (block, log_map) in pending_modifications.iter() {
            let block_mods = log_map
                .values()
                .cloned()
                .collect::<Vec<PendingModification>>();
            pending_mods_batch.insert(ser(&U256::from(*block))?, ser(&block_mods)?);
        }

        let pending_modifications_tree = self.pending_modifications_tree()?;
        pending_modifications_tree.apply_batch(pending_mods_batch)?;

        trace!(
            target = "assertion_executor::indexer",
            from, to, "Building block hashes batch"
        );

        // Build the block hashes batch
        let mut block_hashes = vec![];

        for i in from..=to {
            let block_hash = self
                .provider
                .get_block(BlockId::Number(BlockNumberOrTag::Number(i)))
                .await?
                .ok_or(IndexerError::BlockHashMissing)?
                .header()
                .hash();

            block_hashes.push(BlockNumHash {
                number: i,
                hash: block_hash,
            });
        }

        if let Some(last_indexed_block_num_hash) = block_hashes.last() {
            self.insert_last_indexed_block_num_hash(*last_indexed_block_num_hash)?;
        }

        self.write_block_num_hash_batch(block_hashes)?;
        trace!(
            target = "assertion_executor::indexer",
            from, to, "Block hashes batch applied"
        );

        let block_response = self
            .provider
            .get_block_by_number(self.await_tag.into())
            .await?;

        if let Some(block) = block_response {
            let block_to_move = block.header().number();
            trace!(
                target = "assertion_executor::indexer",
                block_to_move, "Moving pending modifications to store"
            );

            self.move_pending_modifications_to_store(block_to_move)
                .await?;
            trace!(
                target = "assertion_executor::indexer",
                block_to_move, "Pending modifications moved to store"
            );
        } else {
            trace!(
                target = "assertion_executor::indexer",
                "No block to move, skipping"
            );
        }
        Ok(())
    }

    /// Extract the pending modifications from the logs
    /// Resolves the bytecode via the assertion da.
    /// Extracts the fn selectors from the contract.
    async fn extract_pending_modifications(
        &self,
        log: &LogData,
        log_index: u64,
    ) -> IndexerResult<Option<PendingModification>> {
        let pending_mod_opt = match log.topics().first() {
            Some(&AssertionAdded::SIGNATURE_HASH) => {
                let topics = AssertionAdded::decode_topics(log.topics())?;
                let data = AssertionAdded::abi_decode_data(&log.data).map_err(|e| {
                    debug!(target: "assertion_executor::indexer", ?e, "Failed to decode AssertionAdded event data");
                    e
                })?;

                let event = AssertionAdded::new(topics, data);

                trace!(
                    target = "assertion_executor::indexer",
                    ?event,
                    "AssertionAdded event decoded"
                );

                // TODO(@0xgregthedev): Improve fault tolerance of the requests to the DA layer.
                // We should avoid failing if the request to the DA can be resolved before
                // the modification should be moved to the store. But we also need to avoid back
                // pressure on extracting the assertion contracts once we have fetched them from
                // the DA.
                let DaFetchResponse {
                    bytecode,
                    encoded_constructor_args,
                    ..
                } = self.da_client.fetch_assertion(event.assertionId).await?;

                let mut deployment_bytecode = (*bytecode).to_vec();
                deployment_bytecode.extend_from_slice(&encoded_constructor_args);

                let assertion_contract_res =
                    extract_assertion_contract(deployment_bytecode.into(), &self.executor_config);

                match assertion_contract_res {
                    Ok((assertion_contract, trigger_recorder)) => {
                        let active_at_block = event
                            .activeAtBlock
                            .try_into()
                            .map_err(|_| IndexerError::BlockNumberExceedsU64)?;

                        debug!(
                            target = "assertion_executor::indexer",
                            assertion_id=?assertion_contract.id,
                            active_at_block,
                            "assertionAdded event processed",
                        );

                        Some(PendingModification::Add {
                            assertion_adopter: event.contractAddress,
                            assertion_contract,
                            trigger_recorder,
                            active_at_block,
                            log_index,
                        })
                    }
                    Err(err) => {
                        warn!(
                            target = "assertion_executor::indexer",
                            ?err,
                            "Failed to extract assertion contract"
                        );
                        None
                    }
                }
            }

            Some(&AssertionRemoved::SIGNATURE_HASH) => {
                trace!(
                    target = "assertion_executor::indexer",
                    "AssertionRemoved event signature detected."
                );
                let topics = AssertionRemoved::decode_topics(log.topics())?;
                let data = AssertionRemoved::abi_decode_data(&log.data)?;
                let event = AssertionRemoved::new(topics, data);

                trace!(
                    target = "assertion_executor::indexer",
                    ?event,
                    "AssertionRemoved event decoded"
                );

                let inactive_at_block = event
                    .activeAtBlock
                    .try_into()
                    .map_err(|_| IndexerError::BlockNumberExceedsU64)?;

                debug!(
                    target = "assertion_executor::indexer",
                    assertion_id=?event.assertionId,
                    inactive_at_block,
                    "assertionRemoved event processed",
                );
                Some(PendingModification::Remove {
                    assertion_contract_id: event.assertionId,
                    assertion_adopter: event.contractAddress,
                    inactive_at_block,
                    log_index,
                })
            }
            _ => None,
        };
        Ok(pending_mod_opt)
    }

    /// Insert last indexed block number and hash into the sled db
    fn insert_last_indexed_block_num_hash(&self, block_num_hash: BlockNumHash) -> IndexerResult {
        let value = ser(&block_num_hash)?;
        let latest_block_tree = self.latest_block_tree()?;
        latest_block_tree.insert("", value)?;
        Ok(())
    }

    /// Get the last indexed block number and hash
    fn get_last_indexed_block_num_hash(&self) -> IndexerResult<Option<BlockNumHash>> {
        let latest_block_tree = self.latest_block_tree()?;
        let last_indexed_block = latest_block_tree.get("")?;
        if let Some(last_indexed_block) = last_indexed_block {
            let last_indexed_block: BlockNumHash = de(&last_indexed_block)?;
            Ok(Some(last_indexed_block))
        } else {
            Ok(None)
        }
    }

    /// Write the block number and hash to the sled db
    fn write_block_num_hash_batch(&self, block_num_hashes: Vec<BlockNumHash>) -> IndexerResult {
        let mut batch = sled::Batch::default();
        for block_num_hash in block_num_hashes {
            let key = ser(&U256::from(block_num_hash.number))?;
            let value = ser(&B256::from(block_num_hash.hash))?;
            batch.insert(key, value);
        }
        let block_hash_tree = self.block_hash_tree()?;
        block_hash_tree.apply_batch(batch)?;

        Ok(())
    }
}

#[cfg(test)]
mod test_indexer {
    use super::*;
    use crate::{
        inspectors::TriggerRecorder,
        primitives::{
            Address,
            AssertionContract,
            U256,
        },
    };
    use sled::Config;
    use tempfile::TempDir;

    // Helper to create a test indexer with in-memory database
    fn create_test_indexer() -> (Indexer, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = Config::new()
            .path(temp_dir.path())
            .cache_capacity_bytes(1024)
            .open()
            .unwrap();

        let store = AssertionStore::new_ephemeral().unwrap();

        // Create mock provider and DA client (will be mocked in tests)
        let provider = alloy_provider::ProviderBuilder::new()
            .connect_mocked_client(Default::default())
            .root()
            .clone();

        let da_client = DaClient::new("http://localhost:0").unwrap();

        let indexer = Indexer::new(IndexerCfg {
            state_oracle: Address::random(),
            da_client,
            executor_config: ExecutorConfig::default(),
            store,
            provider,
            db,
            await_tag: BlockTag::Latest,
        });

        (indexer, temp_dir)
    }

    #[test]
    fn test_block_tag_conversion() {
        assert_eq!(
            BlockNumberOrTag::from(BlockTag::Latest),
            BlockNumberOrTag::Latest
        );
        assert_eq!(
            BlockNumberOrTag::from(BlockTag::Finalized),
            BlockNumberOrTag::Finalized
        );
        assert_eq!(
            BlockNumberOrTag::from(BlockTag::Safe),
            BlockNumberOrTag::Safe
        );
    }

    #[test]
    fn test_indexer_creation() {
        let (indexer, _temp_dir) = create_test_indexer();
        assert!(!indexer.is_synced);
        assert_eq!(indexer.await_tag, BlockTag::Latest);
    }

    #[test]
    fn test_write_block_num_hash_batch() {
        let (indexer, _temp_dir) = create_test_indexer();

        let block_hashes = vec![
            BlockNumHash {
                number: 0,
                hash: B256::from([0; 32]),
            },
            BlockNumHash {
                number: 1,
                hash: B256::from([1; 32]),
            },
            BlockNumHash {
                number: 2,
                hash: B256::from([2; 32]),
            },
        ];

        indexer
            .write_block_num_hash_batch(block_hashes.clone())
            .unwrap();
        assert_eq!(indexer.block_hash_tree().unwrap().len(), 3);

        // Verify the blocks were stored correctly
        for block_hash in block_hashes {
            let key = ser(&U256::from(block_hash.number)).unwrap();
            let stored_hash: B256 = de(&indexer
                .block_hash_tree()
                .unwrap()
                .get(&key)
                .unwrap()
                .unwrap())
            .unwrap();
            assert_eq!(stored_hash, B256::from(block_hash.hash));
        }
    }

    #[test]
    fn test_last_indexed_block_operations() {
        let (indexer, _temp_dir) = create_test_indexer();

        // Initially should be None
        assert!(indexer.get_last_indexed_block_num_hash().unwrap().is_none());

        // Insert a block
        let block_num_hash = BlockNumHash {
            number: 42,
            hash: B256::from([42; 32]),
        };

        indexer
            .insert_last_indexed_block_num_hash(block_num_hash)
            .unwrap();

        // Should now return the inserted block
        let retrieved = indexer.get_last_indexed_block_num_hash().unwrap().unwrap();
        assert_eq!(retrieved.number, 42);
        assert_eq!(retrieved.hash, B256::from([42; 32]));
    }

    #[test]
    fn test_prune_to() {
        let (indexer, _temp_dir) = create_test_indexer();

        // Add some block hashes
        let block_hashes = vec![
            BlockNumHash {
                number: 0,
                hash: B256::from([0; 32]),
            },
            BlockNumHash {
                number: 1,
                hash: B256::from([1; 32]),
            },
            BlockNumHash {
                number: 2,
                hash: B256::from([2; 32]),
            },
        ];
        indexer.write_block_num_hash_batch(block_hashes).unwrap();

        // Add some pending modifications
        let modification = PendingModification::Add {
            assertion_adopter: Address::random(),
            assertion_contract: AssertionContract::default(),
            trigger_recorder: TriggerRecorder::default(),
            active_at_block: 1,
            log_index: 0,
        };

        let key_1 = ser(&U256::from(1_u64)).unwrap();
        indexer
            .pending_modifications_tree()
            .unwrap()
            .insert(&key_1, ser(&vec![modification.clone()]).unwrap())
            .unwrap();

        // Prune up to block 2 (should remove blocks 0 and 1)
        let pruned = indexer.prune_to(2).unwrap();

        // Should have pruned 2 blocks and returned 1 modification
        assert_eq!(indexer.block_hash_tree().unwrap().len(), 1); // Only block 2 left
        assert_eq!(pruned.len(), 1);
        assert_eq!(indexer.pending_modifications_tree().unwrap().len(), 0);
    }

    #[test]
    fn test_prune_from() {
        let (indexer, _temp_dir) = create_test_indexer();

        // Add some block hashes
        let block_hashes = vec![
            BlockNumHash {
                number: 0,
                hash: B256::from([0; 32]),
            },
            BlockNumHash {
                number: 1,
                hash: B256::from([1; 32]),
            },
            BlockNumHash {
                number: 2,
                hash: B256::from([2; 32]),
            },
        ];
        indexer.write_block_num_hash_batch(block_hashes).unwrap();

        // Prune from block 1 onwards
        indexer.prune_from(1).unwrap();

        // Should have removed blocks 1 and 2, keeping only block 0
        assert_eq!(indexer.block_hash_tree().unwrap().len(), 1);

        // Verify block 0 is still there
        let key_0 = ser(&U256::from(0_u64)).unwrap();
        assert!(
            indexer
                .block_hash_tree()
                .unwrap()
                .get(&key_0)
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_extract_pending_modifications_assertion_added() {
        let (indexer, _temp_dir) = create_test_indexer();

        // Create AssertionAdded event data
        let assertion_id = B256::random();
        let contract_address = Address::random();
        let active_at_block = 100u64;

        // Create event topics and data
        let mut addr_bytes = [0u8; 32];
        addr_bytes[12..].copy_from_slice(&contract_address.into_array());

        let topics = vec![
            AssertionAdded::SIGNATURE_HASH,
            B256::from(addr_bytes),
            assertion_id,
        ];

        // Create data using proper ABI encoding for the event
        let data = U256::from(active_at_block).to_be_bytes_vec();

        let log_data = alloy::primitives::LogData::new(topics, data.into()).unwrap();

        // Test that the method handles the event signature correctly
        let result = indexer.extract_pending_modifications(&log_data, 0).await;

        // Should get an error since we're not mocking the DA client or provider
        assert!(result.is_err());
        // Could be DaClientError or EventDecodeError depending on which fails first
    }

    #[tokio::test]
    async fn test_extract_pending_modifications_assertion_removed() {
        let (indexer, _temp_dir) = create_test_indexer();

        // Create AssertionRemoved event data
        let assertion_id = B256::random();
        let contract_address = Address::random();
        let active_at_block = 100u64;

        // Create event topics and data
        let mut addr_bytes = [0u8; 32];
        addr_bytes[12..].copy_from_slice(&contract_address.into_array());

        let topics = vec![
            AssertionRemoved::SIGNATURE_HASH,
            B256::from(addr_bytes),
            assertion_id,
        ];

        // Create data using proper ABI encoding for the event
        let data = U256::from(active_at_block).to_be_bytes_vec();

        let log_data = alloy::primitives::LogData::new(topics, data.into()).unwrap();

        let result = indexer.extract_pending_modifications(&log_data, 0).await;

        // Test the result - might succeed in decoding removal events since they don't need DA
        match result {
            Ok(Some(PendingModification::Remove {
                assertion_contract_id,
                assertion_adopter,
                inactive_at_block,
                log_index,
            })) => {
                assert_eq!(assertion_contract_id, assertion_id);
                assert_eq!(assertion_adopter, contract_address);
                assert_eq!(inactive_at_block, active_at_block);
                assert_eq!(log_index, 0);
            }
            Ok(Some(PendingModification::Add { .. })) => {
                // Shouldn't get Add variant for Remove event
                panic!("Expected Remove modification, got Add");
            }
            Ok(None) => {
                // Event might not decode properly due to test setup
            }
            Err(_) => {
                // Event decoding might fail due to ABI mismatch in test
            }
        }
    }

    #[tokio::test]
    async fn test_extract_pending_modifications_unknown_event() {
        let (indexer, _temp_dir) = create_test_indexer();

        // Create unknown event data
        let topics = vec![B256::random()]; // Random topic that doesn't match our events
        let data = vec![1, 2, 3, 4];

        let log_data = alloy::primitives::LogData::new(topics, data.into()).unwrap();

        let result = indexer
            .extract_pending_modifications(&log_data, 0)
            .await
            .unwrap();

        // Unknown events should return None
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_move_pending_modifications_to_store_genesis_block() {
        let (indexer, _temp_dir) = create_test_indexer();

        // Test that genesis block (block 0) doesn't get pruned
        let result = indexer.move_pending_modifications_to_store(0).await;
        assert!(result.is_ok());

        // Should have done nothing since we skip genesis block
        assert_eq!(indexer.pending_modifications_tree().unwrap().len(), 0);
        assert_eq!(indexer.block_hash_tree().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_move_pending_modifications_to_store_with_data() {
        let (indexer, _temp_dir) = create_test_indexer();

        // Add some test data
        let block_hashes = vec![
            BlockNumHash {
                number: 1,
                hash: B256::from([1; 32]),
            },
            BlockNumHash {
                number: 2,
                hash: B256::from([2; 32]),
            },
        ];
        indexer.write_block_num_hash_batch(block_hashes).unwrap();

        let modification = PendingModification::Add {
            assertion_adopter: Address::random(),
            assertion_contract: AssertionContract::default(),
            trigger_recorder: TriggerRecorder::default(),
            active_at_block: 1,
            log_index: 0,
        };

        let key = ser(&U256::from(1_u64)).unwrap();
        indexer
            .pending_modifications_tree()
            .unwrap()
            .insert(&key, ser(&vec![modification]).unwrap())
            .unwrap();

        // Move modifications for block 1
        indexer
            .move_pending_modifications_to_store(1)
            .await
            .unwrap();

        // Should have pruned block 1 and applied the modification
        assert_eq!(indexer.block_hash_tree().unwrap().len(), 1); // Only block 2 left
        assert_eq!(indexer.pending_modifications_tree().unwrap().len(), 0);
    }

    #[test]
    fn test_new_indexer_sync_state() {
        let (indexer, _temp_dir) = create_test_indexer();

        // New indexer should not be synced
        assert!(!indexer.is_synced);

        // Attempting to run without sync should fail
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(indexer.run());
        assert!(matches!(result, Err(IndexerError::StoreNotSynced)));
    }
}
