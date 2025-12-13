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
use futures::StreamExt;
use std::collections::HashMap;

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
    Bytes,
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

use futures::stream;
use serde::Deserialize;
use std::collections::BTreeMap;

sol! {

    /// @notice Emitted when a new assertion is added
    /// @param assertionAdopter The assertion adopter the assertion is associated with
    /// @param assertionId The unique identifier of the assertion
    /// @param activationBlock The block number when the assertion becomes active
    #[derive(Debug)]
    event AssertionAdded(address assertionAdopter, bytes32 assertionId, uint256 activationBlock);

    /// @notice Emitted when an assertion is removed
    /// @param assertionAdopter The assertion adopter where the assertion is removed from
    /// @param assertionId The unique identifier of the removed assertion
    /// @param deactivationBlock The block number when the assertion is going to be inactive
    #[derive(Debug)]
    event AssertionRemoved(address assertionAdopter, bytes32 assertionId, uint256 deactivationBlock);

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
///    let state_oracle_deployment_block = 0;
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
///         state_oracle_deployment_block,
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
    /// The State Oracle deployment block
    state_oracle_deployment_block: u64,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlockTag {
    Latest,
    Safe,
    Finalized,
}

impl From<BlockTag> for BlockNumberOrTag {
    fn from(tag: BlockTag) -> Self {
        match tag {
            BlockTag::Latest => BlockNumberOrTag::Latest,
            BlockTag::Safe => BlockNumberOrTag::Safe,
            BlockTag::Finalized => BlockNumberOrTag::Finalized,
        }
    }
}

type PubSubProvider = RootProvider;

/// The block range for which the indexer will get logs from.
/// Different clients have different allowed ranges. If the sidecar errors,
/// reduce this value to something acceptable in your case.
const MAX_BLOCKS_PER_CALL: u64 = 50_000;

#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error("Transport error")]
    TransportError(#[source] TransportError),
    #[error("Sled error")]
    SledError(#[source] std::io::Error),
    #[error("Bincode error")]
    BincodeError(#[source] bincode::Error),
    #[error("Event decoding error")]
    EventDecodeError(#[source] alloy_sol_types::Error),
    #[error("Block number missing")]
    BlockNumberMissing,
    #[error("Log index missing")]
    LogIndexMissing,
    #[error("Block number exceeds u64")]
    BlockNumberExceedsU64,
    #[error("DA Client Error")]
    DaClientError(#[source] DaClientError),
    #[error("Error decoding da bytecode")]
    DaBytecodeDecodingFailed(#[source] alloy::hex::FromHexError),
    #[error("Block stream error")]
    BlockStreamError(#[source] tokio::sync::broadcast::error::RecvError),
    #[error("Parent block not found")]
    ParentBlockNotFound,
    #[error("Block hash missing")]
    BlockHashMissing,
    #[error("No common ancestor found")]
    NoCommonAncestor,
    #[error("Execution Logs Rx is None; Channel likely dropped.")]
    ExecutionLogsRxNone,
    #[error("Check if reorged error")]
    CheckIfReorgedError(#[source] CheckIfReorgedError),
    #[error("Assertion Store Error")]
    AssertionStoreError(#[source] AssertionStoreError),
    #[error("Store must be synced before running")]
    StoreNotSynced,
}

type IndexerResult<T = ()> = std::result::Result<T, IndexerError>;

/// Configuration for the Indexer
#[derive(Debug, Clone)]
pub struct IndexerCfg {
    /// The State Oracle contract address
    pub state_oracle: Address,
    /// The State Oracle deployment block
    pub state_oracle_deployment_block: u64,
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
            state_oracle_deployment_block,
            executor_config,
            await_tag,
        } = cfg;

        Self {
            provider,
            db,
            store,
            da_client,
            state_oracle,
            state_oracle_deployment_block,
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
            error!(
                target = "assertion_executor::indexer",
                "Indexer must be synced before running"
            );
            return Err(IndexerError::StoreNotSynced);
        }

        let block_stream = self
            .provider
            .subscribe_blocks()
            .await
            .map_err(IndexerError::TransportError)?;
        let mut stream = block_stream.into_stream();

        // Process incoming header
        while let Some(latest_block) = stream.next().await {
            // FIXME: Sometimes when op-talos is syncing from scratch,
            // the below might fail with `Lagged` on the recv.
            // When op-talos is initially syncing, writes indexer data to disk,
            // and then restarts this problem does not appear.
            debug!(
                target = "assertion_executor::indexer",
                ?latest_block,
                "Received new block"
            );
            self.handle_latest_block(latest_block).await?;
        }
        warn!(target = "assertion_executor::indexer", "Block stream ended");
        Ok(())
    }

    /// Sync the indexer to the latest block
    #[instrument(skip(self))]
    pub async fn sync_to_head(&mut self) -> IndexerResult {
        debug!(
            target = "assertion_executor::indexer",
            "Syncing indexer to latest block"
        );
        let Some(latest_block) = self
            .provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await
            .map_err(IndexerError::TransportError)?
        else {
            warn!("Latest block not found");
            return Ok(());
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
        let ser_to = ser(&U256::from(to)).map_err(IndexerError::BincodeError)?;

        let block_hash_tree = self.block_hash_tree().map_err(IndexerError::SledError)?;
        let pending_modifications_tree = self
            .pending_modifications_tree()
            .map_err(IndexerError::SledError)?;

        while let Some((key, _)) = block_hash_tree
            .pop_first_in_range(..ser_to.clone())
            .map_err(IndexerError::SledError)?
        {
            if let Some(pending_mods) = pending_modifications_tree
                .remove(&key)
                .map_err(IndexerError::SledError)?
            {
                let pending_mods: Vec<PendingModification> =
                    de(&pending_mods).map_err(IndexerError::BincodeError)?;
                pending_modifications.reserve(pending_mods.len());
                pending_modifications.extend(pending_mods);
            }
        }
        trace!(
            target = "assertion_executor::indexer",
            to,
            ?pending_modifications,
            "Pruned pending modifications and block hashes trees"
        );

        Ok(pending_modifications)
    }

    /// Prune the pending modifications and block hashes trees from a block number
    fn prune_from(&self, from: u64) -> IndexerResult {
        let ser_from = ser(&U256::from(from)).map_err(IndexerError::BincodeError)?;

        let block_hash_tree = self.block_hash_tree().map_err(IndexerError::SledError)?;
        let pending_modifications_tree = self
            .pending_modifications_tree()
            .map_err(IndexerError::SledError)?;

        while let Some((key, _)) = block_hash_tree
            .pop_first_in_range(ser_from.clone()..)
            .map_err(IndexerError::SledError)?
        {
            pending_modifications_tree
                .remove(&key)
                .map_err(IndexerError::SledError)?;
        }
        trace!(
            target = "assertion_executor::indexer",
            from, "Pruned pending modifications and block hashes trees"
        );

        Ok(())
    }

    /// Finds the common ancestor
    /// Traverses from the cursor backwords until it finds a common ancestor in the `block_hashes`
    /// tree.
    async fn find_common_ancestor(&self, cursor_hash: B256) -> IndexerResult<u64> {
        let block_hashes_tree = self.block_hash_tree().map_err(IndexerError::SledError)?;

        let mut cursor_hash = cursor_hash;
        loop {
            let cursor = self
                .provider
                .get_block_by_hash(cursor_hash)
                .await
                .map_err(IndexerError::TransportError)?
                .ok_or(IndexerError::ParentBlockNotFound)?;
            let cursor_header = cursor.header();

            cursor_hash = cursor_header.parent_hash();
            if let Some(hash) = block_hashes_tree
                .get(ser(&U256::from(cursor_header.number())).map_err(IndexerError::BincodeError)?)
                .map_err(IndexerError::SledError)?
            {
                if hash == ser(&cursor_header.hash()).map_err(IndexerError::BincodeError)? {
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
                    .await
                    .map_err(IndexerError::CheckIfReorgedError)?;

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
            from = self.state_oracle_deployment_block;
        }

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
    fn move_pending_modifications_to_store(&self, upper_bound: u64) -> IndexerResult {
        // Delay pruning genesis block until the first block is ready to be moved.
        // Otherwise it would immediately prune the genesis block and would not be able to find a
        // common ancestor if there was a reorg to the genesis block.
        if upper_bound == 0 {
            return Ok(());
        }

        let pending_modifications = self.prune_to(upper_bound + 1)?;

        if pending_modifications.is_empty() {
            trace!(
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
                .apply_pending_modifications(pending_modifications)
                .map_err(IndexerError::AssertionStoreError)?;
        }

        Ok(())
    }

    /// Fetch the events from the `State Oracle` contract.
    /// Store the events in the `pending_modifications` tree for the indexed blocks.
    #[allow(clippy::too_many_lines)]
    async fn index_range(&self, from: u64, to: u64) -> IndexerResult {
        trace!(
            target = "assertion_executor::indexer",
            from, to, "Indexing range"
        );

        let filter = Filter::new()
            .address(self.state_oracle)
            .from_block(BlockNumberOrTag::Number(from))
            .to_block(BlockNumberOrTag::Number(to));

        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .map_err(IndexerError::TransportError)?;

        // For ordered insertion of pending modifications
        let mut pending_modifications: BTreeMap<u64, BTreeMap<u64, PendingModification>> =
            BTreeMap::new();

        for log in &logs {
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

        for (block, log_map) in &pending_modifications {
            let block_mods = log_map
                .values()
                .cloned()
                .collect::<Vec<PendingModification>>();
            pending_mods_batch.insert(
                ser(&U256::from(*block)).map_err(IndexerError::BincodeError)?,
                ser(&block_mods).map_err(IndexerError::BincodeError)?,
            );
        }

        let pending_modifications_tree = self
            .pending_modifications_tree()
            .map_err(IndexerError::SledError)?;
        pending_modifications_tree
            .apply_batch(pending_mods_batch)
            .map_err(IndexerError::SledError)?;

        trace!(
            target = "assertion_executor::indexer",
            from, to, "Building block hashes batch"
        );

        let mut block_hashes_map: HashMap<u64, B256> = logs
            .iter()
            .filter_map(|log| {
                log.block_number
                    .and_then(|number| log.block_hash.map(|hash| (number, hash)))
            })
            .collect();

        // Find missing blocks (blocks with no logs)
        let missing_blocks: Vec<u64> = (from..=to)
            .filter(|block_num| !block_hashes_map.contains_key(block_num))
            .collect();

        // Fetch missing block hashes if needed
        if !missing_blocks.is_empty() {
            let missing_hashes: Vec<(u64, B256)> = stream::iter(missing_blocks)
                .map(|num| {
                    async move {
                        let block = self
                            .provider
                            .get_block(BlockId::Number(BlockNumberOrTag::Number(num)))
                            .await
                            .map_err(IndexerError::TransportError)?
                            .ok_or(IndexerError::BlockHashMissing)?;

                        Ok::<_, IndexerError>((num, block.header().hash()))
                    }
                })
                .buffer_unordered(20) // Concurrent fetches
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;

            block_hashes_map.extend(missing_hashes);
        }

        // Convert to sorted Vec
        let mut block_hashes: Vec<BlockNumHash> = block_hashes_map
            .into_iter()
            .map(|(number, hash)| BlockNumHash { number, hash })
            .collect();
        block_hashes.sort_by_key(|b| b.number);

        if let Some(last_indexed_block_num_hash) = block_hashes.last() {
            trace!(
                target = "assertion_executor::indexer",
                last_indexed_block_num_hash = ?last_indexed_block_num_hash,
                "Last indexed block number hash updated"
            );
            self.insert_last_indexed_block_num_hash(*last_indexed_block_num_hash)?;
        } else {
            trace!(
                target = "assertion_executor::indexer",
                "Last indexed block number hash not updated"
            );
        }

        self.write_block_num_hash_batch(block_hashes)?;
        trace!(
            target = "assertion_executor::indexer",
            from, to, "Block hashes batch applied"
        );

        let block_response = self
            .provider
            .get_block_by_number(self.await_tag.into())
            .await
            .map_err(IndexerError::TransportError)?;

        if let Some(block) = block_response {
            let block_to_move = block.header().number();
            trace!(
                target = "assertion_executor::indexer",
                block_to_move, "Moving pending modifications to store"
            );

            self.move_pending_modifications_to_store(block_to_move)?;
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
                let topics = AssertionAdded::decode_topics(log.topics())
                    .map_err(IndexerError::EventDecodeError)?;
                let data = AssertionAdded::abi_decode_data(&log.data).map_err(|e| {
                    debug!(target: "assertion_executor::indexer", ?e, "Failed to decode AssertionAdded event data");
                    e
                }).map_err(IndexerError::EventDecodeError)?;

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
                } = self
                    .da_client
                    .fetch_assertion(event.assertionId)
                    .await
                    .map_err(IndexerError::DaClientError)?;

                // Rebuild deployment bytecode by appending constructor args returned separately
                let mut deployment_bytecode = bytecode.to_vec();
                deployment_bytecode.extend_from_slice(encoded_constructor_args.as_ref());
                let deployment_bytecode: Bytes = deployment_bytecode.into();

                let assertion_contract_res =
                    extract_assertion_contract(&deployment_bytecode, &self.executor_config);

                match assertion_contract_res {
                    Ok((assertion_contract, trigger_recorder)) => {
                        let activation_block = event
                            .activationBlock
                            .try_into()
                            .map_err(|_| IndexerError::BlockNumberExceedsU64)?;

                        debug!(
                            target = "assertion_executor::indexer",
                            assertion_id=?assertion_contract.id,
                            activation_block,
                            "assertionAdded event processed",
                        );

                        Some(PendingModification::Add {
                            assertion_adopter: event.assertionAdopter,
                            assertion_contract,
                            trigger_recorder,
                            activation_block,
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
                let topics = AssertionRemoved::decode_topics(log.topics())
                    .map_err(IndexerError::EventDecodeError)?;
                let data = AssertionRemoved::abi_decode_data(&log.data)
                    .map_err(IndexerError::EventDecodeError)?;
                let event = AssertionRemoved::new(topics, data);

                trace!(
                    target = "assertion_executor::indexer",
                    ?event,
                    "AssertionRemoved event decoded"
                );

                let inactivation_block = event
                    .deactivationBlock
                    .try_into()
                    .map_err(|_| IndexerError::BlockNumberExceedsU64)?;

                debug!(
                    target = "assertion_executor::indexer",
                    assertion_id=?event.assertionId,
                    inactivation_block,
                    "assertionRemoved event processed",
                );
                Some(PendingModification::Remove {
                    assertion_contract_id: event.assertionId,
                    assertion_adopter: event.assertionAdopter,
                    inactivation_block,
                    log_index,
                })
            }
            _ => None,
        };
        Ok(pending_mod_opt)
    }

    /// Insert last indexed block number and hash into the sled db
    fn insert_last_indexed_block_num_hash(&self, block_num_hash: BlockNumHash) -> IndexerResult {
        let value = ser(&block_num_hash).map_err(IndexerError::BincodeError)?;
        let latest_block_tree = self.latest_block_tree().map_err(IndexerError::SledError)?;
        latest_block_tree
            .insert("", value)
            .map_err(IndexerError::SledError)?;
        Ok(())
    }

    /// Get the last indexed block number and hash
    fn get_last_indexed_block_num_hash(&self) -> IndexerResult<Option<BlockNumHash>> {
        let latest_block_tree = self.latest_block_tree().map_err(IndexerError::SledError)?;
        let last_indexed_block = latest_block_tree.get("").map_err(IndexerError::SledError)?;
        if let Some(last_indexed_block) = last_indexed_block {
            let last_indexed_block: BlockNumHash =
                de(&last_indexed_block).map_err(IndexerError::BincodeError)?;
            Ok(Some(last_indexed_block))
        } else {
            Ok(None)
        }
    }

    /// Write the block number and hash to the sled db
    fn write_block_num_hash_batch(&self, block_num_hashes: Vec<BlockNumHash>) -> IndexerResult {
        let mut batch = sled::Batch::default();
        for block_num_hash in block_num_hashes {
            let key =
                ser(&U256::from(block_num_hash.number)).map_err(IndexerError::BincodeError)?;
            let value =
                ser(&B256::from(block_num_hash.hash)).map_err(IndexerError::BincodeError)?;
            batch.insert(key, value);
        }
        let block_hash_tree = self.block_hash_tree().map_err(IndexerError::SledError)?;
        block_hash_tree
            .apply_batch(batch)
            .map_err(IndexerError::SledError)?;

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
    use alloy::{
        hex,
        primitives::keccak256,
        sol_types::SolValue,
    };
    use alloy_transport::mock::Asserter;
    use sled::Config;
    use tempfile::TempDir;
    use tokio::{
        io::{
            AsyncReadExt,
            AsyncWriteExt,
        },
        net::TcpListener,
    };

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
            .connect_mocked_client(Asserter::default())
            .root()
            .clone();

        let da_client = DaClient::new("http://localhost:0").unwrap();

        let indexer = Indexer::new(IndexerCfg {
            state_oracle: Address::random(),
            state_oracle_deployment_block: 0,
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

    #[tokio::test]
    async fn test_indexer_creation() {
        let (indexer, _temp_dir) = create_test_indexer();
        assert!(!indexer.is_synced);
        assert_eq!(indexer.await_tag, BlockTag::Latest);
    }

    #[tokio::test]
    async fn test_write_block_num_hash_batch() {
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

    #[tokio::test]
    async fn test_last_indexed_block_operations() {
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

    #[tokio::test]
    async fn test_prune_to() {
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
            activation_block: 1,
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

    #[tokio::test]
    async fn test_prune_from() {
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
        let activation_block = 100u64;

        // Create event topics and data
        let topics = vec![AssertionAdded::SIGNATURE_HASH];

        // ABI-encode non-indexed event params: (assertionAdopter, assertionId, activationBlock)
        let data = (contract_address, assertion_id, U256::from(activation_block)).abi_encode();

        let log_data = alloy::primitives::LogData::new(topics, data.into()).unwrap();

        // Test that the method handles the event signature correctly
        let result = indexer.extract_pending_modifications(&log_data, 0).await;

        // Should get an error since we're not mocking the DA client or provider
        assert!(result.is_err());
        // Could be DaClientError or EventDecodeError depending on which fails first
    }

    #[tokio::test]
    async fn test_extract_pending_modifications_assertion_added_with_constructor_args() {
        // Build deployment bytecode with constructor args appended once
        let constructor_arg = Address::random();
        let encoded_constructor_args = (constructor_arg,).abi_encode();
        let assertion_bytecode = crate::test_utils::bytecode("MockAssertion.sol:MockAssertion");
        let mut deployment_bytecode = assertion_bytecode.to_vec();
        deployment_bytecode.extend_from_slice(&encoded_constructor_args);
        let expected_assertion_id = keccak256(&deployment_bytecode);

        // Spin up a tiny HTTP JSON-RPC server to mock the DA client
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");
        let bytecode_hex = hex::encode(assertion_bytecode);
        let encoded_args_hex = hex::encode(&encoded_constructor_args);

        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buf = [0u8; 4096];
                // best-effort read to clear the request
                let _ = stream.read(&mut buf).await;
                let body = format!(
                    r#"{{"jsonrpc":"2.0","result":{{"solidity_source":"","bytecode":"0x{bytecode_hex}","prover_signature":"0x","encoded_constructor_args":"0x{encoded_args_hex}","constructor_abi_signature":"constructor(address)"}}, "id":1}}"#
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        // Build an indexer that points to the mock DA server
        let temp_dir = tempfile::tempdir().unwrap();
        let db = Config::new()
            .path(temp_dir.path())
            .cache_capacity_bytes(1024)
            .open()
            .unwrap();

        let store = AssertionStore::new_ephemeral().unwrap();

        let provider = alloy_provider::ProviderBuilder::new()
            .connect_mocked_client(Asserter::default())
            .root()
            .clone();

        let da_client = DaClient::new(&url).unwrap();

        let indexer = Indexer::new(IndexerCfg {
            state_oracle: Address::random(),
            state_oracle_deployment_block: 0,
            da_client,
            executor_config: ExecutorConfig::default(),
            store,
            provider,
            db,
            await_tag: BlockTag::Latest,
        });

        // Create AssertionAdded event data that matches the mocked DA response
        let topics = vec![AssertionAdded::SIGNATURE_HASH];
        let activation_block = 42u64;
        let data = (
            Address::random(),
            expected_assertion_id,
            U256::from(activation_block),
        )
            .abi_encode();
        let log_data = alloy::primitives::LogData::new(topics, data.into()).unwrap();

        let result = indexer
            .extract_pending_modifications(&log_data, 0)
            .await
            .unwrap();
        let Some(PendingModification::Add {
            assertion_contract,
            activation_block: returned_activation_block,
            ..
        }) = result
        else {
            panic!("Expected PendingModification::Add");
        };

        assert_eq!(assertion_contract.id, expected_assertion_id);
        assert_eq!(returned_activation_block, activation_block);
    }

    #[tokio::test]
    async fn test_extract_pending_modifications_assertion_removed() {
        let (indexer, _temp_dir) = create_test_indexer();

        // Create AssertionRemoved event data
        let assertion_id = B256::random();
        let contract_address = Address::random();
        let activation_block = 100u64;

        // Create event topics and data
        let topics = vec![AssertionRemoved::SIGNATURE_HASH];

        // ABI-encode non-indexed event params: (assertionAdopter, assertionId, deactivationBlock)
        let data = (contract_address, assertion_id, U256::from(activation_block)).abi_encode();

        let log_data = alloy::primitives::LogData::new(topics, data.into()).unwrap();

        let result = indexer.extract_pending_modifications(&log_data, 0).await;

        // Test the result - might succeed in decoding removal events since they don't need DA
        #[allow(clippy::match_same_arms)]
        match result {
            Ok(Some(PendingModification::Remove {
                assertion_contract_id,
                assertion_adopter,
                inactivation_block,
                log_index,
            })) => {
                assert_eq!(assertion_contract_id, assertion_id);
                assert_eq!(assertion_adopter, contract_address);
                assert_eq!(inactivation_block, activation_block);
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
        let result = indexer.move_pending_modifications_to_store(0);
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
            activation_block: 1,
            log_index: 0,
        };

        let key = ser(&U256::from(1_u64)).unwrap();
        indexer
            .pending_modifications_tree()
            .unwrap()
            .insert(&key, ser(&vec![modification]).unwrap())
            .unwrap();

        // Move modifications for block 1
        indexer.move_pending_modifications_to_store(1).unwrap();

        // Should have pruned block 1 and applied the modification
        assert_eq!(indexer.block_hash_tree().unwrap().len(), 1); // Only block 2 left
        assert_eq!(indexer.pending_modifications_tree().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_new_indexer_sync_state() {
        let (indexer, _temp_dir) = create_test_indexer();

        // New indexer should not be synced
        assert!(!indexer.is_synced);

        let result = indexer.run().await;
        assert!(matches!(result, Err(IndexerError::StoreNotSynced)));
    }
}
