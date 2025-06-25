use alloy_network::BlockResponse;
use alloy_provider::{
    Provider,
    RootProvider,
};
use alloy_rpc_types::{
    BlockId,
    BlockNumHash,
    BlockNumberOrTag,
    BlockTransactionsKind,
    Filter,
};
use alloy_transport::TransportError;

use alloy_network_primitives::HeaderResponse;

use alloy_consensus::BlockHeader;

use alloy_sol_types::{
    sol,
    SolEvent,
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
    primitives::{
        Address,
        UpdateBlock,
        B256,
    },
    store::{
        extract_assertion_contract,
        AssertionStore,
        AssertionStoreError,
        PendingModification,
    },
    utils::reorg_utils::{
        check_if_reorged,
        CheckIfReorgedError,
    },
    ExecutorConfig,
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
///         block_hash_tree: db.open_tree("block_hashes").unwrap(),
///         pending_modifications_tree: db.open_tree("pending_modifications").unwrap(),
///         latest_block_tree: db.open_tree("latest_block").unwrap(),
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
    /// The sled db tree for storing block hashes
    block_hash_tree: sled::Tree,
    /// The sled db tree for storing the latest block. Required for tracking progress if not
    /// awaiting(AwaitTag::Latest)
    latest_block_tree: sled::Tree,
    /// The sled db tree for storing pending modifications
    pending_modifications_tree: sled::Tree,
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
    /// Block hash tree
    /// Used to store block hashes for reorg detection
    pub block_hash_tree: sled::Tree,
    /// The sled db tree for storing the latest block. Required for tracking progress if not
    /// awaiting(AwaitTag::Latest)
    pub latest_block_tree: sled::Tree,
    /// Pending modifications tree
    /// Used to store pending modifications for blocks before moving
    /// them to the store
    pub pending_modifications_tree: sled::Tree,
    /// Tag to use as the upper bound of pending modifications which should be applied to the
    /// store.
    pub await_tag: BlockTag,
}

impl Indexer {
    /// Create a new Indexer
    pub fn new(cfg: IndexerCfg) -> Self {
        let IndexerCfg {
            provider,
            block_hash_tree,
            pending_modifications_tree,
            latest_block_tree,
            store,
            da_client,
            state_oracle,
            executor_config,
            await_tag,
        } = cfg;

        Self {
            provider,
            block_hash_tree,
            latest_block_tree,
            pending_modifications_tree,
            store,
            da_client,
            state_oracle,
            executor_config,
            is_synced: false,
            await_tag,
        }
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
            .get_block_by_number(BlockNumberOrTag::Latest, BlockTransactionsKind::Hashes)
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

        while let Some((key, _)) = self.block_hash_tree.pop_first_in_range(..ser_to.clone())? {
            if let Some(pending_mods) = self.pending_modifications_tree.remove(&key)? {
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

        while let Some((key, _)) = self
            .block_hash_tree
            .pop_first_in_range(ser_from.clone()..)?
        {
            self.pending_modifications_tree.remove(&key)?;
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
        let block_hashes_tree = &self.block_hash_tree;

        let mut cursor_hash = cursor_hash;
        loop {
            let cursor = self
                .provider
                .get_block_by_hash(cursor_hash, BlockTransactionsKind::Hashes)
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

        self.pending_modifications_tree
            .apply_batch(pending_mods_batch)?;

        trace!(
            target = "assertion_executor::indexer",
            from,
            to,
            "Building block hashes batch"
        );

        // Build the block hashes batch
        let mut block_hashes = vec![];

        for i in from..=to {
            let block_hash = self
                .provider
                .get_block(
                    BlockId::Number(BlockNumberOrTag::Number(i)),
                    BlockTransactionsKind::Hashes,
                )
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
            from,
            to,
            "Block hashes batch applied"
        );

        let block_to_move = self
            .provider
            .get_block_by_number(self.await_tag.into(), BlockTransactionsKind::Hashes)
            .await?
            .ok_or(IndexerError::ParentBlockNotFound)?
            .header()
            .number();

        trace!(
            target = "assertion_executor::indexer",
            block_to_move,
            "Moving pending modifications to store"
        );

        self.move_pending_modifications_to_store(block_to_move)
            .await?;
        trace!(
            target = "assertion_executor::indexer",
            block_to_move,
            "Pending modifications moved to store"
        );

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
                let data = AssertionAdded::abi_decode_data(&log.data, true).map_err(|e| {
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
                let data = AssertionRemoved::abi_decode_data(&log.data, true)?;
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
        self.latest_block_tree.insert("", value)?;
        Ok(())
    }

    /// Get the last indexed block number and hash
    fn get_last_indexed_block_num_hash(&self) -> IndexerResult<Option<BlockNumHash>> {
        let last_indexed_block = self.latest_block_tree.get("")?;
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
        self.block_hash_tree.apply_batch(batch)?;

        Ok(())
    }
}

#[cfg(test)]
mod test_indexer {
    use super::*;
    use crate::{
        inspectors::{
            CallTracer,
            TriggerRecorder,
        },
        primitives::{
            Address,
            AssertionContract,
            U256,
        },
        test_utils::{
            anvil_provider,
            deployed_bytecode,
            mine_block,
        },
    };
    use alloy_primitives::FixedBytes;
    use std::net::{
        IpAddr,
        Ipv4Addr,
        SocketAddr,
    };
    use std::str::FromStr;

    use sled::Config;

    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use alloy_network::{
        EthereumWallet,
        TransactionBuilder,
    };
    use alloy_node_bindings::AnvilInstance;
    use alloy_provider::ext::AnvilApi;
    use alloy_rpc_types::TransactionRequest;
    use alloy_rpc_types_anvil::MineOptions;
    use alloy_signer_local::PrivateKeySigner;
    use alloy_sol_types::{
        sol,
        SolCall,
    };

    sol! {
        function addAssertion(address contractAddress, bytes32 assertionId) public {}
        function removeAssertion(address contractAddress, bytes32 assertionId) public {}
    }

    // TODO(@0xgregthedev): Create clearer delineation between integration tests and unit tests.
    // Then move integration tests to an integration test directory. A feature flag for docker
    // dependent integration tests may be helpful as well.

    async fn setup_with_tag(await_tag: BlockTag) -> (Indexer, JoinHandle<()>, AnvilInstance) {
        let (provider, anvil) = anvil_provider().await;

        let state_oracle = Address::new([0; 20]);
        let db = Config::tmp().unwrap().open().unwrap();
        let (handle, port) = mock_da_server().await;
        let store = AssertionStore::new_ephemeral().unwrap();
        let da_client = DaClient::new(&format!("http://127.0.0.1:{port}")).unwrap();

        (
            Indexer::new(IndexerCfg {
                provider,
                block_hash_tree: db.open_tree("block_hashes").unwrap(),
                pending_modifications_tree: db.open_tree("pending_modifications").unwrap(),
                latest_block_tree: db.open_tree("latest_block").unwrap(),
                store,
                da_client,
                state_oracle,
                executor_config: ExecutorConfig::default(),
                await_tag,
            }),
            handle,
            anvil,
        )
    }

    async fn setup() -> (Indexer, JoinHandle<()>, AnvilInstance) {
        setup_with_tag(BlockTag::Finalized).await
    }

    fn assertion_src() -> String {
        let assertion = r#"
            // SPDX-License-Identifier: UNLICENSED
            pragma solidity ^0.8.13 ^0.8.28;

            // lib/credible-std/src/PhEvm.sol

            interface PhEvm {
                // An Ethereum log
                struct Log {
                    // The topics of the log, including the signature, if any.
                    bytes32[] topics;
                    // The raw data of the log.
                    bytes data;
                    // The address of the log's emitter.
                    address emitter;
                }

                // Call inputs for the getCallInputs precompile
                struct CallInputs {
                    // The call data of the call.
                    bytes input;
                    /// The gas limit of the call.
                    uint64 gas_limit;
                    // The account address of bytecode that is going to be executed.
                    //
                    // Previously `context.code_address`.
                    address bytecode_address;
                    // Target address, this account storage is going to be modified.
                    //
                    // Previously `context.address`.
                    address target_address;
                    // This caller is invoking the call.
                    //
                    // Previously `context.caller`.
                    address caller;
                    // Call value.
                    //
                    // NOTE: This value may not necessarily be transferred from caller to callee, see [`CallValue`].
                    //
                    // Previously `transfer.value` or `context.apparent_value`.
                    uint256 value;
                }

                //Forks to the state prior to the assertion triggering transaction.
                function forkPreState() external;

                // Forks to the state after the assertion triggering transaction.
                function forkPostState() external;

                // Loads a storage slot from an address
                function load(address target, bytes32 slot) external view returns (bytes32 data);

                // Get the logs from the assertion triggering transaction.
                function getLogs() external returns (Log[] memory logs);

                // Get the call inputs for a given target and selector
                function getCallInputs(address target, bytes4 selector) external view returns (CallInputs[] memory calls);

                // Get state changes for a given contract and storage slot.
                function getStateChanges(address contractAddress, bytes32 slot)
                    external
                    view
                    returns (bytes32[] memory stateChanges);
            }

            // lib/credible-std/src/TriggerRecorder.sol

            interface TriggerRecorder {
                /// @notice Registers storage change trigger for all slots
                /// @param fnSelector The function selector of the assertion function.
                function registerStorageChangeTrigger(bytes4 fnSelector) external view;

                /// @notice Registers storage change trigger for a slot
                /// @param fnSelector The function selector of the assertion function.
                /// @param slot The storage slot to trigger on.
                function registerStorageChangeTrigger(bytes4 fnSelector, bytes32 slot) external view;

                /// @notice Registers balance change trigger for the AA
                /// @param fnSelector The function selector of the assertion function.
                function registerBalanceChangeTrigger(bytes4 fnSelector) external view;

                /// @notice Registers a call trigger for calls to the AA.
                /// @param fnSelector The function selector of the assertion function.
                /// @param triggerSelector The function selector of the trigger function.
                function registerCallTrigger(bytes4 fnSelector, bytes4 triggerSelector) external view;

                /// @notice Records a call trigger for the specified assertion function.
                /// A call trigger signifies that the assertion function should be called
                /// if the assertion adopter is called.
                /// @param fnSelector The function selector of the assertion function.
                function registerCallTrigger(bytes4 fnSelector) external view;
            }

            // lib/credible-std/src/Credible.sol

            /// @notice The Credible contract
            abstract contract Credible {
                //Precompile address -
                PhEvm constant ph = PhEvm(address(uint160(uint256(keccak256("Kim Jong Un Sucks")))));
            }

            // lib/credible-std/src/StateChanges.sol

            /**
             * @title StateChanges
             * @notice Helper contract for converting state changes from bytes32 arrays to typed arrays
             * @dev Inherits from Credible to access the PhEvm interface
             */
            contract StateChanges is Credible {
                /**
                 * @notice Converts state changes for a slot to uint256 array
                 * @param contractAddress The address of the contract to get state changes from
                 * @param slot The storage slot to get state changes for
                 * @return Array of state changes as uint256 values
                 */
                function getStateChangesUint(address contractAddress, bytes32 slot) internal view returns (uint256[] memory) {
                    bytes32[] memory stateChanges = ph.getStateChanges(contractAddress, slot);

                    // Explicit cast to uint256[]
                    uint256[] memory uintChanges;
                    assembly {
                        uintChanges := stateChanges
                    }

                    return uintChanges;
                }

                /**
                 * @notice Converts state changes for a slot to address array
                 * @param contractAddress The address of the contract to get state changes from
                 * @param slot The storage slot to get state changes for
                 * @return Array of state changes as address values
                 */
                function getStateChangesAddress(address contractAddress, bytes32 slot) internal view returns (address[] memory) {
                    bytes32[] memory stateChanges = ph.getStateChanges(contractAddress, slot);

                    assembly {
                        // Zero out the upper 96 bits for each element to ensure clean address casting
                        for { let i := 0 } lt(i, mload(stateChanges)) { i := add(i, 1) } {
                            let addr :=
                                and(
                                    mload(add(add(stateChanges, 0x20), mul(i, 0x20))),
                                    0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff
                                )
                            mstore(add(add(stateChanges, 0x20), mul(i, 0x20)), addr)
                        }
                    }

                    // Explicit cast to address[]
                    address[] memory addressChanges;
                    assembly {
                        addressChanges := stateChanges
                    }

                    return addressChanges;
                }

                /**
                 * @notice Converts state changes for a slot to boolean array
                 * @param contractAddress The address of the contract to get state changes from
                 * @param slot The storage slot to get state changes for
                 * @return Array of state changes as boolean values
                 */
                function getStateChangesBool(address contractAddress, bytes32 slot) internal view returns (bool[] memory) {
                    bytes32[] memory stateChanges = ph.getStateChanges(contractAddress, slot);

                    assembly {
                        // Convert each bytes32 to bool
                        for { let i := 0 } lt(i, mload(stateChanges)) { i := add(i, 1) } {
                            // Any non-zero value is true, zero is false
                            let boolValue := iszero(iszero(mload(add(add(stateChanges, 0x20), mul(i, 0x20)))))
                            mstore(add(add(stateChanges, 0x20), mul(i, 0x20)), boolValue)
                        }
                    }

                    // Explicit cast to bool[]
                    bool[] memory boolChanges;
                    assembly {
                        boolChanges := stateChanges
                    }

                    return boolChanges;
                }

                /**
                 * @notice Gets raw state changes as bytes32 array
                 * @param contractAddress The address of the contract to get state changes from
                 * @param slot The storage slot to get state changes for
                 * @return Array of state changes as bytes32 values
                 */
                function getStateChangesBytes32(address contractAddress, bytes32 slot) internal view returns (bytes32[] memory) {
                    return ph.getStateChanges(contractAddress, slot);
                }

                /**
                 * @notice Calculates the storage slot for a mapping with a given key and offset
                 * @param slot The base storage slot of the mapping
                 * @param key The key in the mapping
                 * @param offset Additional offset to add to the calculated slot
                 * @return The storage slot for the mapping entry
                 */
                function getSlotMapping(bytes32 slot, uint256 key, uint256 offset) private pure returns (bytes32) {
                    return bytes32(uint256(keccak256(abi.encodePacked(key, slot))) + offset);
                }

                // Helper functions for mapping access with keys

                /**
                 * @notice Gets uint256 state changes for a mapping entry
                 * @param contractAddress The contract address
                 * @param slot The mapping's slot
                 * @param key The mapping key
                 * @return Array of state changes as uint256 values
                 */
                function getStateChangesUint(address contractAddress, bytes32 slot, uint256 key)
                    internal
                    view
                    returns (uint256[] memory)
                {
                    return getStateChangesUint(contractAddress, slot, key, 0);
                }

                /**
                 * @notice Gets address state changes for a mapping entry
                 * @param contractAddress The contract address
                 * @param slot The mapping's slot
                 * @param key The mapping key
                 * @return Array of state changes as address values
                 */
                function getStateChangesAddress(address contractAddress, bytes32 slot, uint256 key)
                    internal
                    view
                    returns (address[] memory)
                {
                    return getStateChangesAddress(contractAddress, slot, key, 0);
                }

                /**
                 * @notice Gets boolean state changes for a mapping entry
                 * @param contractAddress The contract address
                 * @param slot The mapping's slot
                 * @param key The mapping key
                 * @return Array of state changes as boolean values
                 */
                function getStateChangesBool(address contractAddress, bytes32 slot, uint256 key)
                    internal
                    view
                    returns (bool[] memory)
                {
                    return getStateChangesBool(contractAddress, slot, key, 0);
                }

                /**
                 * @notice Gets bytes32 state changes for a mapping entry
                 * @param contractAddress The contract address
                 * @param slot The mapping's slot
                 * @param key The mapping key
                 * @return Array of state changes as bytes32 values
                 */
                function getStateChangesBytes32(address contractAddress, bytes32 slot, uint256 key)
                    internal
                    view
                    returns (bytes32[] memory)
                {
                    return getStateChangesBytes32(contractAddress, slot, key, 0);
                }

                // Helper functions for mapping access with keys and offsets

                /**
                 * @notice Gets uint256 state changes for a mapping entry with offset
                 * @param contractAddress The contract address
                 * @param slot The mapping's slot
                 * @param key The mapping key
                 * @param slotOffset Additional offset to add to the slot
                 * @return Array of state changes as uint256 values
                 */
                function getStateChangesUint(address contractAddress, bytes32 slot, uint256 key, uint256 slotOffset)
                    internal
                    view
                    returns (uint256[] memory)
                {
                    return getStateChangesUint(contractAddress, getSlotMapping(slot, key, slotOffset));
                }

                /**
                 * @notice Gets address state changes for a mapping entry with offset
                 * @param contractAddress The contract address
                 * @param slot The mapping's slot
                 * @param key The mapping key
                 * @param slotOffset Additional offset to add to the slot
                 * @return Array of state changes as address values
                 */
                function getStateChangesAddress(address contractAddress, bytes32 slot, uint256 key, uint256 slotOffset)
                    internal
                    view
                    returns (address[] memory)
                {
                    return getStateChangesAddress(contractAddress, getSlotMapping(slot, key, slotOffset));
                }

                /**
                 * @notice Gets boolean state changes for a mapping entry with offset
                 * @param contractAddress The contract address
                 * @param slot The mapping's slot
                 * @param key The mapping key
                 * @param slotOffset Additional offset to add to the slot
                 * @return Array of state changes as boolean values
                 */
                function getStateChangesBool(address contractAddress, bytes32 slot, uint256 key, uint256 slotOffset)
                    internal
                    view
                    returns (bool[] memory)
                {
                    return getStateChangesBool(contractAddress, getSlotMapping(slot, key, slotOffset));
                }

                /**
                 * @notice Gets bytes32 state changes for a mapping entry with offset
                 * @param contractAddress The contract address
                 * @param slot The mapping's slot
                 * @param key The mapping key
                 * @param slotOffset Additional offset to add to the slot
                 * @return Array of state changes as bytes32 values
                 */
                function getStateChangesBytes32(address contractAddress, bytes32 slot, uint256 key, uint256 slotOffset)
                    internal
                    view
                    returns (bytes32[] memory)
                {
                    return getStateChangesBytes32(contractAddress, getSlotMapping(slot, key, slotOffset));
                }
            }

            // lib/credible-std/src/Assertion.sol

            /// @notice Assertion interface for the PhEvm precompile
            abstract contract Assertion is Credible, StateChanges {
                //Trigger recorder address
                TriggerRecorder constant triggerRecorder = TriggerRecorder(address(uint160(uint256(keccak256("TriggerRecorder")))));

                /// @notice Used to record fn selectors and their triggers.
                function triggers() external view virtual;

                /// @notice Registers a call trigger for the AA without specifying an AA function selector.
                /// This will trigger the assertion function on any call to the AA.
                /// @param fnSelector The function selector of the assertion function.
                function registerCallTrigger(bytes4 fnSelector) internal view {
                    triggerRecorder.registerCallTrigger(fnSelector);
                }

                /// @notice Registers a call trigger for calls to the AA with a specific AA function selector.
                /// @param fnSelector The function selector of the assertion function.
                /// @param triggerSelector The function selector upon which the assertion will be triggered.
                function registerCallTrigger(bytes4 fnSelector, bytes4 triggerSelector) internal view {
                    triggerRecorder.registerCallTrigger(fnSelector, triggerSelector);
                }

                /// @notice Registers storage change trigger for any slot
                /// @param fnSelector The function selector of the assertion function.
                function registerStorageChangeTrigger(bytes4 fnSelector) internal view {
                    triggerRecorder.registerStorageChangeTrigger(fnSelector);
                }

                /// @notice Registers storage change trigger for a specific slot
                /// @param fnSelector The function selector of the assertion function.
                /// @param slot The storage slot to trigger on.
                function registerStorageChangeTrigger(bytes4 fnSelector, bytes32 slot) internal view {
                    triggerRecorder.registerStorageChangeTrigger(fnSelector, slot);
                }

                /// @notice Registers balance change trigger for the AA
                /// @param fnSelector The function selector of the assertion function.
                function registerBalanceChangeTrigger(bytes4 fnSelector) internal view {
                    triggerRecorder.registerBalanceChangeTrigger(fnSelector);
                }
            }

            // src/SimpleCounterAssertion.sol

            contract Counter {
                uint256 public number;

                function increment() public {
                    number++;
                }
            }

            contract SimpleCounterAssertion is Assertion {
                event RunningAssertion(uint256 count);

                function assertCount() public {
                    uint256 count = Counter(0x0101010101010101010101010101010101010101).number();
                    emit RunningAssertion(count);
                    if (count > 1) {
                        revert("Counter cannot be greater than 1");
                    }
                }

                function triggers() external view override {
                    registerCallTrigger(this.assertCount.selector);
                }
            }

            contract SimpleCounterAssertionWithArgs is Assertion {
                event RunningAssertion(uint256 count);

                uint256 public limit; 
                
                constructor(uint256 _limit) {
                    limit = _limit;
                }

                function assertCount() public {
                    uint256 count = Counter(0x0101010101010101010101010101010101010101).number();
                    emit RunningAssertion(count);
                    if (count > limit) {
                        revert("Counter cannot be greater than limit");
                    }
                }

                function triggers() external view override {
                    registerCallTrigger(this.assertCount.selector);
                }
            }
        "#;
        assertion.to_string()
    }

    async fn send_modifications(indexer: &Indexer) -> (Address, FixedBytes<32>) {
        send_modifications_inner(indexer, false).await
    }

    async fn send_modifications_with_args(indexer: &Indexer) -> (Address, FixedBytes<32>) {
        send_modifications_inner(indexer, true).await
    }

    async fn send_modifications_inner(
        indexer: &Indexer,
        with_args: bool,
    ) -> (Address, FixedBytes<32>) {
        let registration_mock = deployed_bytecode("AssertionRegistration.sol:RegistrationMock");
        let chain_id = indexer.provider.get_chain_id().await.unwrap();

        indexer
            .provider
            .anvil_set_code(indexer.state_oracle, registration_mock)
            .await
            .unwrap();
        let signer = PrivateKeySigner::random();
        let wallet = EthereumWallet::from(signer.clone());

        let protocol_addr = Address::random();

        let resp = match with_args {
            true => {
                indexer
                    .da_client
                    .submit_assertion_with_args(
                        "SimpleCounterAssertionWithArgs".to_string(),
                        assertion_src(),
                        "0.8.28".to_string(),
                        "constructor(uint256)".to_string(),
                        vec![U256::from(5).to_string()],
                    )
                    .await
            }
            false => {
                indexer
                    .da_client
                    .submit_assertion(
                        "SimpleCounterAssertion".to_string(),
                        assertion_src(),
                        "0.8.28".to_string(),
                    )
                    .await
            }
        };

        let resp = match resp {
            Ok(rax) => {
                println!("response: {rax:?}");
                rax
            }
            Err(e) => {
                panic!("Failed to submit assertion: {e:#?}");
            }
        };

        indexer
            .provider
            .anvil_set_balance(signer.address(), U256::MAX)
            .await
            .unwrap();

        for (nonce, input) in [
            (
                0,
                addAssertionCall::new((protocol_addr, resp.id)).abi_encode(),
            ),
            (
                1,
                removeAssertionCall::new((protocol_addr, resp.id)).abi_encode(),
            ),
        ] {
            let tx = TransactionRequest::default()
                .with_to(indexer.state_oracle)
                .with_input(input)
                .with_nonce(nonce)
                .with_chain_id(chain_id)
                .with_gas_limit(25_000)
                .with_max_priority_fee_per_gas(1_000_000_000)
                .with_max_fee_per_gas(20_000_000_000);

            let tx_envelope = tx.build(&wallet).await.unwrap();

            let _ = indexer
                .provider
                .send_tx_envelope(tx_envelope)
                .await
                .unwrap();
        }

        (protocol_addr, resp.id)
    }

    async fn mock_da_server() -> (JoinHandle<()>, u16) {
        let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
        let config = assertion_da_server::Config {
            db_path: Some(
                tempfile::tempdir()
                    .unwrap()
                    .path()
                    .to_str()
                    .unwrap()
                    .to_string()
                    .into(),
            ),
            listen_addr: addr,
            cache_size: 1024,
            private_key: "00973f42a0620b6fee12391c525daeb64097412e117b8f09a2742e06ca14e0ae"
                .to_string(),
            metrics_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            log_level: tracing::metadata::LevelFilter::current(),
        };

        let server = config.build().await.unwrap();
        let port = server.listener.local_addr().unwrap().port();
        /*
        println!("Binding to: {}", config.listen_addr);
        let listener: &'static mut TcpListener = Box::leak(Box::new(
            TcpListener::bind(&config.listen_addr).await.unwrap(),
        ));
        let local_addr = listener.local_addr().unwrap();
        println!("Bound to: {local_addr}");

        // Try to open the sled db
        let db: sled::Db<1024> = sled::Config::new()
            .path(config.db_path.unwrap())
            .cache_capacity_bytes(config.cache_size)
            .open()
            .unwrap();

        let (db_tx, db_rx) =
            tokio::sync::mpsc::unbounded_channel::<assertion_da_server::api::types::DbRequest>();

        // Start the database management task
        tokio::task::spawn(assertion_da_server::api::db::listen_for_db(db_rx, db));

        // We now want to spawn all internal processes inside of a loop in this macro,
        // tokio selecting them so we can essentially restart the whole assertion
        // loader on demand in case anything fails.
        let docker = Arc::new(Docker::connect_with_local_defaults().unwrap());
        */

        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();

        #[allow(clippy::async_yields_async)]
        let handle = tokio::spawn(async move {
            println!("Serving...");
            server.run(cancel_token_clone).await.unwrap();
            println!("Done serving");
        });

        (handle, port)
    }

    async fn mine_block_write_hash(indexer: &Indexer) -> alloy_rpc_types::Header {
        let header = mine_block(&indexer.provider).await;

        indexer
            .write_block_num_hash_batch(vec![BlockNumHash {
                number: header.number,
                hash: header.hash,
            }])
            .unwrap();

        header
    }

    #[tokio::test]
    async fn test_get_latest_block_num_hash() {
        let batch_a = vec![
            BlockNumHash {
                number: 0,
                hash: B256::from([0; 32]),
            },
            BlockNumHash {
                number: 1,
                hash: B256::from([1; 32]),
            },
        ];

        let batch_b = vec![
            BlockNumHash {
                number: 0,
                hash: B256::from([0; 32]),
            },
            BlockNumHash {
                number: u64::MAX,
                hash: B256::from([1; 32]),
            },
        ];

        let batch_c = vec![
            BlockNumHash {
                number: u64::MAX / 2,
                hash: B256::from([0; 32]),
            },
            BlockNumHash {
                number: u64::MAX,
                hash: B256::from([1; 32]),
            },
        ];

        let batch_d = vec![
            BlockNumHash {
                number: 0,
                hash: B256::from([0; 32]),
            },
            BlockNumHash {
                number: u64::MAX / 2,
                hash: B256::from([1; 32]),
            },
        ];

        for batch in [batch_a, batch_b, batch_c, batch_d] {
            let (indexer, _da, _anvil) = setup().await;
            indexer.write_block_num_hash_batch(batch.clone()).unwrap();
            assert_eq!(
                indexer.block_hash_tree.last().unwrap().map(|(k, v)| {
                    let number: U256 = de(&k).unwrap();
                    let hash: B256 = de(&v).unwrap();
                    BlockNumHash {
                        number: number.try_into().unwrap(),
                        hash,
                    }
                }),
                Some(*batch.last().unwrap())
            );
        }
    }

    #[tokio::test]
    async fn test_index_range_await_latest() {
        let (indexer, da, _anvil) = setup_with_tag(BlockTag::Latest).await;
        tokio::task::spawn(da);

        let (assertion_adopter, _id) = send_modifications(&indexer).await;

        let block = indexer
            .provider
            .get_block_by_number(0.into(), Default::default())
            .await
            .unwrap()
            .unwrap();

        let block_number = block.header.number;

        assert_eq!(block_number, 0);

        let timestamp = block.header.inner.timestamp;

        let _ = indexer
            .provider
            .evm_mine(Some(MineOptions::Timestamp(Some(timestamp + 5))))
            .await;

        indexer.index_range(0, 1).await.unwrap();

        assert_eq!(
            indexer
                .get_last_indexed_block_num_hash()
                .unwrap()
                .unwrap()
                .number,
            1
        );

        assert_eq!(indexer.store.assertion_contract_count(assertion_adopter), 1);

        assert_eq!(
            indexer
                .get_last_indexed_block_num_hash()
                .unwrap()
                .unwrap()
                .number,
            1
        );
    }

    #[tokio::test]
    async fn test_index_range_with_args() {
        let (indexer, da, _anvil) = setup().await;
        tokio::task::spawn(da);

        let (assertion_adopter, id) = send_modifications_with_args(&indexer).await;

        let block = indexer
            .provider
            .get_block_by_number(0.into(), Default::default())
            .await
            .unwrap()
            .unwrap();
        let block_number = block.header.number;

        assert_eq!(block_number, 0);

        let timestamp = block.header.inner.timestamp;

        let _ = indexer
            .provider
            .evm_mine(Some(MineOptions::Timestamp(Some(timestamp + 5))))
            .await;

        indexer.index_range(0, 1).await.unwrap();

        assert_eq!(
            indexer
                .get_last_indexed_block_num_hash()
                .unwrap()
                .unwrap()
                .number,
            1
        );

        let pending_mods_tree = indexer.pending_modifications_tree;

        assert_eq!(pending_mods_tree.len(), 1);

        let key = bincode::serialize(&U256::from(1_u64)).unwrap();

        let value = pending_mods_tree.get(&key).unwrap().unwrap();

        let pending_modifications: Vec<PendingModification> = bincode::deserialize(&value).unwrap();

        let assertion = indexer.da_client.fetch_assertion(id).await.unwrap();

        let mut bytecode = (*assertion.bytecode).to_vec();
        let encoded_constructor_args = assertion.encoded_constructor_args;
        bytecode.extend_from_slice(&encoded_constructor_args);

        let assertion_contract =
            extract_assertion_contract(bytecode.into(), &ExecutorConfig::default()).unwrap();

        assert_eq!(
            assertion_contract
                .0
                .storage
                .get(&U256::from(0))
                .unwrap()
                .present_value,
            U256::from(5)
        );

        assert_eq!(
            pending_modifications,
            vec![
                PendingModification::Add {
                    assertion_adopter,
                    assertion_contract: assertion_contract.0.clone(),
                    trigger_recorder: assertion_contract.1,
                    active_at_block: 65,
                    log_index: 0,
                },
                PendingModification::Remove {
                    inactive_at_block: 65,
                    log_index: 1,
                    assertion_contract_id: id,
                    assertion_adopter,
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_index_range() {
        let (indexer, da, _anvil) = setup().await;
        tokio::task::spawn(da);

        let (assertion_adopter, id) = send_modifications(&indexer).await;

        let block = indexer
            .provider
            .get_block_by_number(0.into(), Default::default())
            .await
            .unwrap()
            .unwrap();
        let block_number = block.header.number;

        assert_eq!(block_number, 0);

        let timestamp = block.header.inner.timestamp;

        let _ = indexer
            .provider
            .evm_mine(Some(MineOptions::Timestamp(Some(timestamp + 5))))
            .await;

        indexer.index_range(0, 1).await.unwrap();

        assert_eq!(
            indexer
                .get_last_indexed_block_num_hash()
                .unwrap()
                .unwrap()
                .number,
            1
        );

        let pending_mods_tree = indexer.pending_modifications_tree;

        assert_eq!(pending_mods_tree.len(), 1);

        let key = bincode::serialize(&U256::from(1_u64)).unwrap();

        let value = pending_mods_tree.get(&key).unwrap().unwrap();

        let pending_modifications: Vec<PendingModification> = bincode::deserialize(&value).unwrap();

        let bytecode = indexer
            .da_client
            .fetch_assertion(id)
            .await
            .unwrap()
            .bytecode;
        let assertion_contract =
            extract_assertion_contract(bytecode, &ExecutorConfig::default()).unwrap();

        assert_eq!(
            pending_modifications,
            vec![
                PendingModification::Add {
                    assertion_adopter,
                    assertion_contract: assertion_contract.0.clone(),
                    trigger_recorder: assertion_contract.1,
                    active_at_block: 65,
                    log_index: 0,
                },
                PendingModification::Remove {
                    inactive_at_block: 65,
                    log_index: 1,
                    assertion_contract_id: id,
                    assertion_adopter,
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_find_common_ancestor_shallow_reorg() {
        let (indexer, _da, _anvil) = setup().await;

        // Mine and store initial blocks
        let _ = mine_block_write_hash(&indexer).await;
        let _ = mine_block_write_hash(&indexer).await;
        let _ = mine_block_write_hash(&indexer).await;

        let latest_block_num = indexer.provider.get_block_number().await.unwrap();
        let reorg_depth = 2;

        // Create reorg by resetting to block 1
        let reorg_options = alloy_rpc_types_anvil::ReorgOptions {
            depth: reorg_depth,
            tx_block_pairs: vec![],
        };

        indexer.provider.anvil_reorg(reorg_options).await.unwrap();

        // Mine new block on different fork
        let new_block = mine_block_write_hash(&indexer).await;

        // Find common ancestor
        let common_ancestor = indexer
            .find_common_ancestor(new_block.inner.parent_hash)
            .await
            .unwrap();

        // Common ancestor should be block 1
        assert_eq!(common_ancestor, latest_block_num - reorg_depth);
    }

    #[tokio::test]
    async fn test_find_common_ancestor_missing_blocks() {
        let (indexer, _da, _anvil) = setup().await;

        // Mine block but don't store it
        let _ = mine_block(&indexer.provider).await;

        // Create reorg
        let reorg_options = alloy_rpc_types_anvil::ReorgOptions {
            depth: 1,
            tx_block_pairs: vec![],
        };
        indexer.provider.anvil_reorg(reorg_options).await.unwrap();

        let new_block1 = mine_block(&indexer.provider).await;

        // Should fail to find common ancestor since no blocks are stored
        let result = indexer
            .find_common_ancestor(new_block1.inner.parent_hash)
            .await;

        assert!(matches!(result, Err(IndexerError::NoCommonAncestor)));
    }

    #[tokio::test]
    async fn test_stream_blocks_normal_case() {
        let (indexer, _da, _anvil) = setup().await;

        // Set up initial state
        let block0 = indexer
            .provider
            .get_block_by_number(0.into(), Default::default())
            .await
            .unwrap()
            .unwrap();

        // Store initial block in db
        //
        indexer
            .write_block_num_hash_batch(vec![BlockNumHash {
                number: block0.header.number,
                hash: block0.header.hash,
            }])
            .unwrap();

        // Create a block subscription
        let mut block_stream = indexer.provider.subscribe_blocks().await.unwrap();

        // Mine a new block
        let _ = indexer.provider.evm_mine(None).await;

        // Process the new block
        let latest_block = block_stream.recv().await.unwrap();
        indexer.handle_latest_block(latest_block).await.unwrap();

        // Verify block was properly indexed; Should have both blocks (0 and 1)
        assert_eq!(indexer.block_hash_tree.len(), 2);
    }

    #[tokio::test]
    async fn test_reorg() {
        for sync_before_mining in [false, true] {
            let (mut indexer, _da, _anvil) = setup().await;

            if sync_before_mining {
                indexer.sync_to_head().await.unwrap();
            }

            // Mine block 1
            let block1 = mine_block(&indexer.provider).await;

            if !sync_before_mining {
                indexer.sync_to_head().await.unwrap();
            }

            // Create a block subscription
            let mut block_stream = indexer.provider.subscribe_blocks().await.unwrap();

            // Simulate a reorg
            let reorg_options = alloy_rpc_types_anvil::ReorgOptions {
                depth: 1,
                tx_block_pairs: vec![],
            };
            indexer.provider.anvil_reorg(reorg_options).await.unwrap();

            // Process the newly mined block
            let latest_block = block_stream.recv().await.unwrap();

            #[allow(clippy::expect_fun_call)] // Error information is helpful, and the extra
            // context is as well.
            indexer.handle_latest_block(latest_block).await.expect(
                format!("Failed to handle latest block. Sync before mining: {sync_before_mining}",)
                    .as_str(),
            );

            // Verify state after reorg
            let last_indexed_block = indexer.get_last_indexed_block_num_hash().unwrap().unwrap();

            // Hash should be different fodf same block num
            assert_ne!(last_indexed_block.hash, block1.hash);
            assert_eq!(last_indexed_block.number, block1.number);

            // Should still have 2 blocks (0 and 1)
            assert_eq!(indexer.block_hash_tree.len(), 2, "Block hashes len");
        }
    }

    #[tokio::test]
    async fn test_move_to_store() {
        let (mut indexer, _da, _anvil) = setup().await;

        indexer.provider.evm_mine(None).await.unwrap();
        indexer.provider.evm_mine(None).await.unwrap();

        indexer.sync_to_head().await.unwrap();
        let aa = Address::random();
        // Add pending modification
        let modification = PendingModification::Add {
            assertion_adopter: aa,
            assertion_contract: AssertionContract::default(),
            trigger_recorder: TriggerRecorder::default(),
            active_at_block: 3,
            log_index: 0,
        };

        indexer
            .pending_modifications_tree
            .insert(
                bincode::serialize(&U256::from(2u64)).unwrap(),
                bincode::serialize(&vec![modification]).unwrap(),
            )
            .unwrap();

        assert_eq!(indexer.pending_modifications_tree.len(), 1);
        assert_eq!(indexer.block_hash_tree.len(), 3);

        let run_0 = (1, 3); // latest block 0 -> Do nothing
        let run_1 = (1, 1); // latest block 1 -> Remove block 0 and Block 1 hashes
        let run_2 = (0, 0); // latest block 2 -> Remove Block 2 hashes and Pending modification

        for (latest_block, (expected_pending_mods, expected_block_hashes)) in
            [run_0, run_1, run_2].iter().enumerate()
        {
            indexer
                .move_pending_modifications_to_store(latest_block as u64)
                .await
                .unwrap();

            let pending_mods_tree = &indexer.pending_modifications_tree;
            let block_hashes_tree = &indexer.block_hash_tree;

            assert_eq!(
                pending_mods_tree.len(),
                *expected_pending_mods,
                "pending_mod length unexpected, latest block: {latest_block}"
            );

            assert_eq!(
                block_hashes_tree.len(),
                *expected_block_hashes,
                "latest block: {latest_block}"
            );
        }
        let mut call_tracer = CallTracer::new();
        call_tracer.insert_trace(aa);

        let active_assertions_2 = indexer.store.read(&call_tracer, U256::from(2)).unwrap();
        let active_assertions_3 = indexer.store.read(&call_tracer, U256::from(3)).unwrap();

        assert_eq!(active_assertions_2.len(), 0);
        assert_eq!(active_assertions_3.len(), 1);
    }

    #[tokio::test]
    async fn test_sync_with_large_range() {
        // Set up the indexer
        let (indexer, da, _anvil) = setup_with_tag(BlockTag::Latest).await;
        tokio::task::spawn(da);

        // Mine a larger number of blocks to better test the chunking behavior
        let num_blocks = 100;

        for _ in 0..num_blocks {
            mine_block(&indexer.provider).await;
        }

        let (assertion_adopter, _) = send_modifications(&indexer).await;

        // Mine additional blocks to ensure the modification is processed
        for i in 0..5 {
            let block = mine_block(&indexer.provider).await;
            println!(
                "Mined additional block {}: number={}, hash={:?}",
                i, block.number, block.hash
            );
        }

        // Get the latest block
        let latest_block = indexer
            .provider
            .get_block_by_number(BlockNumberOrTag::Latest, BlockTransactionsKind::Hashes)
            .await
            .unwrap()
            .unwrap();

        let update_block = UpdateBlock {
            block_number: latest_block.header().number(),
            block_hash: latest_block.header().hash(),
            parent_hash: latest_block.header().parent_hash(),
        };
        indexer.sync(update_block, 50).await.unwrap();

        // Check if we have any pending modifications
        println!(
            "Pending modifications tree size: {}",
            indexer.pending_modifications_tree.len()
        );
        for (key, value) in indexer.pending_modifications_tree.iter().flatten() {
            let block_number: U256 = bincode::deserialize(&key).unwrap();
            let mods: Vec<PendingModification> = bincode::deserialize(&value).unwrap();
            println!("Block {}: {} modification(s)", block_number, mods.len());
        }
        // Directly move all pending modifications to the store
        let last_block_num = latest_block.header().number();
        indexer
            .move_pending_modifications_to_store(last_block_num)
            .await
            .unwrap();

        // Verify store contents
        let count = indexer.store.assertion_contract_count(assertion_adopter);

        // Verify the assertion was found
        assert_eq!(count, 1, "Assertion not found in store");
    }
}
