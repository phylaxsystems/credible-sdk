use crate::config::Config;
use alloy::{
    primitives::{
        B256,
        U256,
    },
    providers::WsConnect,
    rpc::types::{
        Block,
        BlockTransactions,
    },
};
use alloy_provider::{
    Provider,
    ProviderBuilder,
    RootProvider,
};
use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::overlay::OverlayDb,
    store::AssertionStore,
};
use revm::context_interface::block::BlobExcessGasAndPrice;
use shadow_driver::tx_env::to_proto_tx_env;
use sidecar::{
    CoreEngine,
    CoreEngineConfig,
    Sources,
    TransactionsState,
    cache::sources::{
        Source,
        eth_rpc_source::EthRpcSource,
    },
    engine::queue::{
        CommitHead,
        NewIteration,
        QueueTransaction,
        TxQueueContents,
    },
    execution_ids::TxExecutionId,
    transport::grpc::{
        GrpcDecodeError,
        convert_pb_tx_env_to_revm,
    },
};
use std::{
    error::Error as StdError,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::Duration,
};
use thiserror::Error;

const DEFAULT_ITERATION_ID: u64 = 0;
const SOURCE_SYNC_TIMEOUT: Duration = Duration::from_secs(10);

type DynError = Box<dyn StdError + Send + Sync + 'static>;

/// In-process runtime that feeds archive blocks into the sidecar engine queue.
pub(super) struct ReplayRuntime {
    provider: Arc<RootProvider>,
    tx_sender: flume::Sender<TxQueueContents>,
    shutdown_flag: Arc<AtomicBool>,
    engine_handle: Option<std::thread::JoinHandle<Result<(), sidecar::engine::EngineError>>>,
}

impl ReplayRuntime {
    /// Creates a new runtime, including provider/source connectivity and engine spawn.
    pub(super) async fn new(config: &Config) -> Result<Self, RuntimeError> {
        let provider = connect_provider(&config.archive_ws_url).await?;
        let state_source = EthRpcSource::try_build(
            config.archive_ws_url.as_str(),
            config.archive_http_url.as_str(),
        )
        .await
        .map_err(|source| RuntimeError::StateSourceBuild { source })?;
        wait_for_source_sync(state_source.as_ref(), U256::from(config.start_block)).await?;

        let sources: Vec<Arc<dyn Source>> = vec![state_source];
        let cache_sources = Arc::new(Sources::new(sources, 0));
        let cache: OverlayDb<Sources> = OverlayDb::new(Some(cache_sources.clone()));

        let assertion_executor = AssertionExecutor::new(
            ExecutorConfig::default()
                .with_chain_id(config.chain_id)
                .with_assertion_gas_limit(config.assertion_gas_limit),
            AssertionStore::new_ephemeral(),
        );

        let (tx_sender, tx_receiver) = flume::unbounded();
        let engine_state = TransactionsState::new();
        let engine = CoreEngine::new(
            cache,
            cache_sources,
            tx_receiver,
            assertion_executor,
            engine_state,
            CoreEngineConfig {
                transaction_results_max_capacity: 100_000,
                state_sources_sync_timeout: Duration::from_secs(15),
                source_monitoring_period: Duration::from_millis(250),
                overlay_cache_invalidation_every_block: false,
                incident_sender: None,
            },
        )
        .await;

        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let (engine_handle, _engine_exited) = engine
            .spawn(shutdown_flag.clone())
            .map_err(|source| RuntimeError::EngineSpawn { source })?;

        Ok(Self {
            provider,
            tx_sender,
            shutdown_flag,
            engine_handle: Some(engine_handle),
        })
    }

    /// Returns the current head block number from the archive provider.
    pub(super) async fn head_block_number(&self) -> Result<u64, RuntimeError> {
        self.provider.get_block_number().await.map_err(|source| {
            RuntimeError::HeadBlockQuery {
                source: Box::new(source),
            }
        })
    }

    /// Sends the initial commit event to establish the pre-state head.
    pub(super) async fn send_initial_commit(&self, start_block: u64) -> Result<(), RuntimeError> {
        let initial_commit_block = initial_commit_block(start_block);
        let block = fetch_block(&self.provider, initial_commit_block).await?;
        let commit = CommitHead::new(
            U256::from(initial_commit_block),
            DEFAULT_ITERATION_ID,
            None,
            0,
            block.header.hash,
            block.header.parent_beacon_block_root,
            U256::from(block.header.timestamp),
        );
        self.send_event(TxQueueContents::CommitHead(commit))
    }

    /// Fetches and processes all blocks in the inclusive range `[start, end]`.
    pub(super) async fn process_block_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<(), RuntimeError> {
        for block_number in start..=end {
            let block = fetch_block(&self.provider, block_number).await?;
            self.process_block(&block)?;
        }

        Ok(())
    }

    /// Processes one block by pushing `NewIteration`, `Tx*`, and `CommitHead` events.
    fn process_block(&self, block: &Block) -> Result<(), RuntimeError> {
        let block_number = block.header.number;
        let transactions = match &block.transactions {
            BlockTransactions::Full(txs) => txs,
            BlockTransactions::Hashes(_) => {
                return Err(RuntimeError::FullTransactionsExpected { block_number });
            }
            BlockTransactions::Uncle => {
                return Err(RuntimeError::UncleTransactionsPayload { block_number });
            }
        };

        let new_iteration = NewIteration::new(
            DEFAULT_ITERATION_ID,
            revm::context::BlockEnv {
                number: U256::from(block_number),
                beneficiary: block.header.beneficiary,
                timestamp: U256::from(block.header.timestamp),
                gas_limit: block.header.gas_limit,
                basefee: block.header.base_fee_per_gas.unwrap_or_default(),
                difficulty: block.header.difficulty,
                prevrandao: Some(block.header.mix_hash),
                blob_excess_gas_and_price: Some(BlobExcessGasAndPrice {
                    excess_blob_gas: 0,
                    blob_gasprice: 1,
                }),
            },
            Some(block.header.parent_hash),
            block.header.parent_beacon_block_root,
        );
        self.send_event(TxQueueContents::NewIteration(new_iteration))?;

        let mut prev_tx_hash = None;
        for (index, tx) in transactions.iter().enumerate() {
            let tx_hash = *tx.inner.hash();
            let tx_env = convert_pb_tx_env_to_revm(&to_proto_tx_env(tx))
                .map_err(|source| RuntimeError::TxEnvDecode { tx_hash, source })?;
            let queue_tx = QueueTransaction {
                tx_execution_id: TxExecutionId::new(
                    U256::from(block_number),
                    DEFAULT_ITERATION_ID,
                    tx_hash,
                    index as u64,
                ),
                tx_env,
                prev_tx_hash,
            };
            self.send_event(TxQueueContents::Tx(queue_tx))?;
            prev_tx_hash = Some(tx_hash);
        }

        let commit = CommitHead::new(
            U256::from(block_number),
            DEFAULT_ITERATION_ID,
            prev_tx_hash,
            transactions.len() as u64,
            block.header.hash,
            block.header.parent_beacon_block_root,
            U256::from(block.header.timestamp),
        );
        self.send_event(TxQueueContents::CommitHead(commit))?;

        Ok(())
    }

    /// Sends one event into the sidecar engine queue.
    fn send_event(&self, event: TxQueueContents) -> Result<(), RuntimeError> {
        self.tx_sender
            .send(event)
            .map_err(|_| RuntimeError::EngineQueueClosed)
    }

    /// Triggers shutdown signal and closes the producer queue side.
    fn initiate_shutdown(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);

        // Close the real sender immediately so the engine can terminate promptly.
        let (dummy_sender, _dummy_receiver) = flume::unbounded();
        let tx_sender = std::mem::replace(&mut self.tx_sender, dummy_sender);
        drop(tx_sender);
    }

    /// Gracefully shuts down the runtime and joins the engine thread.
    pub(super) async fn shutdown(mut self) -> Result<(), RuntimeError> {
        self.initiate_shutdown();

        if let Some(handle) = self.engine_handle.take() {
            let join_result = tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|source| RuntimeError::EngineJoinTask { source })?;

            let engine_result = join_result.map_err(|_| RuntimeError::EngineThreadPanic)?;
            engine_result.map_err(|source| RuntimeError::EngineThreadExit { source })?;
        }

        Ok(())
    }
}

impl Drop for ReplayRuntime {
    fn drop(&mut self) {
        self.initiate_shutdown();

        if let Some(handle) = self.engine_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Computes the commit block used for initial sidecar sync.
///
/// For non-zero `start_block`, this is `start_block - 1`.
/// For zero, it remains zero.
fn initial_commit_block(start_block: u64) -> u64 {
    if start_block == 0 { 0 } else { start_block - 1 }
}

/// Connects an archive websocket provider.
async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>, RuntimeError> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .map_err(|source| {
            RuntimeError::ProviderConnect {
                source: Box::new(source),
            }
        })?;
    Ok(Arc::new(provider.root().clone()))
}

/// Fetches one full block by number from the archive provider.
async fn fetch_block(
    provider: &Arc<RootProvider>,
    block_number: u64,
) -> Result<Block, RuntimeError> {
    let maybe_block = provider
        .get_block_by_number(block_number.into())
        .full()
        .await
        .map_err(|source| {
            RuntimeError::FetchBlock {
                block_number,
                source: Box::new(source),
            }
        })?;

    maybe_block.ok_or(RuntimeError::BlockNotFound { block_number })
}

/// Waits for `EthRpcSource` to report synced at the specified target block.
async fn wait_for_source_sync(
    source: &EthRpcSource,
    target_block: U256,
) -> Result<(), RuntimeError> {
    let start = tokio::time::Instant::now();
    while start.elapsed() < SOURCE_SYNC_TIMEOUT {
        if source.is_synced(target_block, target_block) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(RuntimeError::StateSourceSyncTimeout {
        target_block,
        timeout_secs: SOURCE_SYNC_TIMEOUT.as_secs(),
    })
}

#[derive(Debug, Error)]
pub(crate) enum RuntimeError {
    #[error("failed to connect to archive websocket provider")]
    ProviderConnect {
        #[source]
        source: DynError,
    },
    #[error("failed to build EthRpcSource")]
    StateSourceBuild {
        #[source]
        source: sidecar::cache::sources::eth_rpc_source::EthRpcSourceError,
    },
    #[error("EthRpcSource failed to sync to block {target_block} within {timeout_secs}s")]
    StateSourceSyncTimeout {
        target_block: U256,
        timeout_secs: u64,
    },
    #[error("failed to spawn sidecar engine thread")]
    EngineSpawn {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to query head block number from archive provider")]
    HeadBlockQuery {
        #[source]
        source: DynError,
    },
    #[error("failed to fetch block {block_number} from archive provider")]
    FetchBlock {
        block_number: u64,
        #[source]
        source: DynError,
    },
    #[error("block {block_number} not found on archive provider")]
    BlockNotFound { block_number: u64 },
    #[error("archive block {block_number} returned hashes instead of full transactions")]
    FullTransactionsExpected { block_number: u64 },
    #[error("archive block {block_number} returned unsupported uncle payload")]
    UncleTransactionsPayload { block_number: u64 },
    #[error("failed to convert transaction env for tx {tx_hash}")]
    TxEnvDecode {
        tx_hash: B256,
        #[source]
        source: GrpcDecodeError,
    },
    #[error("engine queue channel closed while sending event")]
    EngineQueueClosed,
    #[error("failed to await engine join task")]
    EngineJoinTask {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("engine thread panicked")]
    EngineThreadPanic,
    #[error("engine thread exited with error")]
    EngineThreadExit {
        #[source]
        source: sidecar::engine::EngineError,
    },
}

#[cfg(test)]
mod tests {
    use super::initial_commit_block;

    #[test]
    fn initial_commit_block_is_zero_for_zero_start() {
        assert_eq!(initial_commit_block(0), 0);
    }

    #[test]
    fn initial_commit_block_is_previous_for_non_zero_start() {
        assert_eq!(initial_commit_block(42), 41);
    }
}
