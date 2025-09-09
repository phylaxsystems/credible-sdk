//! Configuration module for initializing sidecar components

use crate::args::SidecarArgs;
use alloy_provider::{
    Provider,
    ProviderBuilder,
    WsConnect,
};
use assertion_da_client::DaClient;
use assertion_executor::{
    ExecutorConfig,
    store::{
        AssertionStore,
        AssertionStoreError,
        IndexerCfg,
    },
};
use tracing::{
    debug,
    info,
    trace,
};

/// Initialize `ExecutorConfig` from `SidecarArgs`
pub fn init_executor_config(args: &SidecarArgs) -> ExecutorConfig {
    let config = ExecutorConfig::default()
        .with_spec_id(args.chain.spec_id.clone().into())
        .with_chain_id(args.chain.chain_id)
        .with_assertion_gas_limit(args.credible.assertion_gas_limit);

    debug!(
        spec_id = ?config.spec_id,
        chain_id = config.chain_id,
        assertion_gas_limit = config.assertion_gas_limit,
        "Initialized ExecutorConfig"
    );

    config
}

/// Initialize `AssertionStore` from `SidecarArgs`
pub fn init_assertion_store(args: &SidecarArgs) -> Result<AssertionStore, AssertionStoreError> {
    let mut db_config = sled::Config::new();
    db_config = db_config.path(&args.credible.assertion_executor_db_path);

    if let Some(cache_capacity) = args.credible.cache_capacity_bytes {
        db_config = db_config.cache_capacity_bytes(cache_capacity);
    }

    if let Some(flush_ms) = args.credible.flush_every_ms {
        db_config = db_config.flush_every_ms(Some(flush_ms));
    }

    let db = db_config.open()?;

    info!(
        db_path = ?args.credible.assertion_executor_db_path,
        cache_capacity = ?args.credible.cache_capacity_bytes,
        flush_every_ms = ?args.credible.flush_every_ms,
        "Initialized persistent AssertionStore"
    );

    Ok(AssertionStore::new(db))
}

/// Initialize `IndexerCfg` from `SidecarArgs`
pub async fn init_indexer_config(
    args: &SidecarArgs,
    store: AssertionStore,
    executor_config: &ExecutorConfig,
) -> anyhow::Result<IndexerCfg> {
    trace!(
        da_url = %args.credible.assertion_da_rpc_url,
        indexer_rpc = %args.credible.indexer_rpc_url,
        indexer_db_path = ?args.credible.indexer_db_path,
        block_tag = ?args.credible.block_tag,
        "Initializing indexer"
    );

    // Initialize DA client
    let da_client = DaClient::new(&args.credible.assertion_da_rpc_url)?;

    // Initialize provider for blockchain connection
    let ws_connect = WsConnect::new(&args.credible.indexer_rpc_url);
    let provider = ProviderBuilder::new().connect_ws(ws_connect).await?;
    let provider = provider.root().clone();

    // Initialize indexer database
    let mut indexer_db_config = sled::Config::new();
    indexer_db_config = indexer_db_config.path(&args.credible.indexer_db_path);

    if let Some(cache_capacity) = args.credible.cache_capacity_bytes {
        indexer_db_config = indexer_db_config.cache_capacity_bytes(cache_capacity);
    }

    if let Some(flush_ms) = args.credible.flush_every_ms {
        indexer_db_config = indexer_db_config.flush_every_ms(Some(flush_ms));
    }

    let indexer_db = indexer_db_config.open()?;

    debug!(
        state_oracle = ?args.credible.state_oracle,
        da_url = ?args.credible.assertion_da_rpc_url,
        indexer_rpc = ?args.credible.indexer_rpc_url,
        indexer_db_path = ?args.credible.indexer_db_path,
        block_tag = ?args.credible.block_tag,
        "Initialized IndexerCfg"
    );

    Ok(IndexerCfg {
        state_oracle: args.credible.state_oracle,
        da_client,
        executor_config: executor_config.clone(),
        store,
        provider,
        db: indexer_db,
        await_tag: args.credible.block_tag,
    })
}
