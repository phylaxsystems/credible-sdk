//! Configuration module for initializing sidecar components

use crate::args::Config;
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
        PruneConfig,
    },
};
use tracing::{
    debug,
    info,
    trace,
};

/// Initialize `ExecutorConfig` from `SidecarArgs`
pub fn init_executor_config(config: &Config) -> ExecutorConfig {
    let config = ExecutorConfig::default()
        .with_spec_id(config.chain.spec_id)
        .with_chain_id(config.chain.chain_id)
        .with_assertion_gas_limit(config.credible.assertion_gas_limit);

    debug!(
        spec_id = ?config.spec_id,
        chain_id = config.chain_id,
        assertion_gas_limit = config.assertion_gas_limit,
        "Initialized ExecutorConfig"
    );

    config
}

/// Initialize `AssertionStore` from `SidecarArgs`
pub fn init_assertion_store(config: &Config) -> Result<AssertionStore, AssertionStoreError> {
    let mut db_config = sled::Config::new();
    db_config = db_config.path(&config.credible.assertion_store_db_path);

    if let Some(cache_capacity) = config.credible.cache_capacity_bytes {
        db_config = db_config.cache_capacity_bytes(cache_capacity);
    }

    if let Some(flush_ms) = config.credible.flush_every_ms {
        db_config = db_config.flush_every_ms(Some(flush_ms));
    }

    let db = db_config.open().map_err(AssertionStoreError::SledError)?;

    info!(
        db_path = ?config.credible.assertion_store_db_path,
        cache_capacity = ?config.credible.cache_capacity_bytes,
        flush_every_ms = ?config.credible.flush_every_ms,
        "Initialized persistent AssertionStore"
    );

    Ok(AssertionStore::new(
        db,
        PruneConfig {
            interval_ms: config
                .credible
                .assertion_store_prune_config_interval_ms
                .unwrap_or(60_000),
            retention_blocks: config
                .credible
                .assertion_store_prune_config_retention_blocks
                .unwrap_or_default(),
        },
    ))
}

/// Initialize `IndexerCfg` from `SidecarArgs`
pub async fn init_indexer_config(
    config: &Config,
    store: AssertionStore,
    executor_config: &ExecutorConfig,
) -> anyhow::Result<IndexerCfg> {
    trace!(
        state_oracle = ?config.credible.state_oracle,
        state_oracle_deployment_block = ?config.credible.state_oracle_deployment_block,
        da_url = ?config.credible.assertion_da_rpc_url,
        indexer_rpc = ?config.credible.indexer_rpc_url,
        indexer_db_path = ?config.credible.indexer_db_path,
        block_tag = ?config.credible.block_tag,
        "Initializing indexer"
    );

    // Initialize DA client
    let da_client = DaClient::new(&config.credible.assertion_da_rpc_url)?;

    // Initialize provider for blockchain connection
    let ws_connect = WsConnect::new(&config.credible.indexer_rpc_url);
    let provider = ProviderBuilder::new().connect_ws(ws_connect).await?;
    let provider = provider.root().clone();

    // Initialize indexer database
    let mut indexer_db_config = sled::Config::new();
    indexer_db_config = indexer_db_config.path(&config.credible.indexer_db_path);

    if let Some(cache_capacity) = config.credible.cache_capacity_bytes {
        indexer_db_config = indexer_db_config.cache_capacity_bytes(cache_capacity);
    }

    if let Some(flush_ms) = config.credible.flush_every_ms {
        indexer_db_config = indexer_db_config.flush_every_ms(Some(flush_ms));
    }

    let indexer_db = indexer_db_config.open()?;

    debug!(
        state_oracle = ?config.credible.state_oracle,
        state_oracle_deployment_block = ?config.credible.state_oracle_deployment_block,
        da_url = ?config.credible.assertion_da_rpc_url,
        indexer_rpc = ?config.credible.indexer_rpc_url,
        indexer_db_path = ?config.credible.indexer_db_path,
        block_tag = ?config.credible.block_tag,
        "Initialized IndexerCfg"
    );

    Ok(IndexerCfg {
        state_oracle: config.credible.state_oracle,
        state_oracle_deployment_block: config.credible.state_oracle_deployment_block,
        da_client,
        executor_config: executor_config.clone(),
        store,
        provider,
        db: indexer_db,
        await_tag: config.credible.block_tag,
    })
}
