//! Configuration module for initializing sidecar components

use crate::args::Config;
use assertion_da_client::DaClient;
use assertion_executor::{
    ExecutorConfig,
    store::{
        AssertionStore,
        AssertionStoreError,
        PruneConfig,
        ShovelConsumerCfg,
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

/// Initialize `ShovelConsumerCfg` from sidecar config
pub fn init_shovel_consumer_config(
    config: &Config,
    store: AssertionStore,
    executor_config: &ExecutorConfig,
    da_client: DaClient,
) -> ShovelConsumerCfg {
    debug!(
        shovel_pg_url = ?config.credible.shovel_pg_url,
        da_url = ?config.credible.assertion_da_rpc_url,
        poll_interval_ms = ?config.credible.shovel_poll_interval_ms,
        "Initializing ShovelConsumerCfg"
    );

    ShovelConsumerCfg {
        pg_url: config.credible.shovel_pg_url.clone(),
        da_client,
        executor_config: executor_config.clone(),
        store,
        poll_interval_ms: config.credible.shovel_poll_interval_ms.unwrap_or(1000),
    }
}
