//! Configuration module for initializing sidecar components

use crate::{
    args::Config,
    graphql_event_source::{
        GraphqlEventSource,
        GraphqlEventSourceConfig,
    },
    indexer_syncer::IndexerCfg,
};
use assertion_da_client::DaClient;
use assertion_executor::{
    ExecutorConfig,
    store::{
        AssertionStore,
        AssertionStoreError,
        EventSource,
        EventSourceError,
        PruneConfig,
    },
};
use tracing::{
    debug,
    info,
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

/// Initialize `IndexerCfg` from config.
///
/// Creates the event source and indexer configuration for assertion events.
/// Performs an initial health check to verify the event source is reachable.
pub async fn init_indexer_config(
    config: &Config,
    store: AssertionStore,
    executor_config: &ExecutorConfig,
    da_client: DaClient,
) -> Result<IndexerCfg<GraphqlEventSource>, EventSourceError> {
    let event_source = GraphqlEventSource::new(GraphqlEventSourceConfig {
        graphql_url: config.credible.event_source_url.clone(),
    });

    let poll_interval = config.credible.poll_interval;

    // Verify the event source is reachable before proceeding
    event_source.health_check().await?;

    info!(
        event_source_url = ?config.credible.event_source_url,
        poll_interval_ms = poll_interval.as_millis(),
        "Initialized IndexerCfg (event source healthy)"
    );

    Ok(IndexerCfg {
        event_source,
        store,
        da_client,
        executor_config: executor_config.clone(),
        poll_interval,
    })
}
