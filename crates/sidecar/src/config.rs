//! Configuration module for initializing sidecar components

use crate::args::SidecarArgs;
use assertion_executor::{
    ExecutorConfig,
    store::{
        AssertionStore,
        AssertionStoreError,
    },
};
use tracing::{
    debug,
    info,
};

/// Initialize ExecutorConfig from SidecarArgs
pub fn init_executor_config(args: &SidecarArgs) -> ExecutorConfig {
    let config = ExecutorConfig::default()
        .with_spec_id(args.rollup.spec_id.clone().into())
        .with_assertion_gas_limit(args.credible.assertion_gas_limit);

    debug!(
        spec_id = ?config.spec_id,
        chain_id = config.chain_id,
        assertion_gas_limit = config.assertion_gas_limit,
        "Initialized ExecutorConfig"
    );

    config
}

/// Initialize AssertionStore from SidecarArgs
pub fn init_assertion_store(args: &SidecarArgs) -> Result<AssertionStore, AssertionStoreError> {
    let mut db_config = sled::Config::new();
    db_config = db_config.path(&args.credible.db_path);

    if let Some(cache_capacity) = args.credible.cache_capacity_bytes {
        db_config = db_config.cache_capacity_bytes(cache_capacity);
    }

    if let Some(flush_ms) = args.credible.flush_every_ms {
        db_config = db_config.flush_every_ms(Some(flush_ms));
    }

    let db = db_config.open()?;

    info!(
        db_path = ?args.credible.db_path,
        cache_capacity = ?args.credible.cache_capacity_bytes,
        flush_every_ms = ?args.credible.flush_every_ms,
        "Initialized persistent AssertionStore"
    );

    Ok(AssertionStore::new(db))
}
