#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]

#[macro_use]
extern crate credible_utils;

pub mod embedded;
pub mod genesis;
pub mod geth_version;
pub mod metrics;
pub mod state;
pub mod system_calls;
pub mod worker;

#[cfg(test)]
pub mod integration_tests {
    pub mod mdbx_fixture;
    pub mod setup;
}

pub use embedded::{
    CommitTargetHandle,
    EmbeddedStateWorkerHandle,
    EmbeddedStateWorkerRuntime,
    WorkerStatus,
    WorkerStatusSnapshot,
};
pub use worker::StateWorker;

use alloy_provider::{
    Provider,
    ProviderBuilder,
    RootProvider,
    WsConnect,
};
use anyhow::{
    Context as AnyhowContext,
    Result as AnyhowResult,
};
use geth_version::{
    GethVersionError,
    MIN_GETH_VERSION,
    parse_geth_version,
};
use std::sync::Arc;
use tracing::{
    info,
    warn,
};

#[cfg(test)]
include!("integration_tests/tests.rs");

/// Establish a WebSocket connection to the execution node and expose the
/// underlying `RootProvider`.
pub async fn connect_provider(ws_url: &str) -> AnyhowResult<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("failed to connect to websocket provider")?;
    Ok(Arc::new(provider.root().clone()))
}

/// Validate that the connected execution client meets version requirements.
pub async fn validate_geth_version(provider: &RootProvider) -> AnyhowResult<()> {
    let client_version = provider
        .get_client_version()
        .await
        .context("failed to get client version via web3_clientVersion")?;

    info!(client_version = %client_version, "connected to execution client");

    if let Some(version) = parse_geth_version(&client_version) {
        if version >= MIN_GETH_VERSION {
            info!(
                geth_version = %version,
                min_version = %MIN_GETH_VERSION,
                "Geth version validated"
            );
            return Ok(());
        }

        return Err(GethVersionError {
            current: version,
            minimum: MIN_GETH_VERSION,
        }
        .into());
    }

    warn!(
        client_version = %client_version,
        "connected client is not Geth or version could not be parsed; \
         skipping prestateTracer version validation. Ensure your client \
         correctly implements EIP-6780 SELFDESTRUCT semantics in traces."
    );
    Ok(())
}
