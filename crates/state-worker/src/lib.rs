#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]

#[macro_use]
extern crate credible_utils;

pub mod genesis;
pub mod geth_version;
pub mod metrics;
pub mod state;
pub mod system_calls;
pub mod worker;

#[cfg(test)]
mod integration_tests;

use alloy_provider::{
    Provider,
    ProviderBuilder,
    RootProvider,
    WsConnect,
};
use anyhow::{
    Context,
    Result,
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

/// Establish a WebSocket connection to the execution node and expose the
/// underlying `RootProvider`. The root provider gives us access to the
/// subscription + debug APIs used throughout the worker.
///
/// # Errors
///
/// Returns an error if the WebSocket connection cannot be established.
pub async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("failed to connect to websocket provider")?;
    Ok(Arc::new(provider.root().clone()))
}

/// Validate that the connected execution client meets version requirements.
///
/// Specifically, if the client is Geth, it must be version 1.16.6 or later
/// to ensure correct prestateTracer diffMode behavior for post-Cancun
/// SELFDESTRUCT (EIP-6780).
///
/// # Errors
///
/// Returns an error if Geth version is too old or if the client version
/// cannot be retrieved.
pub async fn validate_geth_version(provider: &RootProvider) -> Result<()> {
    let client_version = provider
        .get_client_version()
        .await
        .context("failed to get client version via web3_clientVersion")?;

    info!(client_version = %client_version, "connected to execution client");

    // Parse Geth version string format: "Geth/v1.16.5-stable-abc123/linux-amd64/go1.23"
    // or similar variations like "Geth/v1.16.5/..."
    if let Some(version) = parse_geth_version(&client_version) {
        if version >= MIN_GETH_VERSION {
            info!(
                geth_version = %version,
                min_version = %MIN_GETH_VERSION,
                "Geth version validated"
            );
            return Ok(());
        }

        // Geth version is too old
        return Err(GethVersionError {
            current: version,
            minimum: MIN_GETH_VERSION,
        }
        .into());
    }

    // Not Geth or unrecognized format - allow to proceed
    // Other clients (Erigon, Nethermind, etc.) may have their own implementations
    warn!(
        client_version = %client_version,
        "connected client is not Geth or version could not be parsed; \
         skipping prestateTracer version validation. Ensure your client \
         correctly implements EIP-6780 SELFDESTRUCT semantics in traces."
    );
    Ok(())
}
