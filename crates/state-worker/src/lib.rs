#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]
#![allow(clippy::missing_errors_doc)]
#![deny(clippy::panic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::unreachable)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![warn(clippy::indexing_slicing)]
#![cfg_attr(test, allow(clippy::panic))]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]
#![cfg_attr(test, allow(clippy::indexing_slicing))]

mod config;
mod genesis;
#[cfg(test)]
mod integration_tests;
mod macros;
mod metrics;
mod state;
mod system_calls;
mod worker;

pub use config::{
    Config,
    ProviderType,
};

use crate::{
    system_calls::SystemCalls,
    worker::StateWorker,
};

use tracing::info;

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
use state_store::{
    Writer,
    mdbx::{
        StateWriter,
        common::CircularBufferConfig,
    },
};
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::sync::broadcast;

/// Run the state worker with an external shutdown receiver.
///
/// This is the main entry point for running state-worker as a library.
/// The caller provides a broadcast receiver that signals when to shut down.
pub async fn run(config: Config, shutdown_rx: broadcast::Receiver<()>) -> Result<()> {
    let provider = connect_provider(&config.ws_url).await?;

    let writer = StateWriter::new(
        config.mdbx_path.as_str(),
        CircularBufferConfig::new(config.state_depth)?,
    )
    .context("failed to initialize database client")?;

    writer
        .ensure_dump_index_metadata()
        .context("failed to ensure database namespace index metadata")?;

    // Load genesis from file
    let file_path = &config.file_to_genesis;
    info!("Loading genesis from file: {}", file_path);
    let contents = std::fs::read_to_string(file_path)
        .with_context(|| format!("failed to read genesis file: {file_path}"))?;

    let genesis_state = genesis::parse_from_str(&contents)
        .with_context(|| format!("failed to parse genesis from file: {file_path}"))?;

    // Extract fork timestamps for system calls
    let system_calls = SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    // Create trace provider
    let trace_provider = state::create_trace_provider(
        config.provider_type,
        provider.clone(),
        Duration::from_secs(30),
    );

    let mut worker = StateWorker::new(
        provider,
        trace_provider,
        writer,
        Some(genesis_state),
        system_calls,
    );

    worker
        .run(config.start_block, shutdown_rx)
        .await
        .context("state worker terminated unexpectedly")
}

/// Establish a WebSocket connection to the execution node.
async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("failed to connect to websocket provider")?;
    Ok(Arc::new(provider.root().clone()))
}
