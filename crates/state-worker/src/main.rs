#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]
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

mod cli;
mod genesis;
#[cfg(test)]
mod integration_tests;
mod macros;
mod metrics;
mod state;
mod system_calls;
mod worker;

use crate::{
    cli::Args,
    system_calls::SystemCalls,
    worker::StateWorker,
};

use rust_tracing::trace;
use tracing::{
    info,
    warn,
};

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
use clap::Parser;
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

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the rustls CryptoProvider for HTTPS support
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("Failed to install rustls crypto provider"))?;

    // Install the shared tracing subscriber used across Credible services.
    let _guard = trace();

    let args = Args::parse();

    let provider = connect_provider(&args.ws_url).await?;
    let writer_reader = StateWriter::new(
        args.mdbx_path.as_str(),
        CircularBufferConfig::new(args.state_depth)?,
    )
    .context("failed to initialize database client")?;
    writer_reader
        .ensure_dump_index_metadata()
        .context("failed to ensure database namespace index metadata")?;

    // Load genesis from file (required to seed initial state)
    let file_path = &args.file_to_genesis;
    info!("Loading genesis from file: {}", file_path);
    let contents = std::fs::read_to_string(file_path)
        .inspect_err(|e| warn!(error = ?e, file_path = file_path, "Failed to read genesis file"))
        .with_context(|| format!("failed to read genesis file: {file_path}"))?;
    let genesis_state = genesis::parse_from_str(&contents)
        .inspect_err(
            |e| warn!(error = ?e, file_path = file_path, "Failed to parse genesis from file"),
        )
        .with_context(|| format!("failed to parse genesis from file: {file_path}"))?;

    // Extract fork timestamps for system calls before consuming genesis
    let system_calls = SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    // Create the trace provider based on config
    let trace_provider = state::create_trace_provider(
        args.provider_type,
        provider.clone(),
        Duration::from_secs(30), // default timeout
    );

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    // Spawn signal handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = shutdown_signal().await {
            warn!("Error setting up signal handler: {}", e);
        } else {
            info!("Shutdown signal received, initiating graceful shutdown...");
            let _ = shutdown_tx_clone.send(());
        }
    });

    let mut worker = StateWorker::new(
        provider,
        trace_provider,
        writer_reader,
        Some(genesis_state),
        system_calls,
    );

    match worker.run(args.start_block, shutdown_rx).await {
        Ok(()) => {
            info!("State worker shutdown gracefully");
            Ok(())
        }
        Err(e) => Err(e).context("state worker terminated unexpectedly"),
    }
}

/// Wait for SIGTERM or SIGINT (Ctrl+C)
async fn shutdown_signal() -> Result<()> {
    use tokio::signal;

    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .context("failed to install SIGTERM handler")?;
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .context("failed to install SIGINT handler")?;

        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("Received SIGINT");
            }
        }
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c()
            .await
            .context("failed to listen for ctrl-c")?;
        info!("Received Ctrl+C");
    }

    Ok(())
}

/// Establish a WebSocket connection to the execution node and expose the
/// underlying `RootProvider`. The root provider gives us access to the
/// subscription + debug APIs used throughout the worker.
async fn connect_provider(ws_url: &str) -> Result<Arc<RootProvider>> {
    let ws = WsConnect::new(ws_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .context("failed to connect to websocket provider")?;
    Ok(Arc::new(provider.root().clone()))
}
