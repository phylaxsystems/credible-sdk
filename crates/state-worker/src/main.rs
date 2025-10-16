#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]

mod cli;
mod genesis;
mod genesis_data;
#[cfg(test)]
mod integration_tests;
mod macros;
mod redis;
mod state;
mod worker;

use crate::{
    cli::Args,
    redis::RedisStateWriter,
    worker::StateWorker,
};

use rust_tracing::trace;
use tracing::{
    info,
    warn,
};

use crate::redis::CircularBufferConfig;
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
use std::sync::Arc;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the rustls CryptoProvider for HTTPS support
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Install the shared tracing subscriber used across Credible services.
    trace();

    let args = Args::parse();

    let provider = connect_provider(&args.ws_url).await?;
    let redis = RedisStateWriter::new(
        &args.redis_url,
        args.redis_namespace.clone(),
        CircularBufferConfig::new(args.state_depth)?,
    )
    .context("failed to initialize redis client")?;
    let genesis_state =
        if let Some(chain_id) = args.chain_id {
            Some(crate::genesis::load_embedded(chain_id).with_context(|| {
                format!("failed to load embedded genesis for chain id {chain_id}")
            })?)
        } else {
            warn!("Chain Id not specified, not loading genesis block!");
            None
        };

    // Create shutdown channel
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

    let mut worker = StateWorker::new(provider, redis, genesis_state);

    // Run worker with shutdown signal
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
