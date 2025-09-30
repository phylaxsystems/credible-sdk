#![doc = include_str!("../README.md")]

mod cli;
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
    let redis = RedisStateWriter::new(&args.redis_url, args.redis_namespace.clone())
        .context("failed to initialize redis client")?;

    let mut worker = StateWorker::new(provider, redis);
    worker
        .run(args.start_block)
        .await
        .context("state worker terminated unexpectedly")
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
