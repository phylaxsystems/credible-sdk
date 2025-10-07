mod cli;
mod listener;

use crate::{
    cli::Args,
    listener::Listener,
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
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
    // Install the shared tracing subscriber used across Credible services.
    trace();

    let args = Args::parse();

    let provider = connect_provider(&args.ws_url).await?;
    let mut listener = Listener::new(provider, &args.sidecar_url, args.request_timeout_seconds);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down gracefully...");
        }
        () = wait_for_sigterm() => {
            tracing::info!("Received SIGTERM, shutting down gracefully...");
        }
        result = listener.run() => {
            match result {
                Ok(()) => tracing::info!("Listener exited normally"),
                Err(e) => tracing::error!(error = ?e, "Listener exited with error"),
            }
        }
    }
    Ok(())
}

async fn wait_for_sigterm() {
    use tokio::signal::unix::{
        SignalKind,
        signal,
    };
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to setup SIGTERM handler");
    sigterm.recv().await;
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
