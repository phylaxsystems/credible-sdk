#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]

use clap::Parser;
use credible_utils::critical;
use rust_tracing::trace;
use state_worker::{
    cli::Args,
    runtime::{
        NoopRuntimeObserver,
        StateWorkerRuntimeConfig,
        run_supervisor,
        shutdown_signal,
    },
};
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::sync::broadcast;
use tracing::{
    info,
    warn,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize the rustls CryptoProvider for HTTPS support
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        warn!("Failed to install rustls crypto provider; continuing without default provider");
    }

    // Install the shared tracing subscriber used across Credible services.
    let _guard = trace();

    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(err) => {
            critical!(error = %err, "Failed to parse CLI args; waiting for restart");
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };

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

    let runtime_config = StateWorkerRuntimeConfig {
        ws_url: args.ws_url,
        mdbx_path: args.mdbx_path,
        start_block: args.start_block,
        state_depth: args.state_depth,
        genesis_path: args.file_to_genesis,
    };

    run_supervisor(&runtime_config, shutdown_rx, Arc::new(NoopRuntimeObserver)).await
}
