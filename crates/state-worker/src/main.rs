#![doc = include_str!("../README.md")]

use anyhow::{
    Context,
    Result,
};
use clap::Parser;
use credible_utils::critical;
use rust_tracing::trace;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{
    info,
    warn,
};

use state_worker::{
    WorkerConfig,
    cli::Args,
    run_supervisor_loop,
};

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = trace();

    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        warn!("Failed to install rustls crypto provider; continuing without default provider");
    }

    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(err) => {
            critical!(error = %err, "Failed to parse CLI args; waiting for restart");
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };

    let shutdown = CancellationToken::new();
    let shutdown_signal_token = shutdown.clone();
    tokio::spawn(async move {
        if let Err(e) = shutdown_signal().await {
            warn!("Error setting up signal handler: {}", e);
        } else {
            info!("Shutdown signal received, initiating graceful shutdown...");
            shutdown_signal_token.cancel();
        }
    });

    let config = match WorkerConfig::try_from(args) {
        Ok(config) => config,
        Err(err) => {
            critical!(error = %err, "Failed to build worker config; waiting for restart");
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };

    run_supervisor_loop(config, None, shutdown).await
}

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
