mod cli;
mod listener;

use crate::{
    cli::Args,
    listener::Listener,
};

use rust_tracing::trace;

use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
    // Install the shared tracing subscriber used across Credible services.
    trace();

    let args = Args::parse();

    let mut listener = Listener::new(&args.ws_url, &args.sidecar_url, args.starting_block)
        .await
        .with_result_querying(true);

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
