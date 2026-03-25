use clap::Parser;
use credible_utils::critical;
use futures_util::FutureExt;
use rust_tracing::trace;
use state_worker::{
    StateWorkerConfig,
    StateWorkerMode,
    run_state_worker_once,
};
use std::{
    panic::AssertUnwindSafe,
    time::Duration,
};
use tokio::sync::broadcast;
use tracing::{
    info,
    warn,
};

mod cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        warn!("Failed to install rustls crypto provider; continuing without default provider");
    }

    let _guard = trace();

    let args = match cli::Args::try_parse() {
        Ok(args) => args,
        Err(err) => {
            critical!(error = %err, "Failed to parse CLI args; waiting for restart");
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };

    let config = StateWorkerConfig {
        ws_url: args.ws_url,
        mdbx_path: args.mdbx_path,
        start_block: args.start_block,
        file_to_genesis: args.file_to_genesis,
        buffered_blocks_capacity: 1,
    };

    let mut restart_count: u64 = 0;
    loop {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = shutdown_signal().await {
                warn!(error = %err, "Error setting up signal handler");
            } else {
                info!("Shutdown signal received, initiating graceful shutdown...");
                let _ = shutdown_tx_clone.send(());
            }
        });

        let result = AssertUnwindSafe(run_state_worker_once(
            &config,
            &StateWorkerMode::Immediate,
            shutdown_rx,
        ))
        .catch_unwind()
        .await;

        match result {
            Ok(Ok(())) => {
                warn!("state worker exited; restarting");
            }
            Ok(Err(err)) => {
                warn!(error = %err, "state worker failed; restarting");
            }
            Err(panic_payload) => {
                if let Some(message) = panic_payload.downcast_ref::<&str>() {
                    warn!(panic = %message, "state worker panicked; restarting");
                } else if let Some(message) = panic_payload.downcast_ref::<String>() {
                    warn!(panic = %message, "state worker panicked; restarting");
                } else {
                    warn!("state worker panicked; restarting");
                }
            }
        }

        restart_count = restart_count.saturating_add(1);
        let restart_delay = Duration::from_secs(1);
        info!(
            restart_count,
            restart_delay_secs = restart_delay.as_secs(),
            "restarting state worker"
        );
        tokio::time::sleep(restart_delay).await;
    }
}

async fn shutdown_signal() -> Result<(), std::io::Error> {
    use tokio::signal;

    #[cfg(unix)]
    {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

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
        signal::ctrl_c().await?;
        info!("Received Ctrl+C");
    }

    Ok(())
}
