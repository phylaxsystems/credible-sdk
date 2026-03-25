use anyhow::Result;
use clap::Parser;
use credible_utils::critical;
use futures_util::FutureExt;
use rust_tracing::trace;
use state_worker::{
    Args,
    StateWorkerConfig,
    run_standalone_state_worker_once,
    shutdown_signal,
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

const INITIAL_RESTART_DELAY: Duration = Duration::from_secs(1);
const MAX_RESTART_DELAY: Duration = Duration::from_secs(32);

#[tokio::main]
async fn main() -> Result<()> {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        warn!("Failed to install rustls crypto provider; continuing without default provider");
    }

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

    let config = StateWorkerConfig::from(args);

    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown_signal_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(err) = shutdown_signal().await {
            warn!("Error setting up signal handler: {}", err);
        } else {
            info!("Shutdown signal received, initiating graceful shutdown...");
            let _ = shutdown_signal_tx.send(());
        }
    });

    let mut restart_count: u64 = 0;
    let mut restart_delay = INITIAL_RESTART_DELAY;
    loop {
        let result = AssertUnwindSafe(run_standalone_state_worker_once(
            &config,
            shutdown_tx.subscribe(),
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
        info!(
            restart_count,
            restart_delay_secs = restart_delay.as_secs(),
            "restarting state worker"
        );
        tokio::time::sleep(restart_delay).await;
        restart_delay = (restart_delay * 2).min(MAX_RESTART_DELAY);
    }
}
