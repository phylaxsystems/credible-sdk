mod cli;

use anyhow::{
    Context,
    Result,
};
use clap::Parser;
use credible_utils::critical;
use futures_util::FutureExt;
use mdbx::StateWriter;
use rust_tracing::trace;
use state_worker::{
    connect_provider,
    embedded::EmbeddedStateWorkerRuntime,
    genesis,
    metrics,
    state,
    system_calls::SystemCalls,
    validate_geth_version,
    worker::StateWorker,
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

use crate::cli::Args;

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

    let mut restart_count: u64 = 0;
    loop {
        let result = AssertUnwindSafe(run_once(&args)).catch_unwind().await;

        match result {
            Ok(Ok(())) => warn!("state worker exited; restarting"),
            Ok(Err(err)) => warn!(error = %err, "state worker failed; restarting"),
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

async fn run_once(args: &Args) -> Result<()> {
    let provider = connect_provider(&args.ws_url).await?;
    validate_geth_version(&provider).await?;

    let writer_reader = match StateWriter::new(args.mdbx_path.as_str()) {
        Ok(writer_reader) => {
            metrics::set_db_healthy(true);
            writer_reader
        }
        Err(err) => {
            metrics::set_db_healthy(false);
            return Err(err).context("failed to initialize database client");
        }
    };

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

    let system_calls = SystemCalls::new(
        genesis_state.config().cancun_time,
        genesis_state.config().prague_time,
    );

    info!(
        cancun_time = ?system_calls.cancun_time,
        prague_time = ?system_calls.prague_time,
        "Configured system call fork timestamps"
    );

    let trace_provider = state::create_trace_provider(provider.clone(), Duration::from_secs(30));
    let worker = StateWorker::new(
        provider,
        trace_provider,
        writer_reader,
        Some(genesis_state),
        system_calls,
    );
    let mut runtime = EmbeddedStateWorkerRuntime::new(worker).with_auto_advance_commit_target();

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(err) = shutdown_signal().await {
            warn!("Error setting up signal handler: {}", err);
        } else {
            info!("Shutdown signal received, initiating graceful shutdown...");
            let _ = shutdown_tx_clone.send(());
        }
    });

    match runtime.run(args.start_block, shutdown_rx).await {
        Ok(()) => {
            info!("State worker shutdown gracefully");
            Ok(())
        }
        Err(err) => Err(err).context("state worker terminated unexpectedly"),
    }
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
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = sigint.recv() => info!("Received SIGINT"),
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
