#![recursion_limit = "1024"]

#[macro_use]
extern crate credible_utils;

mod cli;

use cli::Args;
use state_worker::{
    connect_provider,
    metrics,
    state,
    system_calls::SystemCalls,
    validate_geth_version,
    worker::StateWorker,
};

use futures_util::FutureExt;
use rust_tracing::trace;
use tracing::{
    info,
    warn,
};

use anyhow::{
    Context,
    Result,
};
use clap::Parser;
use mdbx::{
    StateWriter,
    common::CircularBufferConfig,
};
use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::Duration,
};

#[tokio::main]
async fn main() -> Result<()> {
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

    let mut restart_count: u64 = 0;
    loop {
        let result = AssertUnwindSafe(run_once(&args)).catch_unwind().await;

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

async fn run_once(args: &Args) -> Result<()> {
    let provider = connect_provider(&args.ws_url).await?;

    // Validate Geth version for prestateTracer diffMode EIP-6780 correctness
    validate_geth_version(&provider).await?;
    let writer_reader = match StateWriter::new(
        args.mdbx_path.as_str(),
        CircularBufferConfig::new(args.state_depth)?,
    ) {
        Ok(writer_reader) => {
            metrics::set_db_healthy(true);
            writer_reader
        }
        Err(err) => {
            metrics::set_db_healthy(false);
            return Err(err).context("failed to initialize database client");
        }
    };

    // Load genesis from file (required to seed initial state)
    let file_path = &args.file_to_genesis;
    info!("Loading genesis from file: {}", file_path);
    let contents = std::fs::read_to_string(file_path)
        .inspect_err(|e| warn!(error = ?e, file_path = file_path, "Failed to read genesis file"))
        .with_context(|| format!("failed to read genesis file: {file_path}"))?;
    let genesis_state = state_worker::genesis::parse_from_str(&contents)
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
    let trace_provider = state::create_trace_provider(provider.clone(), Duration::from_secs(30));

    // Shared shutdown flag: set to true on SIGTERM/SIGINT.
    let shutdown = Arc::new(AtomicBool::new(false));

    // Spawn signal handler that sets the shutdown flag.
    let shutdown_flag = Arc::clone(&shutdown);
    tokio::spawn(async move {
        if let Err(e) = shutdown_signal().await {
            warn!("Error setting up signal handler: {}", e);
        } else {
            info!("Shutdown signal received, initiating graceful shutdown...");
            shutdown_flag.store(true, Ordering::Release);
        }
    });

    // Standalone mode: no commit_head channel or signal — blocks are written
    // directly to MDBX in process_block().
    let mut worker = StateWorker::new(
        provider,
        trace_provider,
        writer_reader,
        Some(genesis_state),
        system_calls,
        None, // commit_head_rx
        None, // committed_head_signal
        None, // buffer_capacity
    );

    match worker.run(args.start_block, shutdown).await {
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
