#![allow(clippy::needless_pass_by_value)]
//! State worker runner for embedded state worker support.
//!
//! Spawns the state worker on a dedicated OS thread with automatic restart
//! on crashes. The worker only stops when the shutdown flag is set.

use anyhow::{
    anyhow,
    bail,
};
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    thread::JoinHandle,
    time::Duration,
};
use tokio::sync::broadcast;
use tracing::{
    error,
    info,
    warn,
};

/// A constant representing the delay duration before a restart is attempted.
const RESTART_DELAY: Duration = Duration::from_secs(5);

/// Spawns the state worker on a dedicated OS thread with automatic restart.
///
/// The worker runs in an infinite loop, restarting on crashes unless
/// the shutdown flag is set. Each iteration creates its own tokio runtime.
///
/// Returns the thread handle and a receiver that signals when the thread exits.
pub fn spawn_state_worker(
    config: state_worker::Config,
    shutdown_flag: Arc<AtomicBool>,
) -> Result<(JoinHandle<()>, flume::Receiver<()>), anyhow::Error> {
    let (exited_tx, exited_rx) = flume::bounded(1);

    let handle = std::thread::Builder::new()
        .name("state-worker".into())
        .spawn(move || {
            run_state_worker_loop(config, shutdown_flag);
            let _ = exited_tx.send(());
        })
        .map_err(|e| anyhow!("failed to spawn state-worker thread: {e:?}"))?;

    Ok((handle, exited_rx))
}

/// Main loop for the state worker thread.
///
/// Creates a new tokio runtime for each iteration and restarts on failure.
fn run_state_worker_loop(config: state_worker::Config, shutdown_flag: Arc<AtomicBool>) {
    while !shutdown_flag.load(Ordering::Relaxed) {
        info!("Starting state worker...");

        // Create a new tokio runtime for this iteration
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("state-worker-async")
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                error!(error = ?e, "Failed to create tokio runtime for state worker");
                if !shutdown_flag.load(Ordering::Relaxed) {
                    std::thread::sleep(RESTART_DELAY);
                }
                continue;
            }
        };

        // Run the state worker
        let result = runtime.block_on(run_state_worker_inner(
            config.clone(),
            shutdown_flag.clone(),
        ));

        // Shutdown the runtime gracefully
        runtime.shutdown_timeout(Duration::from_secs(5));

        match result {
            Ok(()) => {
                info!("State worker exited cleanly");
                // Clean exit means shutdown was requested
                break;
            }
            Err(e) => {
                error!(error = ?e, "State worker crashed");

                if shutdown_flag.load(Ordering::Relaxed) {
                    info!("Shutdown requested, not restarting state worker");
                    break;
                }

                warn!(
                    delay_secs = RESTART_DELAY.as_secs(),
                    "Restarting state worker after delay..."
                );
                std::thread::sleep(RESTART_DELAY);
            }
        }
    }

    info!("State worker loop exited");
}

/// Inner async function that bridges `AtomicBool` -> broadcast and runs the worker.
async fn run_state_worker_inner(
    config: state_worker::Config,
    shutdown_flag: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    // Create broadcast channel for shutdown signaling
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    // Spawn a task that watches the AtomicBool and sends shutdown signal
    tokio::spawn({
        let shutdown_flag = shutdown_flag.clone();
        async move {
            loop {
                if shutdown_flag.load(Ordering::Relaxed) {
                    let _ = shutdown_tx.send(());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });

    // Run the state worker with our shutdown receiver
    state_worker::run(config, shutdown_rx).await
}
