use crate::service::{
    WorkerRuntimeConfig,
    run_worker_once,
};
use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    thread::JoinHandle,
    time::Duration,
};
use tokio::{
    runtime::Builder,
    sync::{
        broadcast,
        watch,
    },
};
use tracing::{
    info,
    warn,
};

#[derive(Clone, Debug)]
pub struct EmbeddedStateWorkerConfig {
    pub runtime: WorkerRuntimeConfig,
    pub restart_backoff: Duration,
}

#[derive(Clone, Debug)]
pub struct CommitHeadHandle {
    tx: watch::Sender<u64>,
}

impl CommitHeadHandle {
    pub fn flush_up_to(&self, block_number: u64) {
        let _ = self.tx.send(block_number);
    }
}

pub struct EmbeddedStateWorkerHandle {
    pub commit_head: CommitHeadHandle,
    pub committed_head: Arc<AtomicU64>,
    pub thread: JoinHandle<()>,
}

/// Spawn the embedded state worker supervisor on a dedicated OS thread.
///
/// # Errors
///
/// Returns an error if the supervisor thread cannot be spawned.
pub fn spawn_embedded_state_worker(
    config: EmbeddedStateWorkerConfig,
    shutdown: Arc<AtomicBool>,
    committed_head: Arc<AtomicU64>,
) -> std::io::Result<EmbeddedStateWorkerHandle> {
    let initial_commit_head = committed_head.load(Ordering::Acquire);
    let (flush_tx, flush_rx) = watch::channel(initial_commit_head);
    let committed_head_for_thread = Arc::clone(&committed_head);

    let thread = std::thread::Builder::new()
        .name("state-worker".to_string())
        .spawn(move || {
            let runtime = match Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    warn!(error = %error, "failed to build state worker runtime");
                    return;
                }
            };

            let mut restart_count = 0_u64;

            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
                let shutdown_clone = Arc::clone(&shutdown);
                let runtime_config = config.runtime.clone();
                let flush_rx = flush_rx.clone();
                let committed_head = Arc::clone(&committed_head_for_thread);

                let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    runtime.block_on(async move {
                        if shutdown_clone.load(Ordering::Relaxed) {
                            let _ = shutdown_tx.send(());
                        }

                        run_worker_once(
                            &runtime_config,
                            Some(flush_rx),
                            committed_head,
                            shutdown_rx,
                        )
                        .await
                    })
                }));

                match result {
                    Ok(Ok(())) => {
                        info!("embedded state worker exited");
                    }
                    Ok(Err(error)) => {
                        warn!(error = %error, "embedded state worker failed; restarting");
                    }
                    Err(payload) => {
                        if let Some(message) = payload.downcast_ref::<&str>() {
                            warn!(panic = %message, "embedded state worker panicked; restarting");
                        } else if let Some(message) = payload.downcast_ref::<String>() {
                            warn!(panic = %message, "embedded state worker panicked; restarting");
                        } else {
                            warn!("embedded state worker panicked; restarting");
                        }
                    }
                }

                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                restart_count = restart_count.saturating_add(1);
                info!(
                    restart_count,
                    restart_delay_secs = config.restart_backoff.as_secs(),
                    "restarting embedded state worker"
                );
                std::thread::sleep(config.restart_backoff);
            }
        })?;

    Ok(EmbeddedStateWorkerHandle {
        commit_head: CommitHeadHandle { tx: flush_tx },
        committed_head,
        thread,
    })
}
