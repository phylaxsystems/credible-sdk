use clap::Parser;
use credible_utils::critical;
use futures_util::FutureExt;
use state_worker::{
    cli::Args,
    error::Result,
    service::{
        WorkerRuntimeConfig,
        run_worker_once,
        spawn_signal_listener,
    },
};
use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::AtomicU64,
    },
    time::Duration,
};
use tokio::sync::broadcast;
use tracing::{
    info,
    warn,
};

#[tokio::main]
async fn main() -> Result<()> {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        warn!("Failed to install rustls crypto provider; continuing without default provider");
    }

    let _guard = rust_tracing::trace();

    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(err) => {
            critical!(error = %err, "Failed to parse CLI args; waiting for restart");
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };

    let runtime_config = WorkerRuntimeConfig {
        ws_url: args.ws_url,
        mdbx_path: args.mdbx_path,
        start_block: args.start_block,
        state_depth: args.state_depth,
        genesis_file_path: args.file_to_genesis,
        max_buffered_blocks: 1,
    };

    let committed_head = Arc::new(AtomicU64::new(0));
    let mut restart_count: u64 = 0;
    loop {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let signal_handle = spawn_signal_listener(shutdown_tx);

        let result = AssertUnwindSafe(run_worker_once(
            &runtime_config,
            None,
            Arc::clone(&committed_head),
            shutdown_rx,
        ))
        .catch_unwind()
        .await;

        signal_handle.abort();

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
