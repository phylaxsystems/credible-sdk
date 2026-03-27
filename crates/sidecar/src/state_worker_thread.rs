//! Spawns the state worker as a dedicated OS thread inside the sidecar process.
//!
//! The state worker traces blocks from an execution client and sends
//! [`BlockStateUpdate`]s on a bounded flume channel instead of writing
//! to MDBX directly. A separate flush loop (not in this module) is
//! responsible for draining the channel into MDBX, gated by commit-head
//! signals from [`CoreEngine`](crate::engine::CoreEngine).

use crate::args::StateWorkerConfig;
use alloy::primitives::{
    B256,
    Bytes,
    U256,
};
use alloy_provider::{
    Provider,
    ProviderBuilder,
    WsConnect,
};
use mdbx::{
    AccountInfo,
    AccountState,
    AddressHash,
    BlockMetadata,
    BlockStateUpdate,
    CommitStats,
    Reader,
    StateReader,
    Writer,
};
use std::{
    collections::HashMap,
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
use tracing::{
    error,
    info,
    warn,
};

// ---------------------------------------------------------------------------
// ChannelWriter
// ---------------------------------------------------------------------------

/// Error type for [`ChannelWriter`] operations.
#[derive(Debug, thiserror::Error)]
pub enum ChannelWriterError {
    /// The receiving end of the channel was dropped.
    #[error("block state update channel disconnected")]
    ChannelDisconnected,

    /// Delegated reader error.
    #[error(transparent)]
    Reader(#[from] mdbx::common::StateError),
}

/// Adapter that implements [`Writer`] by forwarding [`BlockStateUpdate`]s
/// over a flume channel, and implements [`Reader`] by delegating to an
/// inner [`StateReader`].
///
/// This allows [`StateWorker`](state_worker::StateWorker) to be
/// generic over `Writer + Reader` while the actual MDBX writes happen
/// elsewhere (in the flush loop).
pub struct ChannelWriter {
    sender: flume::Sender<BlockStateUpdate>,
    reader: StateReader,
}

impl ChannelWriter {
    /// Create a new `ChannelWriter`.
    ///
    /// * `sender`  – bounded flume sender for block state updates.
    /// * `reader`  – [`StateReader`] used to satisfy the `Reader` trait
    ///   (e.g. `latest_block_number` on restart).
    #[must_use]
    pub fn new(sender: flume::Sender<BlockStateUpdate>, reader: StateReader) -> Self {
        Self { sender, reader }
    }
}

impl Writer for ChannelWriter {
    type Error = ChannelWriterError;

    fn commit_block(&self, update: &BlockStateUpdate) -> Result<CommitStats, Self::Error> {
        self.sender
            .send(update.clone())
            .map_err(|_| ChannelWriterError::ChannelDisconnected)?;
        Ok(CommitStats::default())
    }

    fn bootstrap_from_snapshot(
        &self,
        accounts: Vec<AccountState>,
        block_number: u64,
        block_hash: B256,
        state_root: B256,
    ) -> Result<CommitStats, Self::Error> {
        let mut update = BlockStateUpdate::new(block_number, block_hash, state_root);
        for account in accounts {
            update.merge_account_state(account);
        }
        self.sender
            .send(update)
            .map_err(|_| ChannelWriterError::ChannelDisconnected)?;
        Ok(CommitStats::default())
    }
}

impl Reader for ChannelWriter {
    type Error = ChannelWriterError;

    fn latest_block_number(&self) -> Result<Option<u64>, Self::Error> {
        self.reader.latest_block_number().map_err(Into::into)
    }

    fn is_block_available(&self, block_number: u64) -> Result<bool, Self::Error> {
        self.reader
            .is_block_available(block_number)
            .map_err(Into::into)
    }

    fn get_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        self.reader
            .get_account(address_hash, block_number)
            .map_err(Into::into)
    }

    fn get_storage(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block_number: u64,
    ) -> Result<Option<U256>, Self::Error> {
        self.reader
            .get_storage(address_hash, slot_hash, block_number)
            .map_err(Into::into)
    }

    fn get_all_storage(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> Result<HashMap<B256, U256>, Self::Error> {
        self.reader
            .get_all_storage(address_hash, block_number)
            .map_err(Into::into)
    }

    fn get_code(&self, code_hash: B256, block_number: u64) -> Result<Option<Bytes>, Self::Error> {
        self.reader
            .get_code(code_hash, block_number)
            .map_err(Into::into)
    }

    fn get_full_account(
        &self,
        address_hash: AddressHash,
        block_number: u64,
    ) -> Result<Option<AccountState>, Self::Error> {
        self.reader
            .get_full_account(address_hash, block_number)
            .map_err(Into::into)
    }

    fn get_block_hash(&self, block_number: u64) -> Result<Option<B256>, Self::Error> {
        self.reader.get_block_hash(block_number).map_err(Into::into)
    }

    fn get_state_root(&self, block_number: u64) -> Result<Option<B256>, Self::Error> {
        self.reader.get_state_root(block_number).map_err(Into::into)
    }

    fn get_block_metadata(&self, block_number: u64) -> Result<Option<BlockMetadata>, Self::Error> {
        self.reader
            .get_block_metadata(block_number)
            .map_err(Into::into)
    }

    fn get_available_block_range(&self) -> Result<Option<(u64, u64)>, Self::Error> {
        self.reader.get_available_block_range().map_err(Into::into)
    }

    fn scan_account_hashes(&self, block_number: u64) -> Result<Vec<AddressHash>, Self::Error> {
        self.reader
            .scan_account_hashes(block_number)
            .map_err(Into::into)
    }
}

// ---------------------------------------------------------------------------
// Spawn logic
// ---------------------------------------------------------------------------

/// Spawn the state worker on a dedicated OS thread named
/// `sidecar-state-worker`.
///
/// The thread creates its own single-threaded tokio runtime, connects to the
/// execution client over WebSocket, and runs
/// [`StateWorker::run`](state_worker::StateWorker::run) in a panic-catching
/// restart loop with exponential backoff.
///
/// # Arguments
///
/// * `config`   – connection and tuning parameters.
/// * `sender`   – bounded channel for outgoing [`BlockStateUpdate`]s.
/// * `reader`   – [`StateReader`] used by [`ChannelWriter`] to answer
///   `latest_block_number` queries on restart.
/// * `shutdown` – shared flag; when set to `true` the worker exits its loop.
///
/// # Errors
///
/// Returns `std::io::Error` if the OS thread cannot be spawned.
pub fn spawn_state_worker(
    config: StateWorkerConfig,
    sender: flume::Sender<BlockStateUpdate>,
    reader: StateReader,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("sidecar-state-worker".into())
        .spawn(move || {
            run_state_worker_loop(&config, &sender, &reader, &shutdown);
        })
}

/// The inner restart loop executed on the dedicated OS thread.
///
/// Each iteration:
/// 1. Checks the shutdown flag.
/// 2. Wraps the async worker run in [`std::panic::catch_unwind`].
/// 3. On failure or panic, applies exponential backoff before retrying.
/// 4. Resets backoff on the first successful block commit (currently
///    approximated by a successful return from `run`).
fn run_state_worker_loop(
    config: &StateWorkerConfig,
    sender: &flume::Sender<BlockStateUpdate>,
    reader: &StateReader,
    shutdown: &Arc<AtomicBool>,
) {
    let mut backoff_ms = config.restart_backoff_base_ms;

    loop {
        if shutdown.load(Ordering::Acquire) {
            info!("state worker shutdown flag set; exiting thread");
            return;
        }

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_state_worker_once(config, sender.clone(), reader.clone(), Arc::clone(shutdown))
        }));

        if shutdown.load(Ordering::Acquire) {
            info!("state worker shutdown flag set after run; exiting thread");
            return;
        }

        match result {
            Ok(Ok(())) => {
                // Clean exit (e.g. shutdown signal inside the worker).
                // Reset backoff since the run completed normally.
                backoff_ms = config.restart_backoff_base_ms;
                info!("state worker exited normally; restarting");
            }
            Ok(Err(err)) => {
                warn!(error = %err, backoff_ms, "state worker failed; retrying after backoff");
            }
            Err(panic_payload) => {
                let msg = panic_message(&*panic_payload);
                error!(panic = %msg, backoff_ms, "state worker panicked; retrying after backoff");
            }
        }

        // Sleep for the backoff duration, checking shutdown periodically.
        let sleep_until = std::time::Instant::now() + Duration::from_millis(backoff_ms);
        while std::time::Instant::now() < sleep_until {
            if shutdown.load(Ordering::Acquire) {
                info!("state worker shutdown during backoff; exiting thread");
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        // Double the backoff, capped at the configured maximum.
        backoff_ms = backoff_ms
            .saturating_mul(2)
            .min(config.restart_backoff_max_ms);
    }
}

/// Execute a single run of the state worker inside a fresh single-threaded
/// tokio runtime.
fn run_state_worker_once(
    config: &StateWorkerConfig,
    sender: flume::Sender<BlockStateUpdate>,
    reader: StateReader,
    shutdown: Arc<AtomicBool>,
) -> Result<(), anyhow::Error> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(async {
        // Connect WebSocket provider.
        let ws = WsConnect::new(&config.ws_url);
        let provider = ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .map_err(|e| anyhow::anyhow!("failed to connect to ws provider: {e}"))?;

        let provider = Arc::new(provider.root().clone());

        // Build trace provider.
        let trace_timeout = Duration::from_millis(config.trace_timeout_ms);
        let trace_provider = state_worker::create_trace_provider(provider.clone(), trace_timeout);

        // Parse genesis state.
        let genesis_state = if config.genesis_path.is_empty() {
            None
        } else {
            let contents = std::fs::read_to_string(&config.genesis_path)
                .map_err(|e| anyhow::anyhow!("failed to read genesis file: {e}"))?;
            let gs = state_worker::parse_genesis_from_str(&contents)
                .map_err(|e| anyhow::anyhow!("failed to parse genesis file: {e}"))?;
            Some(gs)
        };

        // Build system calls from genesis config.
        let system_calls = match &genesis_state {
            Some(gs) => {
                state_worker::SystemCalls::new(gs.config().cancun_time, gs.config().prague_time)
            }
            None => state_worker::SystemCalls::default(),
        };

        // Build the channel-backed writer.
        let channel_writer = ChannelWriter::new(sender, reader);

        let mut worker = state_worker::StateWorker::new(
            provider,
            trace_provider,
            channel_writer,
            genesis_state,
            system_calls,
        );

        // Create a broadcast channel for the internal shutdown signal.
        // The state worker's `run` method expects a `broadcast::Receiver<()>`.
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        // Spawn a task that bridges the `Arc<AtomicBool>` shutdown flag to
        // the broadcast channel expected by the state worker.
        tokio::spawn(async move {
            loop {
                if shutdown.load(Ordering::Acquire) {
                    let _ = shutdown_tx.send(());
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        worker.run(None, shutdown_rx).await
    })
}

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_message_extracts_str() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("boom");
        assert_eq!(panic_message(&*payload), "boom");
    }

    #[test]
    fn panic_message_extracts_string() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("kaboom"));
        assert_eq!(panic_message(&*payload), "kaboom");
    }

    #[test]
    fn panic_message_handles_unknown() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(42_i32);
        assert_eq!(panic_message(&*payload), "unknown panic");
    }
}
