//! # `indexer`
//!
//! Contains setup functions and types to initialize the assertion-executor indexer.

use crate::{
    critical,
    utils::ErrorRecoverability,
};
use assertion_executor::store::{
    Indexer,
    IndexerCfg,
    IndexerError,
};
use tokio::time::{
    Duration,
    Instant,
    sleep,
};

/// Runs the indexer to monitor blockchain events from the Credible Layer state oracle contract
///
/// This function initializes and runs the assertion indexer, which continuously monitors
/// the configured blockchain for events emitted by the state oracle contract. When assertion
/// events are detected, it retrieves the corresponding assertion data from the DA layer
/// and updates the local assertion store to keep the sidecar's validation logic current.
///
/// The indexer syncs to the current blockchain head before entering continuous monitoring mode.
pub async fn run_indexer(indexer_cfg: IndexerCfg) -> Result<(), IndexerError> {
    const RESTART_WINDOW: Duration = Duration::from_secs(5);
    const MAX_RESTARTS_IN_WINDOW: u32 = 5;
    const BACKOFF_STEP_MS: u64 = 200;

    let mut restart_window_start: Option<Instant> = None;
    let mut restart_count = 0u32;

    // First, we create an indexer and sync it to the head.
    loop {
        let indexer = Box::pin(Indexer::new_synced(indexer_cfg.clone())).await?;
        // We can then run the indexer and update the assertion store as new
        // assertions come in
        match indexer.run().await {
            Ok(()) => break Ok(()),
            // `BlockStreamError`/`TransportError` happens when the indexer WS subscriptions
            // close which can happen occasionally due to WS flakiness,
            // we dont need to raise this error further and can try to recover internally.
            //
            // If that fails, we escalte this if we fail to restart 5 times in a row in 5 secs.
            Err(IndexerError::BlockStreamError(e)) /*| Err(IndexerError::TransportError(e))*/ => {
                let now = Instant::now();
                let window_start = restart_window_start.unwrap_or(now);

                if restart_window_start.is_none()
                    || now.duration_since(window_start) > RESTART_WINDOW
                {
                    restart_window_start = Some(now);
                    restart_count = 0;
                }

                restart_count += 1;

                if restart_count > MAX_RESTARTS_IN_WINDOW {
                    critical!(
                        error = ?e,
                        failures_in_window = restart_count,
                        restarts_in_window = restart_count.saturating_sub(1),
                        "indexer restarted too frequently; escalating block stream error",
                    );
                    break Err(IndexerError::BlockStreamError(e));
                }

                let backoff = Duration::from_millis(BACKOFF_STEP_MS * restart_count as u64);

                tracing::warn!(
                    error = ?e,
                    failures_in_window = restart_count,
                    backoff_ms = backoff.as_millis(),
                    "indexer closed channel, restarting after backoff",
                );
                sleep(backoff).await;
                continue;
            }
            Err(IndexerError::TransportError(e)) => {
                critical!(error = ?e, "indexer transport error");
                break Err(IndexerError::TransportError(e));
            }
            Err(e) => break Err(e),
        }
    }
}

impl From<&IndexerError> for ErrorRecoverability {
    fn from(e: &IndexerError) -> Self {
        match e {
            IndexerError::TransportError(_)
            | IndexerError::SledError(_)
            | IndexerError::DaClientError(_)
            | IndexerError::AssertionStoreError(_) => ErrorRecoverability::Unrecoverable,
            IndexerError::BincodeError(_)
            | IndexerError::EventDecodeError(_)
            | IndexerError::BlockNumberMissing
            | IndexerError::LogIndexMissing
            | IndexerError::BlockNumberExceedsU64
            | IndexerError::BlockStreamError(_)
            | IndexerError::DaBytecodeDecodingFailed(_)
            | IndexerError::ParentBlockNotFound
            | IndexerError::BlockHashMissing
            | IndexerError::NoCommonAncestor
            | IndexerError::ExecutionLogsRxNone
            | IndexerError::CheckIfReorgedError(_)
            | IndexerError::StoreNotSynced => ErrorRecoverability::Recoverable,
        }
    }
}
