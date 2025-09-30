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
            // If that fails, we escalate this if we fail to restart 5 times in a row in 5 secs.
            Err(err) => {
                if let Some((kind, log_error)) = transient_error_details(&err) {
                    let decision = next_restart_action(
                        &mut restart_window_start,
                        &mut restart_count,
                        Instant::now(),
                        RESTART_WINDOW,
                        MAX_RESTARTS_IN_WINDOW,
                        BACKOFF_STEP_MS,
                    );

                    match decision {
                        RestartAction::Continue {
                            backoff,
                            failures_in_window,
                        } => {
                            tracing::warn!(
                                error = ?log_error,
                                failures_in_window,
                                backoff_ms = backoff.as_millis(),
                                "indexer transient {} error, restarting after backoff",
                                kind,
                            );
                            sleep(backoff).await;
                            continue;
                        }
                        RestartAction::Escalate { failures_in_window } => {
                            critical!(
                                error = ?log_error,
                                failures_in_window,
                                restarts_in_window = failures_in_window.saturating_sub(1),
                                "indexer restarted too frequently; escalating {} error",
                                kind,
                            );
                            break Err(err);
                        }
                    }
                }

                break Err(err);
            }
        }
    }
}

enum RestartAction {
    Continue {
        backoff: Duration,
        failures_in_window: u32,
    },
    Escalate {
        failures_in_window: u32,
    },
}

fn next_restart_action(
    restart_window_start: &mut Option<Instant>,
    restart_count: &mut u32,
    now: Instant,
    restart_window: Duration,
    max_restarts_in_window: u32,
    backoff_step_ms: u64,
) -> RestartAction {
    let should_reset_window = match restart_window_start {
        Some(start) => now.duration_since(*start) > restart_window,
        None => true,
    };

    if should_reset_window {
        *restart_window_start = Some(now);
        *restart_count = 0;
    }

    *restart_count += 1;

    if *restart_count > max_restarts_in_window {
        RestartAction::Escalate {
            failures_in_window: *restart_count,
        }
    } else {
        let backoff = Duration::from_millis(backoff_step_ms * u64::from(*restart_count));
        RestartAction::Continue {
            backoff,
            failures_in_window: *restart_count,
        }
    }
}

fn transient_error_details(err: &IndexerError) -> Option<(&'static str, &dyn std::fmt::Debug)> {
    match err {
        IndexerError::BlockStreamError(e) => Some(("block stream", e)),
        IndexerError::TransportError(e) => Some(("transport", e)),
        _ => None,
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
