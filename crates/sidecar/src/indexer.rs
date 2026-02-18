//! # `indexer`
//!
//! Contains setup functions and types to initialize the Shovel PG consumer
//! for assertion events.

use crate::{
    critical,
    utils::ErrorRecoverability,
};
use assertion_executor::store::{
    ShovelConsumer,
    ShovelConsumerCfg,
    ShovelConsumerError,
};
use tokio::time::{
    Duration,
    Instant,
    sleep,
};

/// Runs the Shovel PG consumer to monitor assertion events from Postgres.
///
/// Shovel (external process) indexes blockchain events from the State Oracle
/// contract into a PostgreSQL table. This function reads those events and
/// updates the local assertion store.
///
/// The consumer syncs existing events before entering a polling loop.
pub async fn run_shovel_consumer(cfg: ShovelConsumerCfg) -> Result<(), ShovelConsumerError> {
    const RESTART_WINDOW: Duration = Duration::from_secs(30);
    const MAX_RESTARTS_IN_WINDOW: u32 = 15;
    const BACKOFF_STEP_MS: u64 = 500;

    let mut restart_window_start: Option<Instant> = None;
    let mut restart_count = 0u32;

    loop {
        let result = async {
            let mut consumer = ShovelConsumer::connect(cfg.clone()).await?;
            consumer.sync().await?;
            consumer.run().await
        }
        .await;

        match result {
            Ok(()) => break Ok(()),
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
                                "shovel consumer transient {} error, restarting after backoff",
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
                                "shovel consumer restarted too frequently; escalating {} error",
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

fn transient_error_details(err: &ShovelConsumerError) -> Option<(&'static str, &dyn std::fmt::Debug)> {
    match err {
        ShovelConsumerError::PostgresError(e) => Some(("postgres", e)),
        _ => None,
    }
}

impl From<&ShovelConsumerError> for ErrorRecoverability {
    fn from(e: &ShovelConsumerError) -> Self {
        match e {
            ShovelConsumerError::PostgresError(_)
            | ShovelConsumerError::DaClientError(_)
            | ShovelConsumerError::AssertionStoreError(_) => ErrorRecoverability::Unrecoverable,
            ShovelConsumerError::FnSelectorExtractorError(_)
            | ShovelConsumerError::BlockNumberExceedsU64
            | ShovelConsumerError::InvalidRow(_) => ErrorRecoverability::Recoverable,
        }
    }
}
