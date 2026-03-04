mod assertion_bootstrap;
pub mod notifier;
mod runtime;

use crate::{
    config::Config,
    server::models::replay::ReplayRequest,
};
use alloy::{
    primitives::B256,
    transports::{
        RpcError,
        TransportErrorKind,
    },
};
use alloy_provider::{
    Provider,
    RootProvider,
};
use assertion_bootstrap::{
    BootstrapError,
    bootstrap_assertion_store,
};
use runtime::{
    ReplayRuntime,
    ReplayStopMatch,
    RuntimeError,
    query_head_block,
};
use std::{
    sync::atomic::{
        AtomicU64,
        Ordering,
    },
    time::{
        Duration,
        Instant,
    },
};
use thiserror::Error;
use tracing::{
    debug,
    info,
};

#[derive(Debug, Clone)]
pub struct ReplayExecutionSummary {
    pub start_block: u64,
    pub head_block: u64,
    pub replay_window_before: u64,
    pub replay_window_after: u64,
    pub elapsed_millis: u128,
    pub watched_assertion_ids: Vec<B256>,
    pub matched_assertion_id: Option<B256>,
    pub matched_block_number: Option<u64>,
    pub matched_tx_hash: Option<B256>,
    pub matched_incident_payload: Option<sidecar::transaction_observer::DappIncidentPayload>,
}

/// Runs one replay pass from `current_head - replay_window` to current head.
///
/// # Errors
///
/// Returns [`ReplayError`] if querying head state, bootstrapping assertions,
/// runtime initialization, block processing, or runtime shutdown fails.
pub(crate) async fn run_replay(
    config: &Config,
    replay_window: &AtomicU64,
    tuning: ReplayDurationTuning,
    request: &ReplayRequest,
) -> Result<ReplayExecutionSummary, ReplayError> {
    let head = query_head_block(&config.archive_ws_url)
        .await
        .map_err(ReplayError::HeadBlockQuery)?;
    let current_window = replay_window.load(Ordering::Relaxed).max(1);
    let start_block = ReplayDurationTuning::compute_start_block(head, current_window);
    let executor_config = assertion_executor::ExecutorConfig::default()
        .with_chain_id(config.chain_id)
        .with_assertion_gas_limit(config.assertion_gas_limit);

    let (assertion_store, watched_assertion_ids) =
        bootstrap_assertion_store(request, start_block, &executor_config).await?;

    let runtime = ReplayRuntime::new(config, !watched_assertion_ids.is_empty(), assertion_store)
        .await
        .map_err(|source| ReplayError::RuntimeInitialization { source })?;

    runtime
        .wait_for_source_sync(start_block)
        .await
        .map_err(|source| ReplayError::StateSourceSync { source })?;

    let started_at = Instant::now();
    runtime
        .send_initial_commit(start_block)
        .await
        .map_err(|source| ReplayError::InitialCommit { source })?;
    let stop_match = runtime
        .process_block_range(start_block, head, &watched_assertion_ids)
        .await
        .map_err(|source| ReplayError::BlockRangeProcessing { source })?;
    runtime
        .shutdown()
        .await
        .map_err(|source| ReplayError::RuntimeShutdown { source })?;

    let elapsed = started_at.elapsed();
    let next_window = tuning.adjust_replay_window(current_window, elapsed);
    replay_window.store(next_window, Ordering::Relaxed);

    log_replay_outcome(stop_match.as_ref(), watched_assertion_ids.len());
    log_window_adjustment(current_window, next_window, elapsed, tuning);
    let matched_assertion_id = stop_match.as_ref().map(|candidate| candidate.assertion_id);
    let matched_block_number = stop_match.as_ref().map(|candidate| candidate.block_number);
    let matched_tx_hash = stop_match.as_ref().map(|candidate| candidate.tx_hash);
    let matched_incident_payload = stop_match.map(|candidate| candidate.incident_payload);
    let mut watched_assertion_ids: Vec<B256> = watched_assertion_ids.iter().copied().collect();
    watched_assertion_ids.sort_unstable();

    Ok(ReplayExecutionSummary {
        start_block,
        head_block: head,
        replay_window_before: current_window,
        replay_window_after: next_window,
        elapsed_millis: elapsed.as_millis(),
        watched_assertion_ids,
        matched_assertion_id,
        matched_block_number,
        matched_tx_hash,
        matched_incident_payload,
    })
}

#[derive(Debug, Clone, Copy)]
pub struct ReplayStartBlockPreview {
    pub start_block: u64,
    pub head_block: u64,
    pub replay_window: u64,
}

/// Computes the replay start block preview using the current head and replay window.
///
/// # Errors
///
/// Returns [`ReplayError`] if querying the current head block from the shared
/// provider fails.
pub(crate) async fn preview_replay_start_block(
    head_provider: &RootProvider,
    replay_window: &AtomicU64,
) -> Result<ReplayStartBlockPreview, ReplayError> {
    let head = head_provider
        .get_block_number()
        .await
        .map_err(ReplayError::HeadBlockQueryProvider)?;
    let window = replay_window.load(Ordering::Relaxed).max(1);
    let start_block = ReplayDurationTuning::compute_start_block(head, window);

    Ok(ReplayStartBlockPreview {
        start_block,
        head_block: head,
        replay_window: window,
    })
}

fn log_replay_outcome(stop_match: Option<&ReplayStopMatch>, assertions_count: usize) {
    if let Some(stop_match) = stop_match {
        info!(
            assertions_count,
            matched_assertion_id = %stop_match.assertion_id,
            matched_tx_hash = %stop_match.tx_hash,
            matched_block_number = stop_match.block_number,
            "replay stopped early after observing a watched assertion id"
        );
    } else {
        debug!(
            assertions_count,
            "replay run completed without matching a watched assertion id"
        );
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplayDurationTuning {
    min: Duration,
    target: Duration,
    max: Duration,
}

impl ReplayDurationTuning {
    #[must_use]
    pub fn from_config(config: &Config) -> Self {
        Self {
            min: Duration::from_secs_f64((config.replay_duration_min_minutes * 60.0).max(0.1)),
            target: Duration::from_secs_f64(
                (config.replay_duration_target_minutes * 60.0).max(0.1),
            ),
            max: Duration::from_secs_f64((config.replay_duration_max_minutes * 60.0).max(0.1)),
        }
    }

    fn adjust_replay_window(self, current_window: u64, elapsed: Duration) -> u64 {
        if !(self.min < self.target && self.target < self.max) {
            return current_window.max(1);
        }

        if elapsed >= self.min && elapsed <= self.max {
            return current_window.max(1);
        }

        let target_nanos = self.target.as_nanos();
        if target_nanos == 0 {
            return current_window.max(1);
        }
        // `Instant` resolution can produce `0ns` for very fast runs; treat it as
        // the fastest non-zero duration so the window still scales upward.
        let elapsed_nanos = elapsed.as_nanos().max(1);

        let numerator = u128::from(current_window).saturating_mul(target_nanos);
        let scaled = Self::rounded_div(numerator, elapsed_nanos).max(1);

        u64::try_from(scaled).unwrap_or(u64::MAX)
    }

    fn rounded_div(numerator: u128, denominator: u128) -> u128 {
        if denominator == 0 {
            return u128::MAX;
        }

        numerator
            .saturating_add(denominator / 2)
            .saturating_div(denominator)
    }

    const fn compute_start_block(head_block: u64, replay_window: u64) -> u64 {
        head_block.saturating_sub(replay_window)
    }
}

fn log_window_adjustment(
    current_window: u64,
    next_window: u64,
    elapsed: Duration,
    tuning: ReplayDurationTuning,
) {
    let elapsed_secs = elapsed.as_secs_f64();
    if next_window == current_window {
        debug!(
            current_window,
            elapsed_secs,
            min_secs = tuning.min.as_secs_f64(),
            max_secs = tuning.max.as_secs_f64(),
            "replay duration within target range; keeping replay window unchanged"
        );
        return;
    }

    info!(
        current_window,
        next_window,
        elapsed_secs,
        target_secs = tuning.target.as_secs_f64(),
        "adjusted replay window based on replay duration"
    );
}

#[derive(Debug, Error)]
pub(crate) enum ReplayError {
    #[error("failed to initialize replay runtime")]
    RuntimeInitialization {
        #[source]
        source: RuntimeError,
    },
    #[error("failed to query current head block")]
    HeadBlockQuery(#[source] RuntimeError),
    #[error("failed to bootstrap assertion store before replay")]
    AssertionBootstrap(#[from] BootstrapError),
    #[error("failed to query current head block from shared provider")]
    HeadBlockQueryProvider(#[source] RpcError<TransportErrorKind>),
    #[error("failed to wait for source sync at replay start block")]
    StateSourceSync {
        #[source]
        source: RuntimeError,
    },
    #[error("failed to send initial commit to sidecar engine")]
    InitialCommit {
        #[source]
        source: RuntimeError,
    },
    #[error("failed to process requested block range")]
    BlockRangeProcessing {
        #[source]
        source: RuntimeError,
    },
    #[error("failed to shutdown replay runtime")]
    RuntimeShutdown {
        #[source]
        source: RuntimeError,
    },
}

#[cfg(test)]
mod tests {
    use super::ReplayDurationTuning;
    use std::time::Duration;

    #[test]
    fn replay_window_decreases_when_replay_is_too_slow() {
        let tuning = ReplayDurationTuning {
            min: Duration::from_secs(600),
            target: Duration::from_secs(750),
            max: Duration::from_secs(900),
        };

        let next = tuning.adjust_replay_window(1_000, Duration::from_secs(1_200));
        assert_eq!(next, 625);
    }

    #[test]
    fn replay_window_increases_when_replay_is_too_fast() {
        let tuning = ReplayDurationTuning {
            min: Duration::from_secs(600),
            target: Duration::from_secs(750),
            max: Duration::from_secs(900),
        };

        let next = tuning.adjust_replay_window(1_000, Duration::from_secs(500));
        assert_eq!(next, 1_500);
    }

    #[test]
    fn replay_window_increases_when_replay_elapsed_is_zero() {
        let tuning = ReplayDurationTuning {
            min: Duration::from_secs(600),
            target: Duration::from_secs(750),
            max: Duration::from_secs(900),
        };

        let next = tuning.adjust_replay_window(1, Duration::ZERO);
        assert_eq!(next, 750_000_000_000);
    }

    #[test]
    fn replay_window_stays_when_replay_is_in_range() {
        let tuning = ReplayDurationTuning {
            min: Duration::from_secs(600),
            target: Duration::from_secs(750),
            max: Duration::from_secs(900),
        };

        let next = tuning.adjust_replay_window(1_000, Duration::from_secs(750));
        assert_eq!(next, 1_000);
    }

    #[test]
    fn replay_window_stays_when_tuning_is_invalid() {
        let tuning = ReplayDurationTuning {
            min: Duration::from_secs(900),
            target: Duration::from_secs(750),
            max: Duration::from_secs(600),
        };

        let next = tuning.adjust_replay_window(1_000, Duration::from_secs(1_200));
        assert_eq!(next, 1_000);
    }

    #[test]
    fn rounded_div_rounds_half_up() {
        assert_eq!(ReplayDurationTuning::rounded_div(10, 4), 3);
        assert_eq!(ReplayDurationTuning::rounded_div(9, 4), 2);
    }

    #[test]
    fn start_block_preview_uses_saturating_sub() {
        assert_eq!(ReplayDurationTuning::compute_start_block(100, 30), 70);
        assert_eq!(ReplayDurationTuning::compute_start_block(10, 30), 0);
    }
}
