//! Periodic monitoring of Assertion DA reachability.

use alloy::primitives::B256;
use assertion_da_client::{
    DaClient,
    DaClientError,
};
use metrics::{
    counter,
    gauge,
    histogram,
};
use std::{
    future::Future,
    time::{
        Duration,
        Instant,
    },
};
use tokio::time::MissedTickBehavior;
use tracing::{
    debug,
    info,
    warn,
};

/// Interval between Assertion DA reachability checks.
pub const DA_REACHABILITY_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum time to wait for a single reachability probe before treating the
/// endpoint as unreachable.
const DA_REACHABILITY_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Probe a sentinel assertion id that is expected to be absent.
///
/// We treat JSON-RPC errors as "reachable" because they prove the DA endpoint
/// answered a well-formed request.
const REACHABILITY_PROBE_ASSERTION_ID: B256 = B256::ZERO;

/// Run a background loop that periodically checks Assertion DA reachability and
/// emits Prometheus metrics.
pub async fn run_da_reachability_monitor(assertion_da_rpc_url: String) {
    let da_client = match DaClient::new(&assertion_da_rpc_url) {
        Ok(client) => client,
        Err(err) => {
            warn!(
                error = ?err,
                assertion_da_rpc_url = %assertion_da_rpc_url,
                "Failed to initialize Assertion DA reachability monitor"
            );
            gauge!("sidecar_assertion_da_reachable").set(0.0);
            return;
        }
    };

    let mut ticker = tokio::time::interval(DA_REACHABILITY_CHECK_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_reachable: Option<bool> = None;

    loop {
        ticker.tick().await;

        let started_at = Instant::now();
        let check_result = probe_da_reachability(&da_client).await;
        let check_duration = started_at.elapsed();

        histogram!("sidecar_assertion_da_reachability_check_duration_seconds")
            .record(check_duration);

        let is_reachable = check_result.is_ok();
        gauge!("sidecar_assertion_da_reachable").set(if is_reachable { 1.0 } else { 0.0 });

        let status = if is_reachable { "success" } else { "failure" };
        counter!("sidecar_assertion_da_reachability_checks_total", "status" => status).increment(1);

        if last_reachable != Some(is_reachable) {
            match (last_reachable, is_reachable, &check_result) {
                (Some(false), true, _) => {
                    info!(
                        assertion_da_rpc_url = %assertion_da_rpc_url,
                        "Assertion DA reachability recovered"
                    );
                }
                (_, false, Err(err)) => {
                    warn!(
                        error = ?err,
                        assertion_da_rpc_url = %assertion_da_rpc_url,
                        "Assertion DA is unreachable"
                    );
                }
                _ => {}
            }
        } else if let Err(err) = &check_result {
            debug!(
                error = ?err,
                assertion_da_rpc_url = %assertion_da_rpc_url,
                "Assertion DA reachability probe failed"
            );
        }

        last_reachable = Some(is_reachable);
    }
}

async fn probe_da_reachability(da_client: &DaClient) -> Result<(), DaClientError> {
    let result = wait_for_probe_with_timeout(
        DA_REACHABILITY_PROBE_TIMEOUT,
        da_client.fetch_assertion(REACHABILITY_PROBE_ASSERTION_ID),
    )
    .await;

    match result {
        Ok(_) => Ok(()),
        Err(err) if is_reachable_da_error(&err) => Ok(()),
        Err(err) => Err(err),
    }
}

async fn wait_for_probe_with_timeout<T, F>(
    probe_timeout: Duration,
    probe_future: F,
) -> Result<T, DaClientError>
where
    F: Future<Output = Result<T, DaClientError>>,
{
    match tokio::time::timeout(probe_timeout, probe_future).await {
        Ok(result) => result,
        Err(_elapsed) => Err(DaClientError::InvalidResponse("probe timed out".into())),
    }
}

fn is_reachable_da_error(error: &DaClientError) -> bool {
    matches!(error, DaClientError::JsonRpcError { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::{
        MetricKind,
        debugging::{
            DebugValue,
            DebuggingRecorder,
        },
    };

    #[test]
    fn json_rpc_errors_are_considered_reachable() {
        let error = DaClientError::JsonRpcError {
            code: -32001,
            message: "Assertion not found".to_string(),
        };
        assert!(is_reachable_da_error(&error));
    }

    #[test]
    fn non_json_rpc_errors_are_unreachable() {
        let error = DaClientError::InvalidResponse("HTTP error: 503".to_string());
        assert!(!is_reachable_da_error(&error));
    }

    #[tokio::test]
    async fn probe_timeout_returns_invalid_response_error() {
        let result = wait_for_probe_with_timeout(
            Duration::from_millis(10),
            std::future::pending::<Result<(), DaClientError>>(),
        )
        .await;

        assert!(
            matches!(result, Err(DaClientError::InvalidResponse(message)) if message == "probe timed out")
        );
    }

    #[test]
    fn monitor_sets_reachability_gauge_to_zero_when_client_init_fails() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("create tokio runtime");
            runtime.block_on(run_da_reachability_monitor("not a valid URL".to_string()));
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let (_, _, _, value) = snapshot
            .into_iter()
            .find(|(composite_key, _, _, _)| {
                composite_key.kind() == MetricKind::Gauge
                    && composite_key.key().name() == "sidecar_assertion_da_reachable"
            })
            .expect("reachability gauge should be emitted");

        assert_eq!(value, DebugValue::Gauge(0.0.into()));
    }
}
