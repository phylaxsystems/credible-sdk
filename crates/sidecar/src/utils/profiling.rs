//! Utilities for instrumenting asynchronous workloads with Tokio Console and metrics.
//!
//! This module is only activated when the `tokio-console` feature is enabled. When disabled,
//! the helper functions turn into no-ops so call sites do not need additional `cfg` guards.

#[cfg(feature = "tokio-console")]
use {
    console_subscriber::ConsoleLayer,
    std::io::IsTerminal,
    tokio::{
        runtime::Handle,
        task::JoinHandle,
    },
    tracing_subscriber::{
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
};

#[cfg(all(feature = "tokio-console", tokio_unstable))]
use {
    std::time::Duration,
    tokio_metrics::RuntimeMonitor,
};

/// Guard returned by [`init_profiling`] that keeps background instrumentation tasks alive for the
/// lifetime of the benchmark / test run.
#[cfg(feature = "tokio-console")]
#[must_use]
pub struct ProfilingGuard {
    metrics_handle: Option<JoinHandle<()>>,
}

#[cfg(feature = "tokio-console")]
impl Drop for ProfilingGuard {
    fn drop(&mut self) {
        if let Some(handle) = &self.metrics_handle {
            handle.abort();
        }
    }
}

/// No-op guard used when the `tokio-console` feature is disabled.
#[cfg(not(feature = "tokio-console"))]
#[derive(Debug)]
pub struct ProfilingGuard;

/// Initialize Tokio Console + tokio-metrics collection.
///
/// When the `tokio-console` feature is enabled this sets up:
///   * A `ConsoleLayer` so the process can be inspected with `tokio-console`
///   * A background task that logs periodic `tokio-metrics::RuntimeInterval` snapshots
///
/// The function returns a guard that must be held for as long as instrumentation is required.
/// Dropping the guard aborts the background tasks.
#[cfg(feature = "tokio-console")]
pub fn init_profiling(runtime: &Handle) -> Result<ProfilingGuard, String> {
    let console_layer = ConsoleLayer::builder().with_default_env().spawn();

      let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive(
            "alloy_rpc_client=warn"
                .parse()
                .map_err(|e| format!("invalid directive: {e}"))?,
        )
        .add_directive(
            "alloy_transport=warn"
                .parse()
                .map_err(|e| format!("invalid directive: {e}"))?,
        );

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(IsTerminal::is_terminal(&std::io::stderr()))
        .with_writer(std::io::stderr);

    tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(fmt_layer)
        .try_init()
        .map_err(|e| format!("failed to initialize tracing subscriber: {e}"))?;

    let metrics_handle = {
        #[cfg(tokio_unstable)]
        {
            let runtime_handle = runtime.clone();
            Some(runtime.spawn(async move {
                let monitor = RuntimeMonitor::new(&runtime_handle);
                for interval in monitor.intervals() {
                    tracing::info!(
                        target: "tokio::metrics",
                        metrics = ?interval,
                        "runtime metrics snapshot"
                    );
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }
            }))
        }
        #[cfg(not(tokio_unstable))]
        {
            tracing::warn!(
                target: "tokio::metrics",
                "Tokio runtime metrics require building with `--cfg tokio_unstable`; metrics streaming disabled"
            );
            let _ = runtime;
            None
        }
    };

    Ok(ProfilingGuard { metrics_handle })
}

/// No-op placeholder when the `tokio-console` feature is disabled.
#[cfg(not(feature = "tokio-console"))]
pub fn init_profiling(_runtime: &tokio::runtime::Handle) -> Result<ProfilingGuard, String> {
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive(
            "alloy_rpc_client=warn"
                .parse()
                .map_err(|e| format!("invalid directive: {e}"))?,
        )
        .add_directive(
            "alloy_transport=warn"
                .parse()
                .map_err(|e| format!("invalid directive: {e}"))?,
        );

    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| format!("failed to initialize tracing subscriber: {e}"))?;
    Ok(ProfilingGuard)
}
