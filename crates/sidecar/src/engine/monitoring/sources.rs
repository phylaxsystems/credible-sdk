use crate::{
    Cache,
    cache::sources::SourceName,
    engine::EngineError,
    metrics::SourceMetrics,
};
use dashmap::DashMap;
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            AtomicUsize,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};
use tokio::{
    task::AbortHandle,
    time::sleep,
};
use tracing::{
    debug,
    error,
    trace,
};

/// Monitors multiple sources and tracks their synchronization status
///
/// Provides global and per-source metrics about sync state, with automatic
/// Prometheus metric exports. Runs a background monitoring loop that
/// periodically checks all sources.
#[derive(Debug)]
pub struct Sources {
    sources_inner: Arc<SourcesInner>,
    /// Background monitoring task abort handle
    abort_handle: AbortHandle,
}

#[derive(Debug)]
pub struct SourcesInner {
    /// How often to check source synchronization status
    period: Duration,
    /// Global flag: true if at least one source is synced
    any_source_synced: AtomicBool,
    /// Per-source metrics tracking, keyed by source name
    metrics: DashMap<SourceName, SourceMetrics>,
    /// Reference to the cache containing sources to monitor
    cache: Arc<Cache>,
    /// Current head block number
    head_block_number: AtomicU64,
}

impl Drop for Sources {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

impl Sources {
    /// Creates a new `Sources` monitoring instance
    pub fn new(cache: Arc<Cache>, period: Duration) -> Arc<Self> {
        let sources_inner = Arc::new(SourcesInner {
            metrics: cache
                .list_configured_sources()
                .into_iter()
                .map(|source| (source, SourceMetrics::default()))
                .collect(),
            cache,
            period,
            any_source_synced: AtomicBool::new(false),
            head_block_number: AtomicU64::new(0),
        });
        let abort_handle = sources_inner.clone().spawn_monitoring();

        Arc::new(Self {
            sources_inner,
            abort_handle,
        })
    }

    /// Checks if at least one source is currently synced
    pub fn has_synced_source(&self) -> bool {
        self.sources_inner.any_source_synced.load(Ordering::Acquire)
    }

    /// Updates the current head block number
    pub fn update_head_block_number(&self, block_number: u64) {
        self.sources_inner
            .head_block_number
            .store(block_number, Ordering::Relaxed);
    }
}

impl SourcesInner {
    /// Spawns the monitoring loop as a background task
    ///
    /// The task will run indefinitely, checking source synchronization
    /// at the configured period interval.
    ///
    /// # Returns
    ///
    /// An `AbortHandle` that can be used to stop the monitoring task
    fn spawn_monitoring(self: Arc<Self>) -> AbortHandle {
        tokio::task::spawn(self.run()).abort_handle()
    }

    /// Verifies that all state sources are synced, and if not stall until they are.
    ///
    /// If the sources do not become synced after a set amount of time, the function
    /// errors.
    async fn run(self: Arc<Self>) -> ! {
        let start = Instant::now();
        loop {
            let head_block_number = self.head_block_number.load(Ordering::Relaxed);
            let timestamp = Instant::now().duration_since(start).as_secs();

            let mut any_synced = false;

            // Iterate over all configured sources
            for entry in &self.metrics {
                let (source_name, source_metric) = entry.pair();

                // Get the source from cache and check its sync status
                let is_synced = self
                    .cache
                    .iter_synced_sources()
                    .find(|s| s.name() == *source_name)
                    .is_some_and(|source| source.is_synced(head_block_number)); // Default to not
                // synced if source not found

                // Update per-source metrics
                source_metric.update_sync_status(is_synced, timestamp);

                if is_synced {
                    any_synced = true;
                }
            }

            // Update global sync flag
            self.any_source_synced.store(any_synced, Ordering::Release);

            // Commit all metrics to Prometheus
            self.metrics.iter().for_each(|entry| {
                let (name, source_metric) = entry.pair();
                source_metric.commit(name);
            });

            sleep(self.period).await;
        }
    }
}
