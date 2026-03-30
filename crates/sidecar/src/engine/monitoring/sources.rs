use crate::{
    Sources as CacheSources,
    cache::sources::SourceName,
    health::{
        HealthState,
        ReadinessSnapshot,
        SourceReadinessSnapshot,
        WorkerReadiness,
    },
    metrics::{
        RuntimeHealthMetrics,
        SourceMetrics,
        StateWorkerRuntimeSnapshot,
        state_worker_runtime_state,
    },
};
use alloy::primitives::U256;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
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
    error,
    info,
    warn,
};

/// Monitors multiple sources and tracks their synchronization status.
///
/// Provides global and per-source metrics about sync state, updates health
/// readiness state, and logs degraded/fallback transitions for operators.
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
    /// Reference to the underlying state sources
    cache_sources: Arc<CacheSources>,
    /// Current head block number
    head_block_number: RwLock<U256>,
    /// Shared readiness state consumed by the health server
    health_state: Arc<HealthState>,
    /// Last published readiness snapshot
    latest_snapshot: RwLock<ReadinessSnapshot>,
    /// Runtime metrics emitted for degraded mode and state-worker continuity
    runtime_metrics: RuntimeHealthMetrics,
}

impl Drop for Sources {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

impl Sources {
    /// Creates a new `Sources` monitoring instance
    pub fn new(
        cache_sources: Arc<CacheSources>,
        period: Duration,
        health_state: Arc<HealthState>,
    ) -> Arc<Self> {
        let sources_inner = Arc::new(SourcesInner {
            metrics: cache_sources
                .list_configured_sources()
                .into_iter()
                .map(|source| (source, SourceMetrics::default()))
                .collect(),
            cache_sources,
            period,
            any_source_synced: AtomicBool::new(false),
            head_block_number: RwLock::new(U256::ZERO),
            health_state,
            latest_snapshot: RwLock::new(ReadinessSnapshot::default()),
            runtime_metrics: RuntimeHealthMetrics,
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
    pub fn update_head_block_number(&self, block_number: U256) {
        *self.sources_inner.head_block_number.write() = block_number;
    }

    #[cfg(any(test, feature = "test", feature = "bench-utils"))]
    pub fn readiness_snapshot(&self) -> ReadinessSnapshot {
        self.sources_inner.latest_snapshot.read().clone()
    }

    /// Returns the current head block number
    #[cfg(any(test, feature = "test", feature = "bench-utils"))]
    pub fn get_head_block_number(&self) -> U256 {
        *self.sources_inner.head_block_number.read()
    }
}

impl SourcesInner {
    fn spawn_monitoring(self: Arc<Self>) -> AbortHandle {
        tokio::task::spawn(self.run()).abort_handle()
    }

    async fn run(self: Arc<Self>) -> ! {
        let start = Instant::now();
        loop {
            let head_block_number = *self.head_block_number.read();
            let min_synced_block = self.cache_sources.get_minimum_synced_block_number();
            let timestamp = Instant::now().duration_since(start).as_secs();
            let snapshot = self.collect_snapshot(head_block_number, min_synced_block, timestamp);

            self.any_source_synced
                .store(snapshot.ready, Ordering::Release);
            self.log_transitions(&snapshot);
            self.record_runtime_metrics(&snapshot);
            *self.latest_snapshot.write() = snapshot.clone();
            self.health_state.update_readiness(snapshot);

            self.metrics.iter().for_each(|entry| {
                let (name, source_metric) = entry.pair();
                source_metric.commit(name);
            });

            sleep(self.period).await;
        }
    }

    fn collect_snapshot(
        &self,
        head_block_number: U256,
        min_synced_block: U256,
        timestamp: u64,
    ) -> ReadinessSnapshot {
        let mut any_synced = false;
        let mut state_worker_synced = false;
        let mut rpc_source_synced = false;
        let mut source_states = Vec::new();

        for source in self.cache_sources.iter_all_sources() {
            let source_name = source.name();
            let is_synced = source.is_synced(min_synced_block, head_block_number);

            if source_name == SourceName::StateWorker {
                state_worker_synced = is_synced;
            } else if source_name == SourceName::EthRpcSource {
                rpc_source_synced = is_synced;
            }

            if let Some(source_metric) = self.metrics.get(&source_name) {
                source_metric.update_sync_status(is_synced, timestamp);
            }

            any_synced |= is_synced;
            source_states.push(SourceReadinessSnapshot::new(
                source_name.to_string(),
                is_synced,
            ));
        }

        let fallback_active = rpc_source_synced && !state_worker_synced;
        let worker = if state_worker_synced {
            WorkerReadiness::Healthy
        } else if any_synced {
            WorkerReadiness::Degraded
        } else {
            WorkerReadiness::Unavailable
        };

        ReadinessSnapshot {
            ready: any_synced,
            fallback_active,
            worker,
            required_head: saturating_u64(head_block_number),
            minimum_synced_block: saturating_u64(min_synced_block),
            sources: source_states,
        }
    }

    fn log_transitions(&self, snapshot: &ReadinessSnapshot) {
        let previous = self.latest_snapshot.read().clone();
        let worker_runtime = state_worker_runtime_state().snapshot();

        let previous_sources: HashMap<&str, bool> = previous
            .sources
            .iter()
            .map(|source| (source.name.as_str(), source.ready))
            .collect();

        for source in &snapshot.sources {
            let was_ready = previous_sources.get(source.name.as_str()).copied();
            if let Some(was_ready) = was_ready
                && was_ready != source.ready
            {
                if source.ready {
                    info!(source = %source.name, "State source became ready");
                } else {
                    warn!(source = %source.name, "State source became unready");
                }
            }
        }

        if previous.fallback_active != snapshot.fallback_active {
            self.runtime_metrics.increment_fallback_transition();
            let ready_sources = ready_source_names(snapshot);

            if snapshot.fallback_active {
                warn!(
                    ready_sources = %ready_sources.join(","),
                    worker_restart_count = worker_runtime.restart_count,
                    worker_restart_backoff_seconds = worker_runtime.restart_backoff.as_secs_f64(),
                    "Primary state source is unavailable; serving from fallback source"
                );
            } else {
                info!(
                    ready_sources = %ready_sources.join(","),
                    "Primary state source recovered; fallback source no longer active"
                );
            }
        }

        if previous.ready != snapshot.ready {
            self.runtime_metrics.increment_readiness_transition();

            if snapshot.ready {
                info!(
                    ready_sources = %ready_source_names(snapshot).join(","),
                    "Sidecar readiness recovered"
                );
            } else {
                error!(
                    required_head = snapshot.required_head,
                    minimum_synced_block = snapshot.minimum_synced_block,
                    worker_restart_count = worker_runtime.restart_count,
                    worker_restart_backoff_seconds = worker_runtime.restart_backoff.as_secs_f64(),
                    "No state source can satisfy the required range"
                );
            }
        }
    }

    fn record_runtime_metrics(&self, snapshot: &ReadinessSnapshot) {
        self.runtime_metrics.set_readiness(snapshot.ready);
        self.runtime_metrics
            .set_fallback_active(snapshot.fallback_active);
        self.runtime_metrics
            .set_required_head(snapshot.required_head);
        self.runtime_metrics
            .set_required_minimum_block(snapshot.minimum_synced_block);
        self.runtime_metrics
            .set_worker_degraded(snapshot.worker == WorkerReadiness::Degraded);
        self.runtime_metrics
            .set_worker_unavailable(snapshot.worker == WorkerReadiness::Unavailable);
        self.runtime_metrics
            .set_state_worker_degraded(snapshot.worker != WorkerReadiness::Healthy);

        let state_worker_ready = snapshot
            .sources
            .iter()
            .find(|source| source.name == SourceName::StateWorker.to_string())
            .is_some_and(|source| source.ready);

        let worker_runtime = state_worker_runtime_state().snapshot();
        let worker_metric_state = worker_metric_state(snapshot, worker_runtime, state_worker_ready);

        self.runtime_metrics
            .set_legacy_state_worker_db_healthy(worker_metric_state.legacy_db_healthy);
        self.runtime_metrics
            .set_legacy_state_worker_head_block(worker_metric_state.legacy_head_block);
        self.runtime_metrics
            .set_legacy_state_worker_current_block(worker_metric_state.legacy_current_block);
        self.runtime_metrics
            .set_legacy_state_worker_sync_lag_blocks(worker_metric_state.legacy_sync_lag);
        self.runtime_metrics
            .set_legacy_state_worker_syncing(worker_metric_state.legacy_syncing);
        self.runtime_metrics
            .set_legacy_state_worker_following_head(worker_metric_state.legacy_following_head);
        self.runtime_metrics
            .set_state_worker_restart_count(worker_metric_state.restart_count);
        self.runtime_metrics
            .set_state_worker_restart_backoff_seconds(worker_metric_state.restart_backoff);
        self.runtime_metrics
            .set_state_worker_traced_head(worker_metric_state.traced_head);
        self.runtime_metrics
            .set_state_worker_flush_permitted_head(worker_metric_state.flush_permitted_head);
        self.runtime_metrics
            .set_state_worker_durable_head(worker_metric_state.durable_head);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WorkerMetricState {
    restart_count: u64,
    restart_backoff: Duration,
    traced_head: Option<u64>,
    flush_permitted_head: Option<u64>,
    durable_head: Option<u64>,
    legacy_db_healthy: bool,
    legacy_head_block: u64,
    legacy_current_block: Option<u64>,
    legacy_sync_lag: Option<u64>,
    legacy_syncing: bool,
    legacy_following_head: bool,
}

fn worker_metric_state(
    snapshot: &ReadinessSnapshot,
    runtime: StateWorkerRuntimeSnapshot,
    state_worker_ready: bool,
) -> WorkerMetricState {
    let durable_head = runtime.durable_head;

    WorkerMetricState {
        restart_count: runtime.restart_count,
        restart_backoff: runtime.restart_backoff,
        traced_head: runtime.traced_head,
        flush_permitted_head: runtime.flush_permitted_head,
        durable_head,
        legacy_db_healthy: state_worker_ready,
        legacy_head_block: snapshot.required_head,
        legacy_current_block: durable_head,
        legacy_sync_lag: durable_head.map(|head| snapshot.required_head.saturating_sub(head)),
        legacy_syncing: !state_worker_ready && snapshot.ready,
        legacy_following_head: state_worker_ready,
    }
}

fn ready_source_names(snapshot: &ReadinessSnapshot) -> Vec<&str> {
    snapshot
        .sources
        .iter()
        .filter(|source| source.ready)
        .map(|source| source.name.as_str())
        .collect()
}

fn saturating_u64(value: U256) -> u64 {
    value.try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        SourcesInner,
        ready_source_names,
        worker_metric_state,
    };
    use crate::{
        SourceError,
        Sources as CacheSources,
        cache::sources::{
            Source,
            SourceName,
        },
        health::{
            HealthState,
            ReadinessSnapshot,
            SourceReadinessSnapshot,
            WorkerReadiness,
        },
        metrics::StateWorkerRuntimeSnapshot,
    };
    use alloy::primitives::U256;
    use assertion_executor::primitives::{
        AccountInfo,
        Address,
        B256,
        Bytecode,
    };
    use parking_lot::RwLock;
    use revm::{
        DatabaseRef,
        primitives::{
            StorageKey,
            StorageValue,
        },
    };
    use std::{
        sync::{
            Arc,
            atomic::{
                AtomicBool,
                AtomicU64,
                Ordering,
            },
        },
        time::Duration,
    };

    #[derive(Debug)]
    struct MockSource {
        name: SourceName,
        ready: AtomicBool,
    }

    impl MockSource {
        fn new(name: SourceName, ready: bool) -> Self {
            Self {
                name,
                ready: AtomicBool::new(ready),
            }
        }
    }

    impl DatabaseRef for MockSource {
        type Error = SourceError;

        fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(None)
        }

        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            Ok(Bytecode::default())
        }

        fn storage_ref(
            &self,
            _address: Address,
            _index: StorageKey,
        ) -> Result<StorageValue, Self::Error> {
            Ok(U256::ZERO)
        }

        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    impl Source for MockSource {
        fn is_synced(&self, _min_synced_block: U256, _latest_head: U256) -> bool {
            self.ready.load(Ordering::Acquire)
        }

        fn name(&self) -> SourceName {
            self.name
        }

        fn update_cache_status(&self, _min_synced_block: U256, _latest_head: U256) {}
    }

    fn build_inner(sources: Vec<Arc<dyn Source>>) -> SourcesInner {
        let cache_sources = Arc::new(CacheSources::new(sources, 10));
        SourcesInner {
            metrics: cache_sources
                .list_configured_sources()
                .into_iter()
                .map(|source| (source, crate::metrics::SourceMetrics::default()))
                .collect(),
            cache_sources,
            period: Duration::from_millis(20),
            any_source_synced: AtomicBool::new(false),
            head_block_number: RwLock::new(U256::ZERO),
            health_state: Arc::new(HealthState::default()),
            latest_snapshot: RwLock::new(crate::health::ReadinessSnapshot::default()),
            runtime_metrics: crate::metrics::RuntimeHealthMetrics,
        }
    }

    #[test]
    fn collect_snapshot_marks_degraded_when_rpc_fallback_can_serve() {
        let inner = build_inner(vec![
            Arc::new(MockSource::new(SourceName::StateWorker, false)),
            Arc::new(MockSource::new(SourceName::EthRpcSource, true)),
        ]);

        let snapshot = inner.collect_snapshot(U256::from(42), U256::from(32), 1);

        assert!(snapshot.ready);
        assert!(snapshot.fallback_active);
        assert_eq!(snapshot.worker, WorkerReadiness::Degraded);
        assert_eq!(ready_source_names(&snapshot), vec!["EthRpcSource"]);
    }

    #[test]
    fn collect_snapshot_treats_state_worker_as_primary_even_when_configured_second() {
        let inner = build_inner(vec![
            Arc::new(MockSource::new(SourceName::EthRpcSource, true)),
            Arc::new(MockSource::new(SourceName::StateWorker, false)),
        ]);

        let snapshot = inner.collect_snapshot(U256::from(42), U256::from(32), 1);

        assert!(snapshot.ready);
        assert!(snapshot.fallback_active);
        assert_eq!(snapshot.worker, WorkerReadiness::Degraded);
        assert_eq!(ready_source_names(&snapshot), vec!["EthRpcSource"]);
    }

    #[test]
    fn collect_snapshot_marks_unavailable_when_no_source_is_ready() {
        let inner = build_inner(vec![
            Arc::new(MockSource::new(SourceName::StateWorker, false)),
            Arc::new(MockSource::new(SourceName::EthRpcSource, false)),
        ]);

        let snapshot = inner.collect_snapshot(U256::from(42), U256::from(32), 1);

        assert!(!snapshot.ready);
        assert!(!snapshot.fallback_active);
        assert_eq!(snapshot.worker, WorkerReadiness::Unavailable);
        assert!(ready_source_names(&snapshot).is_empty());
    }

    #[test]
    fn worker_metric_state_prefers_runtime_snapshot_over_last_ready_head() {
        let readiness = ReadinessSnapshot {
            ready: true,
            fallback_active: true,
            worker: WorkerReadiness::Degraded,
            required_head: 128,
            minimum_synced_block: 120,
            sources: vec![
                SourceReadinessSnapshot::new("StateWorker", false),
                SourceReadinessSnapshot::new("EthRpcSource", true),
            ],
        };
        let runtime = StateWorkerRuntimeSnapshot {
            restart_count: 3,
            restart_backoff: Duration::from_secs(7),
            traced_head: Some(140),
            flush_permitted_head: Some(136),
            durable_head: Some(132),
        };

        let metrics = worker_metric_state(&readiness, runtime, false);

        assert_eq!(metrics.restart_count, 3);
        assert_eq!(metrics.restart_backoff, Duration::from_secs(7));
        assert_eq!(metrics.traced_head, Some(140));
        assert_eq!(metrics.flush_permitted_head, Some(136));
        assert_eq!(metrics.durable_head, Some(132));
        assert_eq!(metrics.legacy_current_block, Some(132));
        assert_eq!(metrics.legacy_sync_lag, Some(128_u64.saturating_sub(132)));
        assert!(metrics.legacy_syncing);
        assert!(!metrics.legacy_following_head);
    }

    #[test]
    fn worker_metric_state_leaves_unknown_heads_unset_without_runtime_snapshot() {
        let readiness = ReadinessSnapshot {
            ready: true,
            fallback_active: false,
            worker: WorkerReadiness::Healthy,
            required_head: 64,
            minimum_synced_block: 60,
            sources: vec![SourceReadinessSnapshot::new("StateWorker", true)],
        };

        let metrics = worker_metric_state(&readiness, StateWorkerRuntimeSnapshot::default(), true);

        assert_eq!(metrics.restart_count, 0);
        assert_eq!(metrics.restart_backoff, Duration::ZERO);
        assert_eq!(metrics.traced_head, None);
        assert_eq!(metrics.flush_permitted_head, None);
        assert_eq!(metrics.durable_head, None);
        assert_eq!(metrics.legacy_current_block, None);
        assert_eq!(metrics.legacy_sync_lag, None);
        assert!(!metrics.legacy_syncing);
        assert!(metrics.legacy_following_head);
    }
}
