use super::{
    CURRENT_BLOCK_KEY,
    RedisBackend,
    RedisCacheError,
    utils::parse_u64,
};
use crate::critical;
use std::{
    fmt,
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
use tokio::{
    task::{
        JoinHandle,
        spawn_blocking,
    },
    time::{
        MissedTickBehavior,
        interval,
    },
};
use tokio_util::sync::CancellationToken;

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(250);

pub(crate) struct SyncTaskHandle {
    cancel_token: CancellationToken,
    join_handle: JoinHandle<()>,
}

impl fmt::Debug for SyncTaskHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncTaskHandle").finish_non_exhaustive()
    }
}

impl Drop for SyncTaskHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        self.join_handle.abort();
    }
}

pub(crate) fn spawn<B>(
    backend: Arc<B>,
    namespace: String,
    current_block: Arc<AtomicU64>,
    observed_block: Arc<AtomicU64>,
    sync_status: Arc<AtomicBool>,
) -> SyncTaskHandle
where
    B: RedisBackend + 'static,
{
    spawn_with_interval(
        backend,
        namespace,
        current_block,
        observed_block,
        sync_status,
        DEFAULT_SYNC_INTERVAL,
    )
}

fn spawn_with_interval<B>(
    backend: Arc<B>,
    namespace: String,
    current_block: Arc<AtomicU64>,
    observed_block: Arc<AtomicU64>,
    sync_status: Arc<AtomicBool>,
    poll_interval: Duration,
) -> SyncTaskHandle
where
    B: RedisBackend + 'static,
{
    let cancel_token = CancellationToken::new();
    let shutdown_token = cancel_token.clone();
    let namespace = Arc::<str>::from(namespace);

    let join_handle = tokio::spawn(async move {
        let mut ticker = interval(poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => break,
                _ = ticker.tick() => {
                    let backend = Arc::clone(&backend);
                    let namespace = Arc::clone(&namespace);
                    let fetch_result = spawn_blocking(move || fetch_current_block(backend, namespace)).await;

                    match fetch_result {
                        Ok(Ok(Some(block_number))) => {
                            observed_block.store(block_number, Ordering::Release);
                            let target_block = current_block.load(Ordering::Acquire);
                            let within_target = if target_block == 0 {
                                block_number == 0
                            } else {
                                block_number <= target_block
                            };
                            sync_status.store(within_target, Ordering::Release);
                        }
                        Ok(Ok(None)) => {
                            observed_block.store(0, Ordering::Release);
                            sync_status.store(false, Ordering::Release);
                        }
                        Ok(Err(error)) => {
                            critical!(error = ?error, "redis sync task failed to fetch current block");
                            observed_block.store(0, Ordering::Release);
                            sync_status.store(false, Ordering::Release);
                        }
                        Err(join_error) => {
                            critical!(error = ?join_error, "redis sync task panicked while fetching current block");
                            observed_block.store(0, Ordering::Release);
                            sync_status.store(false, Ordering::Release);
                        }
                    }
                }
            }
        }
    });

    SyncTaskHandle {
        cancel_token,
        join_handle,
    }
}

fn fetch_current_block<B: RedisBackend>(
    backend: Arc<B>,
    namespace: Arc<str>,
) -> Result<Option<u64>, RedisCacheError> {
    let key = format!("{}:{}", namespace, CURRENT_BLOCK_KEY);
    backend.get(&key).and_then(|opt| {
        opt.map(|value| parse_u64(&value, &key, CURRENT_BLOCK_KEY))
            .transpose()
    })
}
