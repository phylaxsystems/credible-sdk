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

#[derive(Debug, Clone)]
pub(crate) struct SyncTaskHandle<B> {
    backend: Arc<B>,
    namespace: String,
    current_block: Arc<AtomicU64>,
    observed_block: Arc<AtomicU64>,
    sync_status: Arc<AtomicBool>,
}

impl<B: RedisBackend> SyncTaskHandle<B>  {
    pub fn new(
        backend: Arc<B>,
        namespace: String,
        current_block: Arc<AtomicU64>,
        observed_block: Arc<AtomicU64>,
        sync_status: Arc<AtomicBool>,
    ) -> Self {
        Self {
            backend,
            namespace,
            current_block,
            observed_block,
            sync_status,
        }
    }

    pub async fn run(
        &mut self,
        poll_interval: Duration,
    ) {
        let mut ticker = interval(poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let fetch_result = self.fetch_current_block();

            match fetch_result {
                Ok(Some(block_number)) => {
                    self.observed_block.store(block_number, Ordering::Release);
                    let target_block = self.current_block.load(Ordering::Acquire);
                    let within_target = if target_block == 0 {
                        block_number == 0
                    } else {
                        block_number <= target_block
                    };
                    self.sync_status.store(within_target, Ordering::Release);
                }
                Ok(None) => {
                    self.observed_block.store(0, Ordering::Release);
                    self.sync_status.store(false, Ordering::Release);
                }
                Err(error) => {
                    critical!(error = ?error, "redis sync task failed to fetch current block");
                    self.observed_block.store(0, Ordering::Release);
                    self.sync_status.store(false, Ordering::Release);
                }
                Err(join_error) => {
                    critical!(error = ?join_error, "redis sync task panicked while fetching current block");
                    self.observed_block.store(0, Ordering::Release);
                    self.sync_status.store(false, Ordering::Release);
                }
            }
        }
    }

    fn fetch_current_block(
        &self,
    ) -> Result<Option<u64>, RedisCacheError> {
        let key = format!("{}:{}", self.namespace, CURRENT_BLOCK_KEY);
        self.backend.get(&key).and_then(|opt| {
            opt.map(|value| parse_u64(&value, &key, CURRENT_BLOCK_KEY))
                .transpose()
        })
    }
}
