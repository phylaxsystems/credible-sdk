use crate::critical;
use state_store::StateReader;
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

pub(crate) fn publish_sync_state(
    latest_block: Option<u64>,
    oldest_block: Option<u64>,
    current_block: &AtomicU64,
    observed_block: &AtomicU64,
    oldest_observed_block: &AtomicU64,
    sync_status: &AtomicBool,
) {
    if let Some(block_number) = latest_block {
        observed_block.store(block_number, Ordering::Release);
        let oldest = oldest_block.unwrap_or(block_number);
        oldest_observed_block.store(oldest, Ordering::Release);

        let target_block = current_block.load(Ordering::Acquire);
        let within_target = if target_block == 0 {
            block_number == 0
        } else {
            block_number <= target_block
        };

        sync_status.store(within_target, Ordering::Release);
    } else {
        observed_block.store(0, Ordering::Release);
        oldest_observed_block.store(0, Ordering::Release);
        sync_status.store(false, Ordering::Release);
    }
}

pub(crate) fn spawn_sync_task(
    reader: StateReader,
    current_block: Arc<AtomicU64>,
    observed_block: Arc<AtomicU64>,
    oldest_block: Arc<AtomicU64>,
    sync_status: Arc<AtomicBool>,
    cancel: CancellationToken,
    poll_interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let reader = reader.clone();
                    let available_range =
                        spawn_blocking(move || reader.get_available_block_range()).await;

                    match available_range {
                        Ok(Ok(Some((oldest, latest)))) => {
                            publish_sync_state(
                                Some(latest),
                                Some(oldest),
                                &current_block,
                                &observed_block,
                                &oldest_block,
                                &sync_status,
                            );
                        }
                        Ok(Ok(None)) => {
                            publish_sync_state(
                                None,
                                None,
                                &current_block,
                                &observed_block,
                                &oldest_block,
                                &sync_status,
                            );
                        }
                        Ok(Err(error)) => {
                            critical!(error = ?error, "redis sync task failed to read latest block range");
                            publish_sync_state(
                                None,
                                None,
                                &current_block,
                                &observed_block,
                                &oldest_block,
                                &sync_status,
                            );
                        }
                        Err(join_error) => {
                            critical!(error = ?join_error, "redis sync task join error");
                            publish_sync_state(
                                None,
                                None,
                                &current_block,
                                &observed_block,
                                &oldest_block,
                                &sync_status,
                            );
                        }
                    }
                }
            }
        }
    })
}
