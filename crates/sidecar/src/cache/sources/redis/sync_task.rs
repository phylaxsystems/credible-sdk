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

pub(crate) fn spawn_sync_task(
    reader: StateReader,
    current_block: Arc<AtomicU64>,
    observed_block: Arc<AtomicU64>,
    sync_status: Arc<AtomicBool>,
    cancel: CancellationToken,
    poll_interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let reader = reader.clone();
                    let latest_block = spawn_blocking(move || reader.latest_block_number()).await;

                    match latest_block {
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
                            critical!(error = ?error, "redis sync task failed to read latest block");
                            observed_block.store(0, Ordering::Release);
                            sync_status.store(false, Ordering::Release);
                        }
                        Err(join_error) => {
                            critical!(error = ?join_error, "redis sync task join error");
                            observed_block.store(0, Ordering::Release);
                            sync_status.store(false, Ordering::Release);
                        }
                    }
                }
            }
        }
    })
}
