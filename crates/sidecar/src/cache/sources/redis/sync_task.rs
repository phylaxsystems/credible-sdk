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

pub fn publish_sync_state(
    latest_head: Option<u64>,
    oldest_block: Option<u64>,
    target_block: &AtomicU64,
    observed_block: &AtomicU64,
    oldest_observed_block: &AtomicU64,
    sync_status: &AtomicBool,
) {
    if let Some(block_number) = latest_head {
        observed_block.store(block_number, Ordering::Release);
        let oldest = oldest_block.unwrap_or(block_number);
        oldest_observed_block.store(oldest, Ordering::Release);

        let target_block = target_block.load(Ordering::Acquire);

        let within_target = target_block >= oldest && target_block <= block_number;

        sync_status.store(within_target, Ordering::Release);
    } else {
        observed_block.store(0, Ordering::Release);
        oldest_observed_block.store(0, Ordering::Release);
        sync_status.store(false, Ordering::Release);
    }
}

pub fn spawn_sync_task(
    reader: StateReader,
    target_block: Arc<AtomicU64>,
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
                        Ok(Ok(Some((oldest, latest_head)))) => {
                            publish_sync_state(
                                Some(latest_head),
                                Some(oldest),
                                &target_block,
                                &observed_block,
                                &oldest_block,
                                &sync_status,
                            );
                        }
                        Ok(Ok(None)) => {
                            publish_sync_state(
                                None,
                                None,
                                &target_block,
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
                                &target_block,
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
                                &target_block,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn publish_sync_state_sets_values_within_target_range() {
        let latest_head = AtomicU64::new(7);
        let observed_block = AtomicU64::new(0);
        let oldest_block = AtomicU64::new(0);
        let sync_status = AtomicBool::new(false);

        publish_sync_state(
            Some(8),
            Some(6),
            &latest_head,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        assert_eq!(observed_block.load(Ordering::Acquire), 8);
        assert_eq!(oldest_block.load(Ordering::Acquire), 6);
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn publish_sync_state_marks_unsynced_when_target_below_oldest() {
        let latest_head = AtomicU64::new(5);
        let observed_block = AtomicU64::new(0);
        let oldest_block = AtomicU64::new(0);
        let sync_status = AtomicBool::new(true);

        publish_sync_state(
            Some(8),
            Some(6),
            &latest_head,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        assert_eq!(observed_block.load(Ordering::Acquire), 8);
        assert_eq!(oldest_block.load(Ordering::Acquire), 6);
        assert!(!sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn publish_sync_state_resets_when_latest_missing() {
        let latest_head = AtomicU64::new(10);
        let observed_block = AtomicU64::new(9);
        let oldest_block = AtomicU64::new(7);
        let sync_status = AtomicBool::new(true);

        publish_sync_state(
            None,
            None,
            &latest_head,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        assert_eq!(observed_block.load(Ordering::Acquire), 0);
        assert_eq!(oldest_block.load(Ordering::Acquire), 0);
        assert!(!sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn publish_sync_state_requires_genesis_for_zero_target() {
        let latest_head = AtomicU64::new(0);
        let observed_block = AtomicU64::new(0);
        let oldest_block = AtomicU64::new(0);
        let sync_status = AtomicBool::new(true);

        publish_sync_state(
            Some(5),
            Some(1),
            &latest_head,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        assert_eq!(observed_block.load(Ordering::Acquire), 5);
        assert_eq!(oldest_block.load(Ordering::Acquire), 1);
        assert!(!sync_status.load(Ordering::Acquire));
    }
}
