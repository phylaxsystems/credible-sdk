use crate::{
    cache::sources::redis::CacheStatus,
    critical,
};
use alloy::primitives::U256;
use parking_lot::RwLock;
use state_store::StateReader;
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicBool,
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

#[allow(clippy::needless_pass_by_value)]
pub fn publish_sync_state(
    cache_status: Arc<CacheStatus>,
    latest_head: Option<U256>,
    oldest_block: Option<U256>,
    target_block: &RwLock<U256>,
    observed_block: &RwLock<U256>,
    oldest_observed_block: &RwLock<U256>,
    sync_status: &AtomicBool,
) {
    if let Some(block_number) = latest_head {
        *observed_block.write() = block_number;
        let oldest = oldest_block.unwrap_or(block_number);
        // We shift the oldest block to be +1, so we avoid fetching status for a block in Redis which will be rotated next
        // But we can only do it if the redis depth is > 1
        let shift = if block_number > oldest {
            U256::from(1)
        } else {
            U256::ZERO
        };
        *oldest_observed_block.write() = oldest + shift;

        // Find the intersection of the two ranges
        let lower_bound = (*cache_status.min_synced_block.read()).max(oldest);
        let upper_bound = (*cache_status.latest_head.read()).min(block_number);

        // Update the current target block
        if lower_bound <= upper_bound {
            // Pick the most recent block in the valid range
            *target_block.write() = upper_bound;
        }

        let current_target = *target_block.read();

        let within_target = current_target >= oldest && current_target <= block_number;

        sync_status.store(within_target, Ordering::Release);
    } else {
        *observed_block.write() = U256::ZERO;
        *oldest_observed_block.write() = U256::ZERO;
        sync_status.store(false, Ordering::Release);
    }
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_sync_task(
    cache_status: Arc<CacheStatus>,
    reader: StateReader,
    target_block: Arc<RwLock<U256>>,
    observed_block: Arc<RwLock<U256>>,
    oldest_block: Arc<RwLock<U256>>,
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
                                cache_status.clone(),
                                Some(U256::from(latest_head)),
                                Some(U256::from(oldest)),
                                &target_block,
                                &observed_block,
                                &oldest_block,
                                &sync_status,
                            );
                        }
                        Ok(Ok(None)) => {
                            publish_sync_state(
                                cache_status.clone(),
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
                                cache_status.clone(),
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
                                cache_status.clone(),
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

    fn u256(val: u64) -> U256 {
        U256::from(val)
    }

    #[test]
    fn publish_sync_state_sets_values_within_target_range() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(6)),
            latest_head: RwLock::new(u256(9)),
        });
        let target_block = RwLock::new(u256(7));
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(false);

        publish_sync_state(
            cache_status,
            Some(u256(8)),
            Some(u256(6)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        assert_eq!(*observed_block.read(), u256(8));
        assert_eq!(*oldest_block.read(), u256(7));
        // target_block should be updated to upper_bound = min(9, 8) = 8
        assert_eq!(*target_block.read(), u256(8));
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn publish_sync_state_marks_unsynced_when_target_below_oldest() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(U256::ZERO),
            latest_head: RwLock::new(u256(10)),
        });
        let target_block = RwLock::new(u256(5));
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(true);

        publish_sync_state(
            cache_status,
            Some(u256(8)),
            Some(u256(6)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        assert_eq!(*observed_block.read(), u256(8));
        assert_eq!(*oldest_block.read(), u256(7));
        // target_block should be updated to upper_bound = min(10, 8) = 8
        assert_eq!(*target_block.read(), u256(8));
        // sync_status should be true because 8 is within [6, 8]
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn publish_sync_state_resets_when_latest_missing() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(5)),
            latest_head: RwLock::new(u256(15)),
        });
        let target_block = RwLock::new(u256(10));
        let observed_block = RwLock::new(u256(9));
        let oldest_block = RwLock::new(u256(7));
        let sync_status = AtomicBool::new(true);

        publish_sync_state(
            cache_status,
            None,
            None,
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        assert_eq!(*observed_block.read(), U256::ZERO);
        assert_eq!(*oldest_block.read(), U256::ZERO);
        assert!(!sync_status.load(Ordering::Acquire));
        // target_block should remain unchanged when Redis is unavailable
        assert_eq!(*target_block.read(), u256(10));
    }

    #[test]
    fn publish_sync_state_requires_genesis_for_zero_target() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(2)),
            latest_head: RwLock::new(u256(4)),
        });
        let target_block = RwLock::new(U256::ZERO);
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(true);

        publish_sync_state(
            cache_status,
            Some(u256(5)),
            Some(u256(1)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        assert_eq!(*observed_block.read(), u256(5));
        assert_eq!(*oldest_block.read(), u256(2));
        // Intersection: [max(2,1), min(4,5)] = [2, 4]
        // target_block should be updated to 4
        assert_eq!(*target_block.read(), u256(4));
        // sync_status: 4 is within [1, 5], so should be true
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn test_publish_sync_state_updates_target_block_on_overlap() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(100)),
            latest_head: RwLock::new(u256(100)),
        });
        let target_block = RwLock::new(U256::ZERO);
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(false);

        // Redis syncs to 100 with range [98, 100]
        publish_sync_state(
            cache_status.clone(),
            Some(u256(100)), // latest_head
            Some(u256(98)),  // oldest_block
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        // Should calculate intersection [max(100,98), min(100,100)] = [100, 100]
        assert_eq!(*target_block.read(), u256(100));
        assert!(sync_status.load(Ordering::Acquire));
        assert_eq!(*observed_block.read(), u256(100));
        assert_eq!(*oldest_block.read(), u256(99));
    }

    #[test]
    fn test_publish_sync_state_handles_late_redis_sync() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(100)),
            latest_head: RwLock::new(u256(100)),
        });
        let target_block = RwLock::new(U256::ZERO);
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(false);

        // First call: Redis only has up to 99
        publish_sync_state(
            cache_status.clone(),
            Some(u256(99)),
            Some(u256(97)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        // No overlap: [max(100,97), min(100,99)] = [100, 99], invalid (100 > 99)
        // target_block not updated, not synced
        assert_eq!(*target_block.read(), U256::ZERO);
        assert!(!sync_status.load(Ordering::Acquire));

        // Second call: Redis catches up to 100
        publish_sync_state(
            cache_status.clone(),
            Some(u256(100)),
            Some(u256(98)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        // Now there's overlap: [max(100,98), min(100,100)] = [100, 100]
        assert_eq!(*target_block.read(), u256(100));
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn test_publish_sync_state_respects_cache_status_range() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(95)),
            latest_head: RwLock::new(u256(105)),
        });
        let target_block = RwLock::new(U256::ZERO);
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(false);

        // Redis has [90, 100]
        publish_sync_state(
            cache_status.clone(),
            Some(u256(100)),
            Some(u256(90)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        // Intersection: [max(95,90), min(105,100)] = [95, 100]
        // Should pick upper_bound = 100
        assert_eq!(*target_block.read(), u256(100));
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn test_publish_sync_state_no_update_without_overlap() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(200)),
            latest_head: RwLock::new(u256(250)),
        });
        let target_block = RwLock::new(u256(150)); // Previous valid value
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(true);

        // Redis has [100, 150]
        publish_sync_state(
            cache_status.clone(),
            Some(u256(150)),
            Some(u256(100)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        // No overlap: [max(200,100), min(250,150)] = [200, 150], invalid
        // target_block should NOT be updated
        assert_eq!(*target_block.read(), u256(150));
        // But sync_status should be updated: 150 is within Redis range [100, 150]
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn test_publish_sync_state_with_moving_window() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(95)),
            latest_head: RwLock::new(u256(105)),
        });
        let target_block = RwLock::new(U256::ZERO);
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(false);

        // First: Redis has [90, 100]
        publish_sync_state(
            cache_status.clone(),
            Some(u256(100)),
            Some(u256(90)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        assert_eq!(*target_block.read(), u256(100));
        assert!(sync_status.load(Ordering::Acquire));

        // Second: Redis window moves forward [95, 105]
        publish_sync_state(
            cache_status.clone(),
            Some(u256(105)),
            Some(u256(95)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        // Intersection: [max(95,95), min(105,105)] = [95, 105]
        // Should update to 105
        assert_eq!(*target_block.read(), u256(105));
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn test_publish_sync_state_partial_overlap() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(100)),
            latest_head: RwLock::new(u256(200)),
        });
        let target_block = RwLock::new(U256::ZERO);
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(false);

        // Redis has [150, 250]
        publish_sync_state(
            cache_status.clone(),
            Some(u256(250)),
            Some(u256(150)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        // Intersection: [max(100,150), min(200,250)] = [150, 200]
        // Should pick upper_bound = 200
        assert_eq!(*target_block.read(), u256(200));
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn test_publish_sync_state_exact_match() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(100)),
            latest_head: RwLock::new(u256(100)),
        });
        let target_block = RwLock::new(U256::ZERO);
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(false);

        // Redis has exactly [100, 100]
        publish_sync_state(
            cache_status.clone(),
            Some(u256(100)),
            Some(u256(100)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        // Intersection: [max(100,100), min(100,100)] = [100, 100]
        assert_eq!(*target_block.read(), u256(100));
        assert!(sync_status.load(Ordering::Acquire));
    }

    #[test]
    fn test_publish_sync_state_preserves_target_on_no_overlap() {
        let cache_status = Arc::new(CacheStatus {
            min_synced_block: RwLock::new(u256(50)),
            latest_head: RwLock::new(u256(60)),
        });
        let target_block = RwLock::new(u256(55)); // Previously set valid value
        let observed_block = RwLock::new(U256::ZERO);
        let oldest_block = RwLock::new(U256::ZERO);
        let sync_status = AtomicBool::new(true);

        // Redis has [100, 200] - no overlap with [50, 60]
        publish_sync_state(
            cache_status.clone(),
            Some(u256(200)),
            Some(u256(100)),
            &target_block,
            &observed_block,
            &oldest_block,
            &sync_status,
        );

        // No overlap: [max(50,100), min(60,200)] = [100, 60], invalid
        // target_block should remain at 55
        assert_eq!(*target_block.read(), u256(55));
        // sync_status checks if target_block (55) is within Redis range [100, 200] -> false
        assert!(!sync_status.load(Ordering::Acquire));
    }
}
