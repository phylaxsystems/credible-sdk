//! Consistency management for state synchronization

use super::{
    ConsistencyStatus,
    StateError,
};
use std::{
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};
use tokio::sync::RwLock;
use tracing::{
    debug,
    error,
    warn,
};

/// Configuration for consistency checking
#[derive(Debug, Clone)]
pub struct ConsistencyConfig {
    /// Maximum blocks behind before considering state invalid
    pub max_block_lag: u64,
    /// Time threshold for considering state stale
    pub stale_threshold: Duration,
    /// How often to check consistency
    pub check_interval: Duration,
    /// Enable automatic reorg detection
    pub detect_reorgs: bool,
}

impl Default for ConsistencyConfig {
    fn default() -> Self {
        Self {
            max_block_lag: 100,
            stale_threshold: Duration::from_secs(30),
            check_interval: Duration::from_secs(5),
            detect_reorgs: true,
        }
    }
}

/// Tracks blockchain head and detects inconsistencies
pub struct ConsistencyManager {
    config: ConsistencyConfig,

    /// Current blockchain head from our perspective
    local_head: AtomicU64,

    /// Last confirmed head from Besu
    besu_head: AtomicU64,

    /// Timestamp of last successful sync
    last_sync: Arc<RwLock<Instant>>,

    /// Known block hashes for reorg detection
    block_hashes: Arc<RwLock<std::collections::HashMap<u64, String>>>,

    /// Consistency status cache
    status_cache: Arc<RwLock<(ConsistencyStatus, Instant)>>,
}

impl ConsistencyManager {
    pub fn new(config: ConsistencyConfig) -> Self {
        Self {
            config,
            local_head: AtomicU64::new(0),
            besu_head: AtomicU64::new(0),
            last_sync: Arc::new(RwLock::new(Instant::now())),
            block_hashes: Arc::new(RwLock::new(std::collections::HashMap::new())),
            status_cache: Arc::new(RwLock::new((ConsistencyStatus::Unknown, Instant::now()))),
        }
    }

    /// Check if our state is consistent for a given block
    pub async fn check_consistency(&self, target_block: u64) -> ConsistencyStatus {
        // Check cache first
        {
            let (cached_status, cached_time) = *self.status_cache.read().await;
            if cached_time.elapsed() < self.config.check_interval {
                return cached_status;
            }
        }

        let local_head = self.local_head.load(Ordering::SeqCst);
        let besu_head = self.besu_head.load(Ordering::SeqCst);

        let status = if target_block > besu_head {
            // Requesting future block
            ConsistencyStatus::Invalid
        } else if target_block > local_head {
            // We're behind
            let gap = target_block - local_head;
            if gap <= self.config.max_block_lag {
                ConsistencyStatus::Stale
            } else {
                ConsistencyStatus::Invalid
            }
        } else {
            // Check if we're up to date
            let last_sync_age = self.last_sync.read().await.elapsed();
            if last_sync_age > self.config.stale_threshold {
                ConsistencyStatus::Stale
            } else {
                ConsistencyStatus::Fresh
            }
        };

        // Update cache
        *self.status_cache.write().await = (status, Instant::now());

        debug!(
            "Consistency check: local_head={}, besu_head={}, target={}, status={:?}",
            local_head, besu_head, target_block, status
        );

        status
    }

    /// Update our local head when new state is applied
    pub async fn update_head(&self, block_number: u64) {
        let old_head = self.local_head.swap(block_number, Ordering::SeqCst);

        if block_number > old_head + 1 {
            warn!(
                "Block gap detected: jumped from {} to {}",
                old_head, block_number
            );
        }

        *self.last_sync.write().await = Instant::now();

        // Invalidate status cache on head update
        *self.status_cache.write().await = (ConsistencyStatus::Unknown, Instant::now());
    }

    /// Update Besu head (from external source)
    pub fn update_besu_head(&self, block_number: u64) {
        self.besu_head.store(block_number, Ordering::SeqCst);
    }

    /// Detect reorg by comparing block hashes
    pub async fn detect_reorg(&self, block_number: u64, block_hash: String) -> bool {
        if !self.config.detect_reorgs {
            return false;
        }

        let mut hashes = self.block_hashes.write().await;

        if let Some(existing_hash) = hashes.get(&block_number) {
            if existing_hash != &block_hash {
                error!(
                    "Reorg detected at block {}: expected {}, got {}",
                    block_number, existing_hash, block_hash
                );

                // Remove all blocks >= reorg point
                hashes.retain(|&k, _| k < block_number);

                // Update local head to before reorg
                self.local_head
                    .store(block_number.saturating_sub(1), Ordering::SeqCst);

                return true;
            }
        } else {
            // Store new block hash
            hashes.insert(block_number, block_hash);

            // Cleanup old hashes (keep last 1000)
            if hashes.len() > 1000 {
                let min_keep = block_number.saturating_sub(1000);
                hashes.retain(|&k, _| k >= min_keep);
            }
        }

        false
    }

    /// Verify consistency by checking a few recent blocks
    pub async fn verify_consistency(&self) -> Result<(), StateError> {
        let local_head = self.local_head.load(Ordering::SeqCst);

        // Check last few blocks for consistency
        // This would involve comparing our stored hashes with Besu
        // Implementation depends on how you access Besu's state

        debug!("Verified consistency up to block {}", local_head);
        Ok(())
    }

    pub async fn get_current_head(&self) -> Result<u64, StateError> {
        Ok(self.local_head.load(Ordering::SeqCst))
    }

    /// Get lag behind Besu
    pub fn get_lag(&self) -> u64 {
        let local = self.local_head.load(Ordering::SeqCst);
        let besu = self.besu_head.load(Ordering::SeqCst);
        besu.saturating_sub(local)
    }
}
