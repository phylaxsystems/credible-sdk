use super::{
    BlockDelta,
    BlockNum,
    OverlayEvictionError,
    OverlayEvictionOutcome,
    TableKey,
    TableValue,
};
use alloy_primitives::U256;
use dashmap::DashMap;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicU64,
            Ordering,
        },
    },
};
use tracing::error;

/// Sentinel value indicating no block is currently being tracked.
const NO_ACTIVE_BLOCK: u64 = u64::MAX;

/// Mutable state only accessed at commit / configuration time.
#[derive(Debug, Default)]
struct CommitState {
    deltas: VecDeque<BlockDelta>,
    max_committed_blocks: Option<usize>,
}

/// Shared eviction-tracking state that uses `AtomicU64` for `current_block`
/// so the hot path (`touch_key`) can read it without acquiring a mutex.
///
/// Layout:
///   - `current_block` — read atomically on every cache access (lock-free).
///   - `current_keys`  — only locked for the brief `Vec::push` when a key is
///     first touched in a block. Reads and dedup checks go through the
///     lock-free `last_touched` `DashMap`.
///   - `commit_state`  — locked once per block commit; never contended with
///     `touch_key`.
#[derive(Debug)]
struct EvictionState {
    current_block: AtomicU64,
    current_keys: Mutex<Vec<TableKey>>,
    commit_state: Mutex<CommitState>,
}

impl Default for EvictionState {
    fn default() -> Self {
        Self {
            current_block: AtomicU64::new(NO_ACTIVE_BLOCK),
            current_keys: Mutex::new(Vec::new()),
            commit_state: Mutex::new(CommitState::default()),
        }
    }
}

/// Consolidates all cache-eviction bookkeeping shared by `OverlayDb` and
/// `ActiveOverlay`.
///
/// Holds `Arc` references to the overlay map and the per-key touch map so it
/// can insert, touch, and evict entries without the callers duplicating logic.
/// Cloning is cheap (all fields are `Arc`).
#[derive(Debug, Clone)]
pub(crate) struct EvictionTracker {
    overlay: Arc<DashMap<TableKey, TableValue>>,
    last_touched: Arc<DashMap<TableKey, BlockNum>>,
    state: Arc<EvictionState>,
}

impl EvictionTracker {
    /// Creates a new tracker that operates on the given shared maps.
    pub fn new(
        overlay: Arc<DashMap<TableKey, TableValue>>,
        last_touched: Arc<DashMap<TableKey, BlockNum>>,
    ) -> Self {
        Self {
            overlay,
            last_touched,
            state: Arc::new(EvictionState::default()),
        }
    }

    // ------------------------------------------------------------------
    // Hot-path helpers (called on every cache access)
    // ------------------------------------------------------------------

    /// Records that `key` was accessed in the current block.
    ///
    /// The fast path is fully lock-free: an atomic load of `current_block`
    /// followed by a `DashMap` lookup in `last_touched`. A `Mutex` is only
    /// acquired for the brief `Vec::push` when the key has not yet been
    /// touched in this block.
    pub fn touch_key(&self, key: &TableKey) {
        let block = self.state.current_block.load(Ordering::Acquire);
        if block == NO_ACTIVE_BLOCK {
            return;
        }

        if self.last_touched.get(key).is_none_or(|lt| *lt != block) {
            if let Ok(mut keys) = self.state.current_keys.lock() {
                keys.push(key.clone());
            } else {
                error!(
                    target = "overlay",
                    ?key,
                    "eviction current_keys lock poisoned in touch_key"
                );
                return;
            }
        }
        self.last_touched.insert(key.clone(), block);
    }

    /// Inserts a value into the overlay and records a touch.
    pub fn insert_with_touch(&self, key: &TableKey, value: TableValue) {
        self.overlay.insert(key.clone(), value);
        self.touch_key(key);
    }

    // ------------------------------------------------------------------
    // Block lifecycle (called once per block)
    // ------------------------------------------------------------------

    /// Sets the current block for touch tracking.
    pub fn set_current_block(&self, block: U256) {
        if let Err(error) = self.try_set_current_block(block) {
            error!(
                target = "overlay",
                ?error,
                %block,
                "failed to set current block for overlay eviction tracking"
            );
        }
    }

    /// Sets the current block for touch tracking.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayEvictionError::LockPoisoned`] if a mutex is poisoned.
    pub fn try_set_current_block(&self, block: U256) -> Result<(), OverlayEvictionError> {
        let block = block.saturating_to::<u64>();
        let previous = self.state.current_block.load(Ordering::Acquire);

        if previous != NO_ACTIVE_BLOCK && previous != block {
            // Switching blocks without explicit commit — carry over touched keys
            // as a committed delta so entries remain evictable.
            let previous_keys = {
                let mut keys = self
                    .state
                    .current_keys
                    .lock()
                    .map_err(|_| OverlayEvictionError::LockPoisoned)?;
                std::mem::take(&mut *keys)
            };
            if !previous_keys.is_empty() {
                let mut commit = self
                    .state
                    .commit_state
                    .lock()
                    .map_err(|_| OverlayEvictionError::LockPoisoned)?;
                commit.deltas.push_back(BlockDelta {
                    block: previous,
                    keys: previous_keys,
                });
            }
        }

        self.state.current_block.store(block, Ordering::Release);
        Ok(())
    }

    /// Sets the retention window (number of committed blocks to keep).
    pub fn set_max_committed_blocks(&self, window: usize) {
        if let Err(error) = self.try_set_max_committed_blocks(window) {
            error!(
                target = "overlay",
                ?error,
                window,
                "failed to set max committed overlay blocks"
            );
        }
    }

    /// Sets the retention window.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayEvictionError::LockPoisoned`] if the commit-state
    /// mutex is poisoned.
    pub fn try_set_max_committed_blocks(&self, window: usize) -> Result<(), OverlayEvictionError> {
        let mut commit = self
            .state
            .commit_state
            .lock()
            .map_err(|_| OverlayEvictionError::LockPoisoned)?;
        commit.max_committed_blocks = Some(window);
        Ok(())
    }

    /// Commits the current block and evicts stale blocks beyond the retention
    /// window.
    #[must_use]
    pub fn commit_block(&self, block: U256) -> OverlayEvictionOutcome {
        match self.try_commit_block(block) {
            Ok(outcome) => outcome,
            Err(error) => {
                error!(
                    target = "overlay",
                    ?error,
                    %block,
                    "failed to commit overlay block eviction boundary"
                );
                OverlayEvictionOutcome::default()
            }
        }
    }

    /// Commits the current block and evicts stale blocks beyond the retention
    /// window.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayEvictionError::LockPoisoned`] if a mutex is poisoned.
    pub fn try_commit_block(
        &self,
        block: U256,
    ) -> Result<OverlayEvictionOutcome, OverlayEvictionError> {
        let block = block.saturating_to::<u64>();

        // 1. Drain current_keys under its own short lock.
        let committed_keys = {
            let current = self.state.current_block.load(Ordering::Acquire);
            if current == block {
                let mut keys = self
                    .state
                    .current_keys
                    .lock()
                    .map_err(|_| OverlayEvictionError::LockPoisoned)?;
                self.state
                    .current_block
                    .store(NO_ACTIVE_BLOCK, Ordering::Release);
                std::mem::take(&mut *keys)
            } else {
                debug_assert!(
                    current == NO_ACTIVE_BLOCK,
                    "commit_block called for a different block than current touch-tracking block"
                );
                Vec::new()
            }
        };

        // 2. Acquire commit_state for delta management + eviction.
        let mut commit = self
            .state
            .commit_state
            .lock()
            .map_err(|_| OverlayEvictionError::LockPoisoned)?;

        let Some(window) = commit.max_committed_blocks else {
            commit.deltas.clear();
            return Ok(OverlayEvictionOutcome::default());
        };

        commit.deltas.push_back(BlockDelta {
            block,
            keys: committed_keys,
        });

        let mut latest_evicted_block = None;
        while commit.deltas.len() > window {
            let Some(delta) = commit.deltas.pop_front() else {
                break;
            };
            let mut removed_at_least_one = false;
            for key in delta.keys {
                let should_remove = self
                    .last_touched
                    .get(&key)
                    .is_some_and(|lt| *lt == delta.block);

                if should_remove {
                    self.overlay.remove(&key);
                    self.last_touched.remove(&key);
                    removed_at_least_one = true;
                }
            }

            if removed_at_least_one {
                latest_evicted_block = Some(delta.block);
            }
        }

        Ok(OverlayEvictionOutcome {
            min_required_synced_block: latest_evicted_block.map(U256::from),
        })
    }

    /// Clears all eviction tracking state while preserving the retention window.
    ///
    /// # Errors
    ///
    /// Returns [`OverlayEvictionError::LockPoisoned`] if a mutex is poisoned.
    pub fn invalidate_all(&self) -> Result<(), OverlayEvictionError> {
        self.last_touched.clear();
        self.state
            .current_block
            .store(NO_ACTIVE_BLOCK, Ordering::Release);
        {
            let mut keys = self
                .state
                .current_keys
                .lock()
                .map_err(|_| OverlayEvictionError::LockPoisoned)?;
            keys.clear();
        }
        {
            let mut commit = self
                .state
                .commit_state
                .lock()
                .map_err(|_| OverlayEvictionError::LockPoisoned)?;
            commit.deltas.clear();
            // max_committed_blocks is intentionally preserved
        }
        Ok(())
    }
}
