//! Consistent state data layer for the sidecar
//!
//! This module provides a multi-layered state management system that ensures
//! consistency even when blocks are missed or arrive out of order.

pub mod cache;
pub mod consistency;
pub mod store;
pub mod sync;

use crate::primitives::{
    AccountInfo,
    Address,
    B256,
    Bytecode,
    U256,
};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Errors that can occur in state management
#[derive(thiserror::Error, Debug)]
pub enum StateError {
    #[error("State inconsistency detected: expected block {expected}, got {actual}")]
    InconsistentState { expected: u64, actual: u64 },
    #[error("Block {0} not found in state store")]
    BlockNotFound(u64),
    #[error("State store error: {0}")]
    StoreError(String),
    #[error("RPC fallback failed: {0}")]
    RpcFallbackFailed(String),
}

/// Status of state consistency
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConsistencyStatus {
    /// State is up to date
    Fresh,
    /// State is slightly behind but usable
    Stale,
    /// State is too far behind, needs resync
    Invalid,
    /// Unknown state - needs verification
    Unknown,
}

/// A state change applied by a transaction
#[derive(Debug, Clone)]
pub struct StateChange {
    pub block_number: u64,
    pub tx_index: u32,
    pub tx_hash: B256,
    pub address: Address,
    pub change_type: ChangeType,
    pub before: Option<StateValue>,
    pub after: StateValue,
}

#[derive(Debug, Clone)]
pub enum ChangeType {
    AccountBalance,
    AccountNonce,
    AccountCode,
    Storage { slot: U256 },
    AccountCreated,
    AccountDestroyed,
}

#[derive(Debug, Clone)]
pub enum StateValue {
    Balance(U256),
    Nonce(u64),
    Code(Bytecode),
    Storage(U256),
}

/// Main state manager that coordinates all layers
pub struct StateManager {
    /// High-performance local state store
    store: Arc<store::StateStore>,
    /// Consistency checker and cache invalidator
    consistency: Arc<consistency::ConsistencyManager>,
    /// Background sync service
    sync_service: Arc<sync::StateSyncService>,
    /// In-memory cache layer
    cache: Arc<RwLock<cache::StateCache>>,
}

impl StateManager {
    pub async fn new(config: StateConfig) -> Result<Self, StateError> {
        let store = Arc::new(store::StateStore::new(&config.db_path).await?);
        let consistency = Arc::new(consistency::ConsistencyManager::new(
            config.consistency_config,
        ));
        let sync_service =
            Arc::new(sync::StateSyncService::new(Arc::clone(&store), config.besu_rpc_url).await?);
        let cache = Arc::new(RwLock::new(cache::StateCache::new(config.cache_size)));

        Ok(Self {
            store,
            consistency,
            sync_service,
            cache,
        })
    }

    /// Get account info with automatic consistency checking
    pub async fn get_account(
        &self,
        address: Address,
        block: Option<u64>,
    ) -> Result<Option<AccountInfo>, StateError> {
        let target_block = block.unwrap_or(self.get_head_block().await?);

        // 1. Check consistency first
        match self.consistency.check_consistency(target_block).await {
            ConsistencyStatus::Fresh => {
                // Try cache first
                if let Some(account) = self.cache.read().await.get_account(address, target_block) {
                    return Ok(Some(account));
                }

                // Fall back to store
                self.store.get_account(address, Some(target_block)).await
            }
            ConsistencyStatus::Stale => {
                // Use store directly, skip cache
                self.store.get_account(address, Some(target_block)).await
            }
            ConsistencyStatus::Invalid => {
                // Invalidate cache and trigger resync
                self.cache.write().await.invalidate_all();
                self.sync_service.trigger_resync(target_block).await?;

                // Use RPC fallback while resyncing
                self.sync_service
                    .get_account_via_rpc(address, target_block)
                    .await
            }
            ConsistencyStatus::Unknown => {
                // Verify consistency and retry
                self.consistency.verify_consistency().await?;
                self.get_account(address, block).await
            }
        }
    }

    /// Apply a batch of state changes atomically
    pub async fn apply_state_changes(&self, changes: Vec<StateChange>) -> Result<(), StateError> {
        // 1. Validate changes are sequential
        self.validate_sequential(changes)?;

        // 2. Apply to store first (WAL for durability)
        self.store.apply_changes_atomic(changes.clone()).await?;

        // 3. Update cache
        self.cache.write().await.apply_changes(changes);

        // 4. Update consistency tracker
        if let Some(latest_change) = changes.last() {
            self.consistency
                .update_head(latest_change.block_number)
                .await;
        }

        Ok(())
    }

    async fn get_head_block(&self) -> Result<u64, StateError> {
        self.consistency.get_current_head().await
    }

    fn validate_sequential(&self, changes: &[StateChange]) -> Result<(), StateError> {
        // Ensure changes are in block order
        for window in changes.windows(2) {
            if window[0].block_number > window[1].block_number {
                return Err(StateError::InconsistentState {
                    expected: window[0].block_number,
                    actual: window[1].block_number,
                });
            }
        }
        Ok(())
    }
}

/// Configuration for state management
#[derive(Debug, Clone)]
pub struct StateConfig {
    pub db_path: String,
    pub besu_rpc_url: String,
    pub cache_size: usize,
    pub consistency_config: consistency::ConsistencyConfig,
    pub sync_config: sync::SyncConfig,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            db_path: "./sidecar_state.db".to_string(),
            besu_rpc_url: "http://localhost:8545".to_string(),
            cache_size: 1_000_000, // 1M entries
            consistency_config: consistency::ConsistencyConfig::default(),
            sync_config: sync::SyncConfig::default(),
        }
    }
}
