use revm::context::BlockEnv;
use std::sync::{
    Arc,
    RwLock,
};

/// Block context that maintains current block information for tracing across all requests
#[derive(Debug, Clone, Default)]
pub struct BlockContext {
    current_head: Arc<RwLock<Option<u64>>>,
}

impl BlockContext {
    pub fn new() -> Self {
        Self {
            current_head: Arc::new(RwLock::new(None)),
        }
    }

    /// Update block context with a new block number
    pub fn update(&self, block_number: u64) {
        match self.current_head.write() {
            Ok(mut guard) => {
                *guard = Some(block_number);
            }
            Err(e) => {
                tracing::error!(error = ?e, "Failed to acquire write lock for block context");
            }
        }
    }

    /// Get the current head
    pub fn current_head(&self) -> Option<u64> {
        match self.current_head.read() {
            Ok(guard) => *guard,
            Err(e) => {
                tracing::error!(error = ?e, "Failed to acquire read lock for block context");
                None
            }
        }
    }
}
