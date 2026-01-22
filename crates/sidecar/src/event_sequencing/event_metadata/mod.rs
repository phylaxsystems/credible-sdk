#[cfg(test)]
mod tests;

use crate::engine::queue::TxQueueContents;
use alloy::primitives::{
    TxHash,
    U256,
};
use std::hash::{
    DefaultHasher,
    Hash,
    Hasher,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompleteEventMetadata {
    NewIteration {
        block_number: U256,
        iteration_id: u64,
    },
    Transaction {
        block_number: U256,
        iteration_id: u64,
        index: u64,
        tx_hash: TxHash,
        prev_tx_hash: Option<TxHash>,
    },
    Reorg {
        block_number: U256,
        iteration_id: u64,
        tx_hash: TxHash,
        index: u64,
        /// Transaction hashes being reorged. Length determines the reorg depth.
        tx_hashes: Vec<TxHash>,
    },
    CommitHead {
        block_number: U256,
        selected_iteration_id: u64,
        n_transactions: u64,
        prev_tx_hash: Option<TxHash>,
    },
}

impl Hash for CompleteEventMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::NewIteration {
                block_number,
                iteration_id,
            } => {
                0u8.hash(state);
                block_number.hash(state);
                iteration_id.hash(state);
            }
            Self::Transaction {
                block_number,
                iteration_id,
                index,
                tx_hash,
                prev_tx_hash,
            } => {
                1u8.hash(state);
                block_number.hash(state);
                iteration_id.hash(state);
                index.hash(state);
                tx_hash.hash(state);
                prev_tx_hash.hash(state);
            }
            Self::Reorg {
                block_number,
                iteration_id,
                tx_hash,
                index,
                tx_hashes,
            } => {
                2u8.hash(state);
                block_number.hash(state);
                iteration_id.hash(state);
                tx_hash.hash(state);
                index.hash(state);
                // Hash the length and each element
                tx_hashes.len().hash(state);
                for h in tx_hashes {
                    h.hash(state);
                }
            }
            Self::CommitHead {
                block_number,
                selected_iteration_id,
                n_transactions,
                prev_tx_hash,
            } => {
                3u8.hash(state);
                block_number.hash(state);
                selected_iteration_id.hash(state);
                n_transactions.hash(state);
                prev_tx_hash.hash(state);
            }
        }
    }
}

impl From<EventMetadata> for CompleteEventMetadata {
    fn from(value: EventMetadata) -> Self {
        match value {
            EventMetadata::NewIteration {
                block_number,
                iteration_id,
            } => {
                Self::NewIteration {
                    block_number,
                    iteration_id,
                }
            }
            EventMetadata::Transaction {
                block_number,
                iteration_id,
                index,
                tx_hash,
                prev_tx_hash,
            } => {
                Self::Transaction {
                    block_number,
                    iteration_id,
                    index,
                    tx_hash,
                    prev_tx_hash,
                }
            }
            EventMetadata::Reorg {
                block_number,
                iteration_id,
                tx_hash,
                index,
                tx_hashes,
            } => {
                Self::Reorg {
                    block_number,
                    iteration_id,
                    tx_hash,
                    index,
                    tx_hashes,
                }
            }
            EventMetadata::CommitHead {
                block_number,
                selected_iteration_id,
                n_transactions,
                prev_tx_hash,
            } => {
                Self::CommitHead {
                    block_number,
                    selected_iteration_id,
                    n_transactions,
                    prev_tx_hash,
                }
            }
        }
    }
}

impl From<&CompleteEventMetadata> for EventMetadata {
    fn from(value: &CompleteEventMetadata) -> Self {
        match value {
            CompleteEventMetadata::NewIteration {
                block_number,
                iteration_id,
            } => {
                Self::NewIteration {
                    block_number: *block_number,
                    iteration_id: *iteration_id,
                }
            }
            CompleteEventMetadata::CommitHead {
                block_number,
                selected_iteration_id,
                n_transactions,
                prev_tx_hash,
            } => {
                Self::CommitHead {
                    block_number: *block_number,
                    selected_iteration_id: *selected_iteration_id,
                    n_transactions: *n_transactions,
                    prev_tx_hash: *prev_tx_hash,
                }
            }
            CompleteEventMetadata::Reorg {
                tx_hash,
                block_number,
                iteration_id,
                index,
                tx_hashes,
            } => {
                Self::Reorg {
                    tx_hash: *tx_hash,
                    block_number: *block_number,
                    iteration_id: *iteration_id,
                    index: *index,
                    tx_hashes: tx_hashes.clone(),
                }
            }
            CompleteEventMetadata::Transaction {
                tx_hash,
                block_number,
                iteration_id,
                index,
                prev_tx_hash,
            } => {
                Self::Transaction {
                    tx_hash: *tx_hash,
                    block_number: *block_number,
                    iteration_id: *iteration_id,
                    index: *index,
                    prev_tx_hash: *prev_tx_hash,
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum EventMetadata {
    NewIteration {
        block_number: U256,
        iteration_id: u64,
    },
    Transaction {
        block_number: U256,
        iteration_id: u64,
        index: u64,
        tx_hash: TxHash,
        prev_tx_hash: Option<TxHash>,
    },
    Reorg {
        block_number: U256,
        iteration_id: u64,
        tx_hash: TxHash,
        index: u64,
        /// Transaction hashes being reorged. Length determines the reorg depth.
        tx_hashes: Vec<TxHash>,
    },
    CommitHead {
        block_number: U256,
        selected_iteration_id: u64,
        n_transactions: u64,
        prev_tx_hash: Option<TxHash>,
    },
}

impl EventMetadata {
    pub fn prev_tx_hash(&self) -> Option<TxHash> {
        match self {
            Self::Transaction { prev_tx_hash, .. } | Self::CommitHead { prev_tx_hash, .. } => {
                *prev_tx_hash
            }
            _ => None,
        }
    }

    pub fn tx_hash(&self) -> Option<TxHash> {
        match self {
            Self::Transaction { tx_hash, .. } | Self::Reorg { tx_hash, .. } => Some(*tx_hash),
            _ => None,
        }
    }

    pub fn block_number(&self) -> U256 {
        match self {
            Self::NewIteration { block_number, .. }
            | Self::Transaction { block_number, .. }
            | Self::Reorg { block_number, .. }
            | Self::CommitHead { block_number, .. } => *block_number,
        }
    }

    pub fn is_new_iteration(&self) -> bool {
        matches!(self, Self::NewIteration { .. })
    }

    pub fn is_reorg(&self) -> bool {
        matches!(self, Self::Reorg { .. })
    }

    pub fn is_commit_head(&self) -> bool {
        matches!(self, Self::CommitHead { .. })
    }

    pub fn is_transaction(&self) -> bool {
        matches!(self, Self::Transaction { .. })
    }

    /// Calculates what should be the previous / expected event for a current event.
    /// For example, if the current event is a transaction index 2, the previous event is the
    /// previous transaction (index 1).
    /// If the current event is a commit head, the previous event is the last transaction in the chain.
    /// Returns `None` if the current event is the first event in the chain.
    pub fn calculate_previous_event(&self) -> Option<EventMetadata> {
        match self {
            EventMetadata::Transaction {
                block_number,
                iteration_id,
                index,
                ..
            } => {
                if *index == 0 {
                    Some(EventMetadata::NewIteration {
                        block_number: *block_number,
                        iteration_id: *iteration_id,
                    })
                } else {
                    Some(EventMetadata::Transaction {
                        block_number: *block_number,
                        iteration_id: *iteration_id,
                        index: *index - 1,
                        tx_hash: TxHash::default(),
                        prev_tx_hash: None,
                    })
                }
            }
            EventMetadata::Reorg {
                block_number,
                tx_hash,
                index,
                iteration_id,
                tx_hashes,
            } => {
                // The depth is derived from tx_hashes.len()
                // For a depth-N reorg at index I, we remove transactions from
                // index (I - N + 1) to I inclusive. The "previous event" (what we
                // depend on) is the transaction at index (I - N), which is the last
                // transaction that will remain after the reorg.
                //
                // Example: depth=2 reorg at index=3 removes txs at indices 2 and 3,
                // leaving tx at index 1 as the new tip. Previous event = tx[1].
                //
                // Edge case: if depth > index (e.g., depth=3 at index=1), we're
                // reorging back to before any transactions, so previous = NewIteration.
                let depth = tx_hashes.len() as u64;
                if depth > *index {
                    Some(EventMetadata::NewIteration {
                        block_number: *block_number,
                        iteration_id: *iteration_id,
                    })
                } else {
                    let prev_index = index.saturating_sub(depth);
                    Some(EventMetadata::Transaction {
                        block_number: *block_number,
                        iteration_id: *iteration_id,
                        tx_hash: *tx_hash,
                        index: prev_index,
                        prev_tx_hash: None,
                    })
                }
            }
            EventMetadata::CommitHead {
                block_number,
                selected_iteration_id,
                n_transactions,
                prev_tx_hash,
            } => {
                if *n_transactions == 0 {
                    Some(EventMetadata::NewIteration {
                        block_number: *block_number,
                        iteration_id: *selected_iteration_id,
                    })
                } else if let Some(prev_tx_hash) = prev_tx_hash {
                    Some(EventMetadata::Transaction {
                        block_number: *block_number,
                        iteration_id: *selected_iteration_id,
                        tx_hash: *prev_tx_hash,
                        index: n_transactions - 1,
                        prev_tx_hash: None,
                    })
                } else {
                    Some(EventMetadata::NewIteration {
                        block_number: *block_number,
                        iteration_id: *selected_iteration_id,
                    })
                }
            }
            EventMetadata::NewIteration {
                block_number,
                iteration_id,
            } => {
                if *block_number == 0 {
                    None
                } else {
                    Some(EventMetadata::CommitHead {
                        block_number: block_number - U256::from(1),
                        selected_iteration_id: 0,
                        n_transactions: 0,
                        prev_tx_hash: None,
                    })
                }
            }
        }
    }

    pub fn cancel_each_other(&self, other: &Self) -> bool {
        match (self, other) {
            (
                EventMetadata::Reorg {
                    block_number,
                    iteration_id,
                    index,
                    tx_hash,
                    ..
                },
                EventMetadata::Transaction {
                    block_number: other_block_number,
                    iteration_id: other_iteration_id,
                    index: other_index,
                    tx_hash: other_tx_hash,
                    ..
                },
            )
            | (
                EventMetadata::Transaction {
                    block_number: other_block_number,
                    tx_hash: other_tx_hash,
                    index: other_index,
                    iteration_id: other_iteration_id,
                    ..
                },
                EventMetadata::Reorg {
                    block_number,
                    tx_hash,
                    index,
                    iteration_id,
                    ..
                },
            ) => {
                block_number == other_block_number
                    && iteration_id == other_iteration_id
                    && index == other_index
                    && tx_hash == other_tx_hash
            }
            _ => false,
        }
    }
}

/// Custom implementation of Hash so we do not depend on `tx_hash`, `selected_iteration_id`,
/// `n_transactions` or `prev_tx_hash`
/// So for example, a `Transaction` variant with the same `block_number`, `iteration_id` and index will
/// hash to the same value
impl Hash for EventMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            EventMetadata::NewIteration {
                block_number,
                iteration_id,
            } => {
                "newIteration".hash(state);
                block_number.hash(state);
                iteration_id.hash(state);
            }
            EventMetadata::Transaction {
                block_number,
                iteration_id,
                index,
                ..
            } => {
                "transaction".hash(state);
                block_number.hash(state);
                iteration_id.hash(state);
                index.hash(state);
            }
            EventMetadata::Reorg {
                block_number,
                index,
                iteration_id,
                tx_hashes,
                ..
            } => {
                "reorg".hash(state);
                block_number.hash(state);
                iteration_id.hash(state);
                index.hash(state);
                // Hash the depth (length of tx_hashes)
                tx_hashes.len().hash(state);
            }
            EventMetadata::CommitHead {
                block_number,
                selected_iteration_id,
                prev_tx_hash,
                n_transactions,
            } => {
                "commitHead".hash(state);
                block_number.hash(state);
            }
        }
    }
}

impl PartialEq for EventMetadata {
    fn eq(&self, other: &Self) -> bool {
        // Helper function to compute hash
        fn compute_hash<T: Hash>(value: &T) -> u64 {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        }

        compute_hash(self) == compute_hash(other)
    }
}

impl Eq for EventMetadata {}

impl From<&TxQueueContents> for EventMetadata {
    fn from(value: &TxQueueContents) -> Self {
        match value {
            TxQueueContents::Tx(queue) => {
                Self::Transaction {
                    block_number: queue.tx_execution_id.block_number,
                    iteration_id: queue.tx_execution_id.iteration_id,
                    index: queue.tx_execution_id.index,
                    tx_hash: queue.tx_execution_id.tx_hash,
                    prev_tx_hash: queue.prev_tx_hash,
                }
            }
            TxQueueContents::Reorg(queue) => {
                Self::Reorg {
                    block_number: queue.tx_execution_id.block_number,
                    iteration_id: queue.tx_execution_id.iteration_id,
                    tx_hash: queue.tx_execution_id.tx_hash,
                    index: queue.tx_execution_id.index,
                    tx_hashes: queue.tx_hashes.clone(),
                }
            }
            TxQueueContents::CommitHead(queue) => {
                Self::CommitHead {
                    block_number: queue.block_number,
                    selected_iteration_id: queue.selected_iteration_id,
                    n_transactions: queue.n_transactions,
                    prev_tx_hash: queue.last_tx_hash,
                }
            }
            TxQueueContents::NewIteration(queue) => {
                Self::NewIteration {
                    block_number: queue.block_env.number,
                    iteration_id: queue.iteration_id,
                }
            }
        }
    }
}
