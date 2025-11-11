#[cfg(test)]
mod tests;

use crate::engine::queue::TxQueueContents;
use alloy::primitives::TxHash;
use std::hash::{
    Hash,
    Hasher,
};

#[derive(Clone, Debug)]
pub enum EventMetadata {
    NewIteration {
        block_number: u64,
        iteration_id: u64,
    },
    Transaction {
        block_number: u64,
        iteration_id: u64,
        index: u64,
        tx_hash: TxHash,
        prev_tx_hash: Option<TxHash>,
    },
    Reorg {
        block_number: u64,
        iteration_id: u64,
        tx_hash: TxHash,
        index: u64,
    },
    CommitHead {
        block_number: u64,
        selected_iteration_id: u64,
        n_transactions: u64,
        prev_tx_hash: Option<TxHash>,
    },
}

impl EventMetadata {
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
            } => {
                Some(EventMetadata::Transaction {
                    block_number: *block_number,
                    iteration_id: *iteration_id,
                    tx_hash: *tx_hash,
                    index: *index,
                    prev_tx_hash: None,
                })
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
                        block_number: block_number - 1,
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
                ..
            } => {
                "reorg".hash(state);
                block_number.hash(state);
                iteration_id.hash(state);
                index.hash(state);
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

impl From<&TxQueueContents> for EventMetadata {
    fn from(value: &TxQueueContents) -> Self {
        match value {
            TxQueueContents::Tx(queue, _) => {
                Self::Transaction {
                    block_number: queue.tx_execution_id.block_number,
                    iteration_id: queue.tx_execution_id.iteration_id,
                    index: 0, // FIXME: Propagate the new field
                    tx_hash: queue.tx_execution_id.tx_hash,
                    prev_tx_hash: None, // FIXME: Propagate the new field
                }
            }
            TxQueueContents::Reorg(queue, _) => {
                Self::Reorg {
                    block_number: queue.block_number,
                    iteration_id: queue.iteration_id,
                    tx_hash: queue.tx_hash,
                    index: 0, // FIXME: Propagate the new field
                }
            }
            TxQueueContents::CommitHead(queue, _) => {
                Self::CommitHead {
                    block_number: queue.block_number,
                    selected_iteration_id: queue.selected_iteration_id,
                    n_transactions: queue.n_transactions,
                    prev_tx_hash: queue.last_tx_hash,
                }
            }
            TxQueueContents::NewIteration(queue, _) => {
                Self::NewIteration {
                    block_number: queue.block_env.number,
                    iteration_id: queue.iteration_id,
                }
            }
        }
    }
}
