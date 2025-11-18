#![allow(clippy::needless_range_loop)]
use super::*;
use crate::event_sequencing::event_metadata::EventMetadata;
use alloy::primitives::TxHash;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{
        Hash,
        Hasher,
    },
};

// Helper function to create a test tx_hash
fn test_hash(value: u8) -> TxHash {
    let mut bytes = [0u8; 32];
    bytes[0] = value;
    TxHash::from(bytes)
}

// Helper to calculate hash value
fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

// Helper to recursively collect all previous events
fn collect_event_chain(event: &EventMetadata) -> Vec<EventMetadata> {
    let mut chain = vec![];
    let mut current = event.calculate_previous_event();

    while let Some(prev) = current {
        chain.push(prev.clone());
        current = prev.calculate_previous_event();
    }

    chain
}

// Helper to collect the event chain up to a limit
fn collect_event_chain_limited(event: &EventMetadata, limit: usize) -> Vec<EventMetadata> {
    let mut chain = vec![];
    let mut current = event.calculate_previous_event();

    while let Some(prev) = current {
        chain.push(prev.clone());
        if chain.len() >= limit {
            break;
        }
        current = prev.calculate_previous_event();
    }

    chain
}

#[test]
fn test_transaction_index_0_returns_new_iteration() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 0,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let prev = tx.calculate_previous_event().unwrap();
    match prev {
        EventMetadata::NewIteration {
            block_number,
            iteration_id,
        } => {
            assert_eq!(block_number, 100);
            assert_eq!(iteration_id, 5);
        }
        _ => panic!("Expected NewIteration"),
    }
}

#[test]
fn test_transaction_index_greater_than_0_returns_previous_transaction() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 3,
        tx_hash: test_hash(1),
        prev_tx_hash: Some(test_hash(2)),
    };

    let prev = tx.calculate_previous_event().unwrap();
    match prev {
        EventMetadata::Transaction {
            block_number,
            iteration_id,
            index,
            ..
        } => {
            assert_eq!(block_number, 100);
            assert_eq!(iteration_id, 5);
            assert_eq!(index, 2);
        }
        _ => panic!("Expected Transaction"),
    }
}

#[test]
fn test_reorg_returns_transaction() {
    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 2,
    };

    let prev = reorg.calculate_previous_event().unwrap();
    match prev {
        EventMetadata::Transaction {
            block_number,
            iteration_id,
            index,
            tx_hash,
            ..
        } => {
            assert_eq!(block_number, 100);
            assert_eq!(iteration_id, 5);
            assert_eq!(index, 1);
            assert_eq!(tx_hash, test_hash(1));
        }
        _ => panic!("Expected Transaction"),
    }
}

#[test]
fn test_commit_head_with_zero_transactions_returns_new_iteration() {
    let commit = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 0,
        prev_tx_hash: None,
    };

    let prev = commit.calculate_previous_event().unwrap();
    match prev {
        EventMetadata::NewIteration {
            block_number,
            iteration_id,
        } => {
            assert_eq!(block_number, 100);
            assert_eq!(iteration_id, 5);
        }
        _ => panic!("Expected NewIteration"),
    }
}

#[test]
fn test_commit_head_with_transactions_returns_last_transaction() {
    let commit = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(9)),
    };

    let prev = commit.calculate_previous_event().unwrap();
    match prev {
        EventMetadata::Transaction {
            block_number,
            iteration_id,
            index,
            tx_hash,
            ..
        } => {
            assert_eq!(block_number, 100);
            assert_eq!(iteration_id, 5);
            assert_eq!(index, 9);
            assert_eq!(tx_hash, test_hash(9));
        }
        _ => panic!("Expected Transaction"),
    }
}

#[test]
fn test_new_iteration_at_block_0_returns_none() {
    let new_iter = EventMetadata::NewIteration {
        block_number: 0,
        iteration_id: 5,
    };

    assert!(new_iter.calculate_previous_event().is_none());
}

#[test]
fn test_new_iteration_returns_previous_block_commit_head() {
    let new_iter = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    let prev = new_iter.calculate_previous_event().unwrap();
    match prev {
        EventMetadata::CommitHead {
            block_number,
            selected_iteration_id,
            n_transactions,
            prev_tx_hash,
        } => {
            assert_eq!(block_number, 99);
            assert_eq!(selected_iteration_id, 0);
            assert_eq!(n_transactions, 0);
            assert_eq!(prev_tx_hash, None);
        }
        _ => panic!("Expected CommitHead"),
    }
}

#[test]
fn test_new_iteration_at_block_1_returns_commit_head_at_block_0() {
    let new_iter = EventMetadata::NewIteration {
        block_number: 1,
        iteration_id: 3,
    };

    let prev = new_iter.calculate_previous_event().unwrap();
    match prev {
        EventMetadata::CommitHead { block_number, .. } => {
            assert_eq!(block_number, 0);
        }
        _ => panic!("Expected CommitHead"),
    }
}

#[test]
fn test_recursive_chain_from_commit_head_to_new_iteration() {
    let commit = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 5,
        prev_tx_hash: Some(test_hash(4)),
    };

    let chain = collect_event_chain_limited(&commit, 10);

    // Should have at least 6 events: 5 transactions + 1 new_iteration
    assert!(chain.len() >= 6);

    // Verify the chain goes: Transaction(4) -> Transaction(3) -> ... -> Transaction(0) -> NewIteration
    for i in 0..5 {
        match &chain[i] {
            EventMetadata::Transaction {
                index,
                block_number,
                ..
            } => {
                assert_eq!(*index, 4 - i as u64);
                assert_eq!(*block_number, 100);
            }
            _ => panic!("Expected Transaction at position {i}"),
        }
    }

    match &chain[5] {
        EventMetadata::NewIteration { block_number, .. } => {
            assert_eq!(*block_number, 100);
        }
        _ => panic!("Expected NewIteration at position 5"),
    }
}

#[test]
fn test_recursive_chain_from_transaction_to_new_iteration() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 3,
        tx_hash: test_hash(3),
        prev_tx_hash: Some(test_hash(2)),
    };

    let chain = collect_event_chain_limited(&tx, 10);

    // Should have at least 4 events: Transaction(2), Transaction(1), Transaction(0), NewIteration
    assert!(chain.len() >= 4);

    for i in 0..3 {
        match &chain[i] {
            EventMetadata::Transaction { index, .. } => {
                assert_eq!(*index, 2 - i as u64);
            }
            _ => panic!("Expected Transaction at position {i}"),
        }
    }

    match &chain[3] {
        EventMetadata::NewIteration { .. } => {}
        _ => panic!("Expected NewIteration at end of chain"),
    }
}

#[test]
fn test_recursive_chain_from_reorg_to_new_iteration() {
    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 2,
    };

    let chain = collect_event_chain_limited(&reorg, 10);

    // Should have Transaction(2) -> Transaction(1) -> Transaction(0) -> NewIteration
    assert!(chain.len() >= 4);

    // First should be transaction at index 2
    match &chain[0] {
        EventMetadata::Transaction { index, .. } => {
            assert_eq!(*index, 1);
        }
        _ => panic!("Expected Transaction at position 0"),
    }
}

#[test]
fn test_recursive_chain_from_commit_head_with_zero_transactions() {
    let commit = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 0,
        prev_tx_hash: None,
    };

    let chain = collect_event_chain_limited(&commit, 10);

    // Should have at least NewIteration
    assert!(!chain.is_empty());

    match &chain[0] {
        EventMetadata::NewIteration { block_number, .. } => {
            assert_eq!(*block_number, 100);
        }
        _ => panic!("Expected NewIteration"),
    }
}

#[test]
fn test_recursive_chain_crosses_block_boundary() {
    let new_iter = EventMetadata::NewIteration {
        block_number: 5,
        iteration_id: 2,
    };

    let chain = collect_event_chain_limited(&new_iter, 10);

    // Should have CommitHead(4) -> NewIteration(4) -> CommitHead(3) -> ...
    assert!(!chain.is_empty());

    match &chain[0] {
        EventMetadata::CommitHead { block_number, .. } => {
            assert_eq!(*block_number, 4);
        }
        _ => panic!("Expected CommitHead at previous block"),
    }

    if chain.len() >= 2 {
        match &chain[1] {
            EventMetadata::NewIteration { block_number, .. } => {
                assert_eq!(*block_number, 4);
            }
            _ => panic!("Expected NewIteration at block 4"),
        }
    }
}

#[test]
fn test_recursive_chain_reaches_block_0() {
    let new_iter = EventMetadata::NewIteration {
        block_number: 3,
        iteration_id: 0,
    };

    let chain = collect_event_chain(&new_iter);

    // Should eventually reach block 0 and stop
    // Chain: CommitHead(2) -> NewIter(2) -> CommitHead(1) -> NewIter(1) -> CommitHead(0) -> NewIter(0) -> None
    assert_eq!(chain.len(), 6);

    // Last event should be NewIteration at block 0
    match chain.last().unwrap() {
        EventMetadata::NewIteration { block_number, .. } => {
            assert_eq!(*block_number, 0);
        }
        _ => panic!("Expected NewIteration at block 0 at end of chain"),
    }
}

#[test]
fn test_recursive_chain_preserves_block_and_iteration_within_block() {
    let commit = EventMetadata::CommitHead {
        block_number: 42,
        selected_iteration_id: 7,
        n_transactions: 3,
        prev_tx_hash: Some(test_hash(2)),
    };

    let chain = collect_event_chain_limited(&commit, 4);

    // First 4 events should all be in block 42
    for i in 0..4 {
        match &chain[i] {
            EventMetadata::Transaction {
                block_number,
                iteration_id,
                ..
            }
            | EventMetadata::NewIteration {
                block_number,
                iteration_id,
            } => {
                assert_eq!(*block_number, 42);
                assert_eq!(*iteration_id, 7);
            }
            _ => {}
        }
    }
}

#[test]
fn test_reorg_and_transaction_cancel() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: Some(test_hash(2)),
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
    };

    assert!(tx.cancel_each_other(&reorg));
    assert!(reorg.cancel_each_other(&tx)); // Symmetric
}

#[test]
fn test_transaction_and_reorg_different_tx_hash_dont_cancel() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(2), // Different tx_hash
    };

    assert!(!tx.cancel_each_other(&reorg));
    assert!(!reorg.cancel_each_other(&tx));
}

#[test]
fn test_transaction_and_reorg_different_index_dont_cancel() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        index: 3, // Different index
        tx_hash: test_hash(1),
    };

    assert!(!tx.cancel_each_other(&reorg));
    assert!(!reorg.cancel_each_other(&tx));
}

#[test]
fn test_transaction_and_reorg_different_block_dont_cancel() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 101, // Different block
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
    };

    assert!(!tx.cancel_each_other(&reorg));
    assert!(!reorg.cancel_each_other(&tx));
}

#[test]
fn test_transaction_and_reorg_different_iteration_dont_cancel() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 6, // Different iteration
        index: 2,
        tx_hash: test_hash(1),
    };

    assert!(!tx.cancel_each_other(&reorg));
    assert!(!reorg.cancel_each_other(&tx));
}

#[test]
fn test_identical_transactions_dont_cancel() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: Some(test_hash(2)),
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: Some(test_hash(3)),
    };

    // Transactions don't cancel each other, only Reorg and Transaction do
    assert!(!tx1.cancel_each_other(&tx2));
    assert!(!tx2.cancel_each_other(&tx1));
}

#[test]
fn test_identical_reorgs_dont_cancel() {
    let reorg1 = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 2,
    };

    let reorg2 = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 2,
    };

    // Reorgs don't cancel each other, only Reorg and Transaction do
    assert!(!reorg1.cancel_each_other(&reorg2));
    assert!(!reorg2.cancel_each_other(&reorg1));
}

#[test]
fn test_commit_head_and_transaction_dont_cancel() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let commit = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 3,
        prev_tx_hash: None,
    };

    assert!(!tx.cancel_each_other(&commit));
    assert!(!commit.cancel_each_other(&tx));
}

#[test]
fn test_commit_head_and_reorg_dont_cancel() {
    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 2,
    };

    let commit = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 3,
        prev_tx_hash: None,
    };

    assert!(!reorg.cancel_each_other(&commit));
    assert!(!commit.cancel_each_other(&reorg));
}

#[test]
fn test_new_iteration_and_transaction_dont_cancel() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 0,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let new_iter = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    assert!(!tx.cancel_each_other(&new_iter));
    assert!(!new_iter.cancel_each_other(&tx));
}

#[test]
fn test_new_iteration_and_reorg_dont_cancel() {
    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 2,
    };

    let new_iter = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    assert!(!reorg.cancel_each_other(&new_iter));
    assert!(!new_iter.cancel_each_other(&reorg));
}

#[test]
fn test_new_iterations_dont_cancel() {
    let iter1 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    let iter2 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    assert!(!iter1.cancel_each_other(&iter2));
}

#[test]
fn test_commit_heads_dont_cancel() {
    let commit1 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(9)),
    };

    let commit2 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(9)),
    };

    assert!(!commit1.cancel_each_other(&commit2));
}

#[test]
fn test_hash_transaction_same_identity_different_tx_hash() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(99), // Different tx_hash
        prev_tx_hash: None,
    };

    assert_eq!(calculate_hash(&tx1), calculate_hash(&tx2));
}

#[test]
fn test_hash_transaction_same_identity_different_prev_tx_hash() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: Some(test_hash(10)),
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: Some(test_hash(20)), // Different prev_tx_hash
    };

    assert_eq!(calculate_hash(&tx1), calculate_hash(&tx2));
}

#[test]
fn test_hash_transaction_different_index() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 3, // Different index
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    assert_ne!(calculate_hash(&tx1), calculate_hash(&tx2));
}

#[test]
fn test_hash_transaction_different_block() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 101, // Different block
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    assert_ne!(calculate_hash(&tx1), calculate_hash(&tx2));
}

#[test]
fn test_hash_transaction_different_iteration() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 6, // Different iteration
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    assert_ne!(calculate_hash(&tx1), calculate_hash(&tx2));
}

#[test]
fn test_hash_reorg_same_identity_different_tx_hash() {
    let reorg1 = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 2,
    };

    let reorg2 = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(99), // Different tx_hash
        index: 2,
    };

    assert_eq!(calculate_hash(&reorg1), calculate_hash(&reorg2));
}

#[test]
fn test_hash_reorg_different_index() {
    let reorg1 = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 2,
    };

    let reorg2 = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 3, // Different index
    };

    assert_ne!(calculate_hash(&reorg1), calculate_hash(&reorg2));
}

#[test]
fn test_hash_new_iteration_same_identity() {
    let iter1 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    let iter2 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    assert_eq!(calculate_hash(&iter1), calculate_hash(&iter2));
}

#[test]
fn test_hash_new_iteration_different_block() {
    let iter1 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    let iter2 = EventMetadata::NewIteration {
        block_number: 101, // Different block
        iteration_id: 5,
    };

    assert_ne!(calculate_hash(&iter1), calculate_hash(&iter2));
}

#[test]
fn test_hash_new_iteration_different_iteration() {
    let iter1 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    let iter2 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 6, // Different iteration
    };

    assert_ne!(calculate_hash(&iter1), calculate_hash(&iter2));
}

#[test]
fn test_hash_commit_head_ignores_selected_iteration_id() {
    let commit1 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(9)),
    };

    let commit2 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 99, // Different selected_iteration_id
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(9)),
    };

    assert_eq!(calculate_hash(&commit1), calculate_hash(&commit2));
}

#[test]
fn test_hash_commit_head_ignores_n_transactions() {
    let commit1 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(9)),
    };

    let commit2 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 20, // Different n_transactions
        prev_tx_hash: Some(test_hash(9)),
    };

    assert_eq!(calculate_hash(&commit1), calculate_hash(&commit2));
}

#[test]
fn test_hash_commit_head_ignores_prev_tx_hash() {
    let commit1 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(9)),
    };

    let commit2 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(50)), // Different prev_tx_hash
    };

    assert_eq!(calculate_hash(&commit1), calculate_hash(&commit2));
}

#[test]
fn test_hash_commit_head_different_block() {
    let commit1 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(9)),
    };

    let commit2 = EventMetadata::CommitHead {
        block_number: 101, // Different block
        selected_iteration_id: 5,
        n_transactions: 10,
        prev_tx_hash: Some(test_hash(9)),
    };

    assert_ne!(calculate_hash(&commit1), calculate_hash(&commit2));
}

#[test]
fn test_hash_different_variants_same_fields() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 5,
        tx_hash: test_hash(1),
        index: 2,
    };

    // Different variants should hash differently even with same fields
    assert_ne!(calculate_hash(&tx), calculate_hash(&reorg));
}

#[test]
fn test_hash_transaction_and_new_iteration_different() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 0,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let new_iter = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 5,
    };

    assert_ne!(calculate_hash(&tx), calculate_hash(&new_iter));
}

#[test]
fn test_transaction_at_max_index() {
    let tx = EventMetadata::Transaction {
        block_number: u64::MAX,
        iteration_id: u64::MAX,
        index: u64::MAX,
        tx_hash: test_hash(1),
        prev_tx_hash: None,
    };

    let prev = tx.calculate_previous_event().unwrap();
    match prev {
        EventMetadata::Transaction { index, .. } => {
            assert_eq!(index, u64::MAX - 1);
        }
        _ => panic!("Expected Transaction"),
    }
}

#[test]
fn test_commit_head_with_one_transaction() {
    let commit = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 5,
        n_transactions: 1,
        prev_tx_hash: Some(test_hash(0)),
    };

    let chain = collect_event_chain_limited(&commit, 5);

    // Should have Transaction(0) -> NewIteration -> ...
    assert!(chain.len() >= 2);

    match &chain[0] {
        EventMetadata::Transaction { index, .. } => {
            assert_eq!(*index, 0);
        }
        _ => panic!("Expected Transaction at index 0"),
    }
}

#[test]
fn test_hash_consistency_with_clone() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 5,
        index: 2,
        tx_hash: test_hash(1),
        prev_tx_hash: Some(test_hash(2)),
    };

    let tx2 = tx1.clone();

    assert_eq!(calculate_hash(&tx1), calculate_hash(&tx2));
}

#[test]
fn test_transaction_equality_ignores_tx_hash() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
        prev_tx_hash: None,
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
        prev_tx_hash: Some(TxHash::random()),
    };

    assert_eq!(
        tx1, tx2,
        "Transactions with same block/iteration/index should be equal despite different tx_hash"
    );
}

#[test]
fn test_new_iteration_inequality_different_block() {
    let iter1 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 1,
    };

    let iter2 = EventMetadata::NewIteration {
        block_number: 101,
        iteration_id: 1,
    };

    assert_ne!(
        iter1, iter2,
        "NewIterations with different block_number should not be equal"
    );
}

#[test]
fn test_new_iteration_inequality_different_iteration() {
    let iter1 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 1,
    };

    let iter2 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 2,
    };

    assert_ne!(
        iter1, iter2,
        "NewIterations with different iteration_id should not be equal"
    );
}

#[test]
fn test_commit_head_inequality_different_block() {
    let commit1 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 1,
        n_transactions: 10,
        prev_tx_hash: Some(TxHash::random()),
    };

    let commit2 = EventMetadata::CommitHead {
        block_number: 101,
        selected_iteration_id: 1,
        n_transactions: 10,
        prev_tx_hash: Some(TxHash::random()),
    };

    assert_ne!(
        commit1, commit2,
        "CommitHeads with different block_number should not be equal"
    );
}

#[test]
fn test_transaction_inequality_different_index() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
        prev_tx_hash: None,
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 6,
        tx_hash: TxHash::random(),
        prev_tx_hash: None,
    };

    assert_ne!(
        tx1, tx2,
        "Transactions with different index should not be equal"
    );
}

#[test]
fn test_reorg_equality_ignores_tx_hash() {
    let reorg1 = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
    };

    let reorg2 = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
    };

    assert_eq!(
        reorg1, reorg2,
        "Reorgs with same block/iteration/index should be equal despite different tx_hash"
    );
}

#[test]
fn test_new_iteration_equality() {
    let iter1 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 1,
    };

    let iter2 = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 1,
    };

    assert_eq!(
        iter1, iter2,
        "NewIterations with same block/iteration should be equal"
    );
}

#[test]
fn test_commit_head_equality_ignores_most_fields() {
    let commit1 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 1,
        n_transactions: 10,
        prev_tx_hash: Some(TxHash::random()),
    };

    let commit2 = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 999, // Different!
        n_transactions: 5,          // Different!
        prev_tx_hash: None,         // Different!
    };

    assert_eq!(
        commit1, commit2,
        "CommitHeads with same block_number should be equal regardless of other fields"
    );
}

#[test]
fn test_different_variants_not_equal() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::default(),
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::default(),
    };

    assert_ne!(
        tx, reorg,
        "Different event types should not be equal even with same fields"
    );
}

#[test]
fn test_cancel_each_other_transaction_and_reorg() {
    let tx_hash = TxHash::random();

    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash,
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash,
    };

    assert!(
        tx.cancel_each_other(&reorg),
        "Transaction and Reorg with same identifiers should cancel"
    );
    assert!(
        reorg.cancel_each_other(&tx),
        "Should work in both directions"
    );
}

#[test]
fn test_cancel_each_other_different_tx_hash() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
    };

    assert!(
        !tx.cancel_each_other(&reorg),
        "Transaction and Reorg with different tx_hash should not cancel"
    );
}

#[test]
fn test_cancel_each_other_same_type_no_cancel() {
    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
        prev_tx_hash: None,
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
        prev_tx_hash: None,
    };

    assert!(
        !tx1.cancel_each_other(&tx2),
        "Two Transactions should not cancel each other"
    );
}

#[test]
fn test_hash_consistency() {
    use std::hash::{
        DefaultHasher,
        Hash,
        Hasher,
    };

    fn compute_hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    let tx1 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
        prev_tx_hash: None,
    };

    let tx2 = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
        prev_tx_hash: Some(TxHash::random()),
    };

    assert_eq!(
        compute_hash(&tx1),
        compute_hash(&tx2),
        "Equal events should produce same hash"
    );
}

#[test]
fn test_calculate_previous_event_transaction_first_index() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 0,
        tx_hash: TxHash::random(),
        prev_tx_hash: None,
    };

    let prev = tx.calculate_previous_event();
    assert!(prev.is_some());

    let expected = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 1,
    };
    assert_eq!(prev.unwrap(), expected);
}

#[test]
fn test_calculate_previous_event_transaction_non_zero_index() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
        prev_tx_hash: None,
    };

    let prev = tx.calculate_previous_event().unwrap();

    match prev {
        EventMetadata::Transaction {
            block_number,
            iteration_id,
            index,
            ..
        } => {
            assert_eq!(block_number, 100);
            assert_eq!(iteration_id, 1);
            assert_eq!(index, 4);
        }
        _ => panic!("Expected Transaction variant"),
    }
}

#[test]
fn test_calculate_previous_event_new_iteration_at_genesis() {
    let new_iter = EventMetadata::NewIteration {
        block_number: 0,
        iteration_id: 0,
    };

    let prev = new_iter.calculate_previous_event();
    assert!(
        prev.is_none(),
        "NewIteration at block 0 should have no previous event"
    );
}

#[test]
fn test_all_variant_combinations_not_equal() {
    let tx = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::default(),
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::default(),
    };

    let new_iter = EventMetadata::NewIteration {
        block_number: 100,
        iteration_id: 1,
    };

    let commit = EventMetadata::CommitHead {
        block_number: 100,
        selected_iteration_id: 1,
        n_transactions: 5,
        prev_tx_hash: None,
    };

    // Transaction vs others
    assert_ne!(tx, reorg, "Transaction != Reorg");
    assert_ne!(tx, new_iter, "Transaction != NewIteration");
    assert_ne!(tx, commit, "Transaction != CommitHead");

    // Reorg vs others
    assert_ne!(reorg, tx, "Reorg != Transaction");
    assert_ne!(reorg, new_iter, "Reorg != NewIteration");
    assert_ne!(reorg, commit, "Reorg != CommitHead");

    // NewIteration vs others
    assert_ne!(new_iter, tx, "NewIteration != Transaction");
    assert_ne!(new_iter, reorg, "NewIteration != Reorg");
    assert_ne!(new_iter, commit, "NewIteration != CommitHead");

    // CommitHead vs others
    assert_ne!(commit, tx, "CommitHead != Transaction");
    assert_ne!(commit, reorg, "CommitHead != Reorg");
    assert_ne!(commit, new_iter, "CommitHead != NewIteration");
}

#[test]
fn test_cancel_each_other_all_field_combinations() {
    let base_tx_hash = TxHash::random();

    // Base case - should cancel
    let tx_base = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: base_tx_hash,
        prev_tx_hash: None,
    };

    let reorg_base = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: base_tx_hash,
    };

    assert!(
        tx_base.cancel_each_other(&reorg_base),
        "Base case should cancel"
    );

    // Different block_number - should NOT cancel
    let reorg_diff_block = EventMetadata::Reorg {
        block_number: 101,
        iteration_id: 1,
        index: 5,
        tx_hash: base_tx_hash,
    };
    assert!(
        !tx_base.cancel_each_other(&reorg_diff_block),
        "Different block_number should not cancel"
    );
    assert!(
        !reorg_diff_block.cancel_each_other(&tx_base),
        "Should be symmetric"
    );

    // Different iteration_id - should NOT cancel
    let reorg_diff_iter = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 2,
        index: 5,
        tx_hash: base_tx_hash,
    };
    assert!(
        !tx_base.cancel_each_other(&reorg_diff_iter),
        "Different iteration_id should not cancel"
    );
    assert!(
        !reorg_diff_iter.cancel_each_other(&tx_base),
        "Should be symmetric"
    );

    // Different index - should NOT cancel
    let reorg_diff_index = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 6,
        tx_hash: base_tx_hash,
    };
    assert!(
        !tx_base.cancel_each_other(&reorg_diff_index),
        "Different index should not cancel"
    );
    assert!(
        !reorg_diff_index.cancel_each_other(&tx_base),
        "Should be symmetric"
    );

    // Different tx_hash - should NOT cancel
    let reorg_diff_hash = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash: TxHash::random(),
    };
    assert!(
        !tx_base.cancel_each_other(&reorg_diff_hash),
        "Different tx_hash should not cancel"
    );
    assert!(
        !reorg_diff_hash.cancel_each_other(&tx_base),
        "Should be symmetric"
    );

    // Multiple fields different - should NOT cancel
    let reorg_multiple_diff = EventMetadata::Reorg {
        block_number: 101,
        iteration_id: 2,
        index: 6,
        tx_hash: TxHash::random(),
    };
    assert!(
        !tx_base.cancel_each_other(&reorg_multiple_diff),
        "Multiple different fields should not cancel"
    );
}

#[test]
fn test_cancel_each_other_prev_tx_hash_irrelevant() {
    let tx_hash = TxHash::random();

    let tx_with_prev = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash,
        prev_tx_hash: Some(TxHash::random()),
    };

    let tx_without_prev = EventMetadata::Transaction {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash,
        prev_tx_hash: None,
    };

    let reorg = EventMetadata::Reorg {
        block_number: 100,
        iteration_id: 1,
        index: 5,
        tx_hash,
    };

    assert!(
        tx_with_prev.cancel_each_other(&reorg),
        "Transaction with prev_tx_hash should cancel with Reorg"
    );
    assert!(
        tx_without_prev.cancel_each_other(&reorg),
        "Transaction without prev_tx_hash should cancel with Reorg"
    );
}
