use crate::engine::{
    TransactionResult,
    queue::TxQueueContents,
};
use assertion_executor::primitives::B256;
use dashmap::{
    DashMap,
    DashSet,
    mapref::one::Ref,
};
use revm::primitives::alloy_primitives::TxHash;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::error;

#[derive(Debug)]
pub struct TransactionsState {
    transaction_results: DashMap<B256, TransactionResult>,
    /// DashMap containing the pending queries from the reading the transaction result.
    /// It contains the transaction hash as key and the oneshot sender as value. The result shall be
    /// sent via oneshot channel once it is ready.
    transaction_results_pending_requests: DashMap<TxHash, oneshot::Sender<TransactionResult>>,
    /// HashSet containing the accepted transactions which haven't been processed yet.
    accepted_txs: DashSet<TxHash>,
}

impl TransactionsState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            transaction_results: DashMap::new(),
            transaction_results_pending_requests: DashMap::new(),
            accepted_txs: DashSet::new(),
        })
    }

    pub fn add_transaction_result(&self, tx_hash: B256, result: TransactionResult) {
        self.transaction_results.insert(tx_hash, result.clone());
        self.accepted_txs.remove(&tx_hash);
        self.process_pending_queries(tx_hash, result);
    }

    pub fn remove_transaction_result(&self, tx_hash: &B256) {
        self.transaction_results.remove(tx_hash);
    }

    pub fn add_accepted_tx(&self, tx_queue_contents: &TxQueueContents) {
        if let TxQueueContents::Tx(tx) = tx_queue_contents {
            self.accepted_txs.insert(tx.tx_hash);
        }
    }

    /// The transaction is processed if it is either accepted or it was already processed by the engine.
    pub fn is_tx_received(&self, tx_hash: &TxHash) -> bool {
        self.accepted_txs.contains(tx_hash) || self.transaction_results.contains_key(tx_hash)
    }

    /// Check if there is a pending query for the processed result
    fn process_pending_queries(&self, tx_hash: TxHash, result: TransactionResult) {
        // O(1)
        let Some((_, sender)) = self.transaction_results_pending_requests.remove(&tx_hash) else {
            return;
        };
        // Purposedly ignore this error in case there is a race condition and the result is sent twice
        // O(1)
        let _ = sender.send(result.clone()).map_err(|e| {
            error!(
                target = "transactions_state",
                error = ?e,
                tx_hash = %tx_hash,
                "Failed to send transaction result to query sender"
            );
        });
    }

    pub fn get_transaction_result(
        &self,
        tx_hash: &B256,
    ) -> Option<Ref<'_, B256, TransactionResult>> {
        self.transaction_results.get(tx_hash)
    }

    pub fn get_all_transaction_result(&self) -> &DashMap<B256, TransactionResult> {
        &self.transaction_results
    }

    /// Requests a transaction result, if the result is available, it is returned immediately, if the result is not available, it is return an oneshot channel for receiving the result as soon as it is available
    pub fn request_transaction_result(&self, tx_hash: &TxHash) -> RequestTransactionResult {
        let result = self.get_transaction_result(tx_hash);
        match result {
            Some(result) => RequestTransactionResult::Result(result.clone()),
            None => {
                let (response_tx, response_rx) = oneshot::channel();
                self.transaction_results_pending_requests
                    .insert(*tx_hash, response_tx);

                // Check the race condition in which the engine is faster than the transport layer process:
                // Then it could happen:
                // 1. The engine is processing the requested transaction
                // 2. This process checks for the result and doesn't find it
                // 3. The engine adds the result to the state
                // 4. This process adds the query to pending_queries
                // 5. The engine already checked pending_queries just before it was written
                if let Some(result) = self.get_transaction_result(tx_hash) {
                    self.transaction_results_pending_requests.remove(tx_hash);
                    return RequestTransactionResult::Result(result.clone());
                }
                RequestTransactionResult::Channel(response_rx)
            }
        }
    }
}

pub enum RequestTransactionResult {
    Result(TransactionResult),
    Channel(oneshot::Receiver<TransactionResult>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::queue::{
        QueueTransaction,
        TxQueueContents,
    };
    use assertion_executor::primitives::{
        Bytes,
        ExecutionResult,
        TxEnv,
    };
    use revm::{
        context::{
            BlockEnv,
            result::{
                Output,
                SuccessReason,
            },
        },
        primitives::alloy_primitives::B256,
    };
    use std::sync::Arc;
    use tokio::time::{
        Duration,
        timeout,
    };

    /// Helper function to create a test transaction hash
    fn create_test_tx_hash() -> B256 {
        B256::from([1u8; 32])
    }

    /// Helper function to create another test transaction hash
    fn create_test_tx_hash_2() -> B256 {
        B256::from([2u8; 32])
    }

    /// Helper function to create a test transaction result
    fn create_test_transaction_result() -> TransactionResult {
        TransactionResult::ValidationCompleted {
            execution_result: ExecutionResult::Success {
                reason: SuccessReason::Stop,
                gas_used: 21000,
                gas_refunded: 0,
                logs: vec![],
                output: Output::Call(Bytes::default()),
            },
            is_valid: true,
        }
    }

    /// Helper function to create a validation error result
    fn create_validation_error_result() -> TransactionResult {
        TransactionResult::ValidationError("Test validation error".to_string())
    }

    /// Helper function to create a test TxQueueContents with transaction
    fn create_test_tx_queue_contents(tx_hash: B256) -> TxQueueContents {
        TxQueueContents::Tx(QueueTransaction {
            tx_hash,
            tx_env: TxEnv::default(),
        })
    }

    /// Helper function to create a test TxQueueContents with block
    fn create_test_block_queue_contents() -> TxQueueContents {
        TxQueueContents::Block(BlockEnv::default())
    }

    #[test]
    fn test_new_creates_empty_state() {
        let state = TransactionsState::new();

        assert!(state.transaction_results.is_empty());
        assert!(state.transaction_results_pending_requests.is_empty());
        assert!(state.accepted_txs.is_empty());
    }

    #[test]
    fn test_add_transaction_result_stores_result() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_hash, result.clone());

        assert_eq!(state.transaction_results.len(), 1);
        let stored_result = state.get_transaction_result(&tx_hash).unwrap();
        assert_eq!(*stored_result, result);
    }

    #[test]
    fn test_add_transaction_result_removes_from_accepted_txs() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let tx_queue_contents = create_test_tx_queue_contents(tx_hash);

        // First add to accepted transactions
        state.add_accepted_tx(&tx_queue_contents);
        assert!(state.accepted_txs.contains(&tx_hash));

        // Then add the result, should remove from accepted_txs
        let result = create_test_transaction_result();
        state.add_transaction_result(tx_hash, result);

        assert!(!state.accepted_txs.contains(&tx_hash));
        assert_eq!(state.transaction_results.len(), 1);
    }

    #[test]
    fn test_add_accepted_tx_with_transaction() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let tx_queue_contents = create_test_tx_queue_contents(tx_hash);

        state.add_accepted_tx(&tx_queue_contents);

        assert!(state.accepted_txs.contains(&tx_hash));
        assert_eq!(state.accepted_txs.len(), 1);
    }

    #[test]
    fn test_add_accepted_tx_with_block_does_nothing() {
        let state = TransactionsState::new();
        let block_queue_contents = create_test_block_queue_contents();

        state.add_accepted_tx(&block_queue_contents);

        assert!(state.accepted_txs.is_empty());
    }

    #[test]
    fn test_is_tx_received_with_accepted_tx() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let tx_queue_contents = create_test_tx_queue_contents(tx_hash);

        state.add_accepted_tx(&tx_queue_contents);

        assert!(state.is_tx_received(&tx_hash));
    }

    #[test]
    fn test_is_tx_received_with_processed_result() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_hash, result);

        assert!(state.is_tx_received(&tx_hash));
    }

    #[test]
    fn test_is_tx_received_returns_false_for_unknown_tx() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();

        assert!(!state.is_tx_received(&tx_hash));
    }

    #[test]
    fn test_get_transaction_result_returns_none_for_nonexistent() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();

        let result = state.get_transaction_result(&tx_hash);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_transaction_result_returns_stored_result() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_hash, result.clone());

        let stored_result = state.get_transaction_result(&tx_hash).unwrap();
        assert_eq!(*stored_result, result);
    }

    #[test]
    fn test_get_all_transaction_result_returns_reference() {
        let state = TransactionsState::new();
        let tx_hash1 = create_test_tx_hash();
        let tx_hash2 = create_test_tx_hash_2();
        let result1 = create_test_transaction_result();
        let result2 = create_validation_error_result();

        state.add_transaction_result(tx_hash1, result1);
        state.add_transaction_result(tx_hash2, result2);

        let all_results = state.get_all_transaction_result();
        assert_eq!(all_results.len(), 2);
        assert!(all_results.contains_key(&tx_hash1));
        assert!(all_results.contains_key(&tx_hash2));
    }

    #[tokio::test]
    async fn test_request_transaction_result_returns_immediately_if_available() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_hash, result.clone());

        match state.request_transaction_result(&tx_hash) {
            RequestTransactionResult::Result(returned_result) => {
                assert_eq!(returned_result, result);
            }
            RequestTransactionResult::Channel(_) => {
                panic!("Expected immediate result, got channel");
            }
        }
    }

    #[tokio::test]
    async fn test_request_transaction_result_returns_channel_if_not_available() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();

        match state.request_transaction_result(&tx_hash) {
            RequestTransactionResult::Result(_) => {
                panic!("Expected channel, got immediate result");
            }
            RequestTransactionResult::Channel(receiver) => {
                // Verify the pending request is stored
                assert!(
                    state
                        .transaction_results_pending_requests
                        .contains_key(&tx_hash)
                );

                // Clean up by dropping receiver to avoid hanging test
                drop(receiver);
            }
        }
    }

    #[tokio::test]
    async fn test_process_pending_queries_sends_result_to_waiting_receiver() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let result = create_test_transaction_result();

        // First request the transaction result (this will create a pending query)
        let receiver = match state.request_transaction_result(&tx_hash) {
            RequestTransactionResult::Channel(rx) => rx,
            RequestTransactionResult::Result(_) => panic!("Expected channel"),
        };

        // Verify the pending request exists
        assert!(
            state
                .transaction_results_pending_requests
                .contains_key(&tx_hash)
        );

        // Add the transaction result (this should trigger the pending query processing)
        state.add_transaction_result(tx_hash, result.clone());

        // Verify the pending request was removed
        assert!(
            !state
                .transaction_results_pending_requests
                .contains_key(&tx_hash)
        );

        // Verify we can receive the result through the channel
        let received_result = timeout(Duration::from_millis(100), receiver)
            .await
            .expect("Should receive result within timeout")
            .expect("Channel should not be closed");

        assert_eq!(received_result, result);
    }

    #[tokio::test]
    async fn test_race_condition_handling_in_request_transaction_result() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let result = create_test_transaction_result();

        // Simulate race condition: add result after request is made but before channel is returned
        let (tx, _rx) = oneshot::channel();
        state
            .transaction_results_pending_requests
            .insert(tx_hash, tx);

        // Add the transaction result
        state.add_transaction_result(tx_hash, result.clone());

        // Now request should return immediate result due to race condition handling
        match state.request_transaction_result(&tx_hash) {
            RequestTransactionResult::Result(returned_result) => {
                assert_eq!(returned_result, result);
                // Verify pending request was cleaned up
                assert!(
                    !state
                        .transaction_results_pending_requests
                        .contains_key(&tx_hash)
                );
            }
            RequestTransactionResult::Channel(_) => {
                panic!("Expected immediate result due to race condition handling");
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_pending_queries_for_different_transactions() {
        let state = TransactionsState::new();
        let tx_hash1 = create_test_tx_hash();
        let tx_hash2 = create_test_tx_hash_2();
        let result1 = create_test_transaction_result();
        let result2 = create_validation_error_result();

        // Create pending queries for both transactions
        let receiver1 = match state.request_transaction_result(&tx_hash1) {
            RequestTransactionResult::Channel(rx) => rx,
            RequestTransactionResult::Result(_) => panic!("Expected channel for tx1"),
        };

        let receiver2 = match state.request_transaction_result(&tx_hash2) {
            RequestTransactionResult::Channel(rx) => rx,
            RequestTransactionResult::Result(_) => panic!("Expected channel for tx2"),
        };

        // Verify both pending requests exist
        assert!(
            state
                .transaction_results_pending_requests
                .contains_key(&tx_hash1)
        );
        assert!(
            state
                .transaction_results_pending_requests
                .contains_key(&tx_hash2)
        );

        // Add result for first transaction
        state.add_transaction_result(tx_hash1, result1.clone());

        // Verify first pending request was removed, second still exists
        assert!(
            !state
                .transaction_results_pending_requests
                .contains_key(&tx_hash1)
        );
        assert!(
            state
                .transaction_results_pending_requests
                .contains_key(&tx_hash2)
        );

        // Verify first receiver gets the result
        let received_result1 = timeout(Duration::from_millis(100), receiver1)
            .await
            .expect("Should receive result1 within timeout")
            .expect("Channel should not be closed");
        assert_eq!(received_result1, result1);

        // Add result for second transaction
        state.add_transaction_result(tx_hash2, result2.clone());

        // Verify second pending request was removed
        assert!(
            !state
                .transaction_results_pending_requests
                .contains_key(&tx_hash2)
        );

        // Verify second receiver gets the result
        let received_result2 = timeout(Duration::from_millis(100), receiver2)
            .await
            .expect("Should receive result2 within timeout")
            .expect("Channel should not be closed");
        assert_eq!(received_result2, result2);
    }

    #[test]
    fn test_process_pending_queries_handles_no_pending_query() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let result = create_test_transaction_result();

        // This should not panic even when there's no pending query
        state.add_transaction_result(tx_hash, result);

        // Verify the result was still stored
        assert!(state.transaction_results.contains_key(&tx_hash));
    }

    #[tokio::test]
    async fn test_dropped_receiver_does_not_cause_issues() {
        let state = TransactionsState::new();
        let tx_hash = create_test_tx_hash();
        let result = create_test_transaction_result();

        // Create and immediately drop the receiver
        match state.request_transaction_result(&tx_hash) {
            RequestTransactionResult::Channel(rx) => drop(rx),
            RequestTransactionResult::Result(_) => panic!("Expected channel"),
        };

        // Adding the result should not panic even though receiver was dropped
        state.add_transaction_result(tx_hash, result);

        // Verify the result was stored and pending request was cleaned up
        assert!(state.transaction_results.contains_key(&tx_hash));
        assert!(
            !state
                .transaction_results_pending_requests
                .contains_key(&tx_hash)
        );
    }

    #[test]
    fn test_concurrent_access_thread_safety() {
        use std::thread;

        let state = Arc::new(TransactionsState::new());
        let mut handles = vec![];

        // Spawn multiple threads that concurrently access the state
        for i in 0..10 {
            let state_clone = Arc::clone(&state);
            let handle = thread::spawn(move || {
                let tx_hash = B256::from([i as u8; 32]);
                let result = create_test_transaction_result();

                // Add accepted tx
                let tx_queue_contents = create_test_tx_queue_contents(tx_hash);
                state_clone.add_accepted_tx(&tx_queue_contents);

                // Check if received
                assert!(state_clone.is_tx_received(&tx_hash));

                // Add result
                state_clone.add_transaction_result(tx_hash, result.clone());

                // Verify result exists
                let stored_result = state_clone.get_transaction_result(&tx_hash).unwrap();
                assert_eq!(*stored_result, result);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final state
        assert_eq!(state.transaction_results.len(), 10);
        assert!(state.accepted_txs.is_empty()); // All should have been removed by add_transaction_result
    }
}
