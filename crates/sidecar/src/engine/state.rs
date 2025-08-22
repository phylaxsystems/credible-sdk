use crate::engine::queue::GetTransactionResultQueueReceiver;
use crate::engine::{
    EngineError,
    StateResults,
    TransactionResult,
};
use assertion_executor::primitives::B256;
use dashmap::mapref::one::Ref;
use std::sync::Arc;
use tracing::error;

pub(crate) struct State {
    get_tx_result_receiver: GetTransactionResultQueueReceiver,
    state_results: Arc<StateResults>,
}

impl State {
    pub(crate) fn new(
        get_tx_result_receiver: GetTransactionResultQueueReceiver,
        state_results: Arc<StateResults>,
    ) -> Self {
        Self {
            get_tx_result_receiver,
            state_results,
        }
    }

    /// Get transaction result by hash.
    fn get_transaction_result(&self, tx_hash: &B256) -> Option<Ref<'_, B256, TransactionResult>> {
        self.state_results.transaction_results.get(tx_hash)
    }

    pub(crate) async fn run(&mut self) -> Result<(), EngineError> {
        loop {
            let event = self.get_tx_result_receiver.try_recv().map_err(|e| {
                error!(
                    target = "engine",
                    error = ?e,
                    "Get transaction result queue channel closed"
                );
                EngineError::GetTxResultChannelClosed
            })?;

            let tx_hash = event.tx_hash;
            let result = self.get_transaction_result(&tx_hash);
            match result {
                Some(result) => {
                    event.sender.send(result.clone()).map_err(|e| {
                        error!(
                            target = "engine",
                            error = ?e,
                            tx_hash = %tx_hash,
                            "Failed to send transaction result to query sender"
                        );
                        EngineError::GetTxResultChannelClosed
                    })?;
                }
                None => {
                    self.state_results
                        .pending_queries
                        .insert(event.tx_hash, event.sender);

                    // Check the race condition in which the engine is faster than this process:
                    // Then it could happen:
                    // 1. The engine is processing the requested transaction
                    // 2. This process checks for the result and doesn't find it
                    // 3. The engine adds the result to the state
                    // 4. This process adds the query to pending_queries
                    // 5. The engine already checked pending_queries just before it was written
                    let result = self.get_transaction_result(&tx_hash);
                    if let Some(result) = result
                        && let Some((_, sender)) =
                            self.state_results.pending_queries.remove(&tx_hash)
                    {
                        sender.send(result.clone()).map_err(|e| {
                            error!(
                                target = "engine",
                                error = ?e,
                                tx_hash = %tx_hash,
                                "Failed to send transaction result to query sender"
                            );
                            EngineError::GetTxResultChannelClosed
                        })?;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::queue::QueryGetTxResult;
    use assertion_executor::primitives::ExecutionResult;
    use assertion_executor::primitives::HaltReason;
    use crossbeam::channel::unbounded;
    use revm::context::result::{
        Output,
        SuccessReason,
    };
    use std::sync::Arc;
    use tokio::sync::oneshot;

    fn create_test_state() -> (
        State,
        crossbeam::channel::Sender<QueryGetTxResult>,
        Arc<StateResults>,
    ) {
        let (sender, receiver) = unbounded();
        let state_results = StateResults::new();
        let state = State::new(receiver, state_results.clone());
        (state, sender, state_results)
    }

    fn create_sample_transaction_result(is_valid: bool) -> TransactionResult {
        if is_valid {
            TransactionResult::ValidationCompleted {
                execution_result: ExecutionResult::Success {
                    reason: SuccessReason::Return,
                    gas_used: 21000,
                    gas_refunded: 0,
                    logs: vec![],
                    output: Output::Call(vec![].into()),
                },
                is_valid: true,
            }
        } else {
            TransactionResult::ValidationCompleted {
                execution_result: ExecutionResult::Halt {
                    reason: HaltReason::CallNotAllowedInsideStatic,
                    gas_used: 100000,
                },
                is_valid: false,
            }
        }
    }

    #[test]
    fn test_state_new() {
        let (sender, receiver) = unbounded();
        let state_results = StateResults::new();

        let state = State::new(receiver, state_results.clone());

        // Verify state was constructed correctly by testing basic functionality
        let tx_hash = B256::from([0x99; 32]);
        let transaction_result = create_sample_transaction_result(true);

        // Insert via the shared state_results
        state_results
            .transaction_results
            .insert(tx_hash, transaction_result.clone());

        // Verify state can access it
        let result = state.get_transaction_result(&tx_hash);
        assert!(
            result.is_some(),
            "State should have access to shared state_results"
        );
        assert_eq!(*result.unwrap(), transaction_result);

        // Clean up
        drop(sender);
    }

    #[test]
    fn test_get_transaction_result_existing() {
        let (state, _sender, state_results) = create_test_state();

        // Insert a test transaction result
        let tx_hash = B256::from([0x11; 32]);
        let transaction_result = create_sample_transaction_result(true);

        state_results
            .transaction_results
            .insert(tx_hash, transaction_result.clone());

        // Test getting the result
        let result = state.get_transaction_result(&tx_hash);
        assert!(result.is_some(), "Should find existing transaction result");
        assert_eq!(*result.unwrap(), transaction_result);
    }

    #[test]
    fn test_get_transaction_result_nonexistent() {
        let (state, _sender, _state_results) = create_test_state();

        let tx_hash = B256::from([0x22; 32]);
        let result = state.get_transaction_result(&tx_hash);
        assert!(
            result.is_none(),
            "Should return None for non-existent transaction"
        );
    }

    #[tokio::test]
    async fn test_run_handles_existing_result() {
        let (mut state, sender, state_results) = create_test_state();

        // Insert a test transaction result first
        let tx_hash = B256::from([0x33; 32]);
        let transaction_result = create_sample_transaction_result(true);

        state_results
            .transaction_results
            .insert(tx_hash, transaction_result.clone());

        // Create a query for this transaction
        let (result_sender, result_receiver) = oneshot::channel();
        let query = QueryGetTxResult {
            tx_hash,
            sender: result_sender,
        };

        // Send the query
        sender.send(query).unwrap();

        // Run state processing (will process one query and continue the loop)
        // We need to spawn this in a task and then cancel it since run() has an infinite loop
        let state_handle = tokio::spawn(async move {
            let _ = state.run().await;
        });

        // Give it time to process the query
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Check that we received the result
        let received_result = result_receiver.await.unwrap();
        assert_eq!(received_result, transaction_result);

        // Clean up the task
        state_handle.abort();
        let _ = state_handle.await;
    }

    #[tokio::test]
    async fn test_run_handles_pending_query() {
        let (mut state, sender, state_results) = create_test_state();

        let tx_hash = B256::from([0x44; 32]);

        // Create a query for a transaction that doesn't exist yet
        let (result_sender, _result_receiver) = oneshot::channel();
        let query = QueryGetTxResult {
            tx_hash,
            sender: result_sender,
        };

        // Send the query
        sender.send(query).unwrap();

        // Run state processing in background
        let state_handle = tokio::spawn(async move {
            let _ = state.run().await;
        });

        // Give it time to process and add to pending queries
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Verify the query was added to pending queries
        {
            assert!(
                state_results.pending_queries.contains_key(&tx_hash),
                "Query should be added to pending queries"
            );
        }

        // Clean up
        state_handle.abort();
        let _ = state_handle.await;
    }

    #[tokio::test]
    async fn test_run_channel_closed_error() {
        let (mut state, sender, _state_results) = create_test_state();

        // Close the sender channel
        drop(sender);

        // Run should return an error when the channel is closed
        let result = state.run().await;
        assert!(
            result.is_err(),
            "Should return error when channel is closed"
        );
        match result.unwrap_err() {
            EngineError::GetTxResultChannelClosed => {
                // Expected error
            }
            other => panic!("Expected GetTxResultChannelClosed error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_run_send_error_handling() {
        let (mut state, sender, state_results) = create_test_state();

        // Insert a test transaction result
        let tx_hash = B256::from([0x55; 32]);
        let transaction_result = create_sample_transaction_result(true);
        state_results
            .transaction_results
            .insert(tx_hash, transaction_result);

        // Create a query with a receiver that we'll drop to simulate send failure
        let (result_sender, result_receiver) = oneshot::channel();
        drop(result_receiver); // Drop receiver to make send fail

        let query = QueryGetTxResult {
            tx_hash,
            sender: result_sender,
        };

        // Send the query
        sender.send(query).unwrap();

        // Run state processing
        let state_handle = tokio::spawn(async move {
            let _ = state.run().await;
        });

        // Give it time to process the query (it should handle the send error gracefully)
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Clean up
        state_handle.abort();
        let _ = state_handle.await;

        // The test passes if we get here without panicking - error was handled gracefully
    }

    #[test]
    fn test_multiple_queries_for_same_transaction() {
        let (state, _sender, state_results) = create_test_state();

        let tx_hash = B256::from([0x66; 32]);

        // Create multiple pending queries for the same transaction
        let (sender1, _receiver1) = oneshot::channel();
        let (sender2, _receiver2) = oneshot::channel();

        // Add them to pending queries manually
        {
            state_results.pending_queries.insert(tx_hash, sender1);
            // This will overwrite the first sender - only one query per tx_hash is kept
            state_results.pending_queries.insert(tx_hash, sender2);
        }

        // Verify only the last sender remains
        {
            assert_eq!(
                state_results.pending_queries.len(),
                1,
                "Should only have one pending query per tx_hash"
            );
            assert!(
                state_results.pending_queries.contains_key(&tx_hash),
                "Should contain the tx_hash"
            );
        }

        // Clean up to avoid dropping the channel receivers in the wrong order
        drop(state);
    }

    #[test]
    fn test_state_results_thread_safety() {
        let (_state, _sender, state_results) = create_test_state();
        let tx_hash = B256::from([0x77; 32]);

        // Test that StateResults can be safely shared between threads
        let state_results_clone = state_results.clone();
        let handle = std::thread::spawn(move || {
            let transaction_result = create_sample_transaction_result(true);

            // Insert from another thread
            state_results_clone
                .transaction_results
                .insert(tx_hash, transaction_result);
        });

        handle.join().unwrap();

        // Verify we can read the result from the original thread
        let result = state_results.transaction_results.get(&tx_hash);
        assert!(
            result.is_some(),
            "Should be able to read result inserted from another thread"
        );
    }

    #[test]
    fn test_validation_error_result() {
        let (state, _sender, state_results) = create_test_state();

        let tx_hash = B256::from([0x88; 32]);
        let error_result = TransactionResult::ValidationError("Test validation error".to_string());

        state_results
            .transaction_results
            .insert(tx_hash, error_result.clone());

        let result = state.get_transaction_result(&tx_hash);
        assert!(result.is_some(), "Should find validation error result");
        match result.unwrap().clone() {
            TransactionResult::ValidationError(msg) => {
                assert_eq!(msg, "Test validation error");
            }
            other => panic!("Expected ValidationError, got {:?}", other),
        }
    }

    #[test]
    fn test_failed_validation_result() {
        let (state, _sender, state_results) = create_test_state();

        let tx_hash = B256::from([0x99; 32]);
        let failed_result = create_sample_transaction_result(false);

        state_results
            .transaction_results
            .insert(tx_hash, failed_result.clone());

        let result = state.get_transaction_result(&tx_hash);
        assert!(result.is_some(), "Should find failed validation result");
        match result.unwrap().clone() {
            TransactionResult::ValidationCompleted {
                is_valid,
                execution_result,
            } => {
                assert!(!is_valid, "Transaction should be marked as invalid");
                match execution_result {
                    ExecutionResult::Halt { reason, .. } => {
                        assert_eq!(reason, HaltReason::CallNotAllowedInsideStatic);
                    }
                    other => panic!("Expected Revert result, got {:?}", other),
                }
            }
            other => panic!("Expected ValidationCompleted, got {:?}", other),
        }
    }

    #[test]
    fn test_concurrent_access_to_transaction_results() {
        let (_state, _sender, state_results) = create_test_state();

        // Spawn multiple threads that read and write transaction results
        let mut handles = vec![];

        for i in 0..10 {
            let state_results_clone = state_results.clone();
            let handle = std::thread::spawn(move || {
                let tx_hash = B256::from([i; 32]);
                let transaction_result = create_sample_transaction_result(i % 2 == 0);

                // Insert result
                state_results_clone
                    .transaction_results
                    .insert(tx_hash, transaction_result.clone());

                // Read it back
                let result = state_results_clone.transaction_results.get(&tx_hash);
                assert!(result.is_some(), "Should find result we just inserted");
                assert_eq!(*result.unwrap(), transaction_result);
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all results are there
        assert_eq!(
            state_results.transaction_results.len(),
            10,
            "Should have 10 results"
        );
    }

    #[test]
    fn test_dashmap_behavior() {
        let (_state, _sender, state_results) = create_test_state();
        let tx_hash = B256::from([0xaa; 32]);

        // Test that DashMap behaves as expected
        assert!(
            state_results.transaction_results.is_empty(),
            "Should start empty"
        );

        let transaction_result = create_sample_transaction_result(true);
        state_results
            .transaction_results
            .insert(tx_hash, transaction_result.clone());

        assert_eq!(
            state_results.transaction_results.len(),
            1,
            "Should have one entry"
        );
        assert!(
            state_results.transaction_results.contains_key(&tx_hash),
            "Should contain the key"
        );

        // Test get vs get_mut behavior
        let read_result = state_results.transaction_results.get(&tx_hash);
        assert!(read_result.is_some(), "Should be able to read");
        assert_eq!(*read_result.unwrap(), transaction_result);

        // Test removal
        let removed = state_results.transaction_results.remove(&tx_hash);
        assert!(removed.is_some(), "Should be able to remove");
        assert_eq!(removed.unwrap().1, transaction_result);

        assert!(
            state_results.transaction_results.is_empty(),
            "Should be empty after removal"
        );
    }

    #[test]
    fn test_pending_queries_mutex_behavior() {
        let (_state, _sender, state_results) = create_test_state();
        let tx_hash = B256::from([0xbb; 32]);

        // Test that the mutex works correctly
        {
            assert!(
                state_results.pending_queries.is_empty(),
                "Should start empty"
            );

            let (sender, _receiver) = oneshot::channel();
            state_results.pending_queries.insert(tx_hash, sender);
            assert_eq!(
                state_results.pending_queries.len(),
                1,
                "Should have one pending query"
            );
        } // Lock is dropped here

        // Test that we can access it again
        {
            assert!(
                state_results.pending_queries.contains_key(&tx_hash),
                "Should still contain the query"
            );
        }

        // Test removal
        {
            let removed = state_results.pending_queries.remove(&tx_hash);
            assert!(removed.is_some(), "Should be able to remove");
        }

        {
            assert!(
                state_results.pending_queries.is_empty(),
                "Should be empty after removal"
            );
        }
    }
}
