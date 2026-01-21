use crate::{
    engine::{
        TransactionResult,
        queue::TxQueueContents,
    },
    execution_ids::TxExecutionId,
};
use dashmap::{
    DashMap,
    mapref::one::Ref,
};
use std::{
    sync::{
        Arc,
        Mutex,
    },
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::broadcast,
    task::JoinHandle,
    time::Instant,
};
use tracing::{
    error,
    warn,
};

/// Event emitted when a transaction result is available.
#[derive(Debug, Clone)]
pub struct TransactionResultEvent {
    pub tx_execution_id: TxExecutionId,
    pub result: TransactionResult,
}

#[derive(Clone, Debug)]
struct PendingResultRequest {
    sender: broadcast::Sender<TransactionResult>,
    created_at: Instant,
}

#[derive(Debug)]
pub struct TransactionsState {
    transaction_results: DashMap<TxExecutionId, TransactionResult>,
    /// `DashMap` containing the pending queries from the reading the transaction result.
    /// It contains the transaction execution id as key and the broadcast sender as value. The result shall be
    /// sent via broadcast channel once it is ready.
    ///
    /// It is also used to create new receivers for multiple clients waiting for the result.
    ///
    /// Pending requests are cleaned up by a background task to avoid unbounded growth.
    transaction_results_pending_requests: DashMap<TxExecutionId, PendingResultRequest>,
    /// Accepted transactions which haven't been processed yet, with insertion time.
    accepted_txs: DashMap<TxExecutionId, Instant>,
    /// Optional channel for streaming transaction results to external observers (e.g., transports).
    result_event_sender: Option<flume::Sender<TransactionResultEvent>>,
    pending_requests_ttl: Duration,
    accepted_txs_ttl: Duration,
    bg_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl TransactionsState {
    pub fn new() -> Arc<Self> {
        Self::new_with_ttls(Duration::from_secs(600), Duration::from_secs(600))
    }

    pub fn new_with_ttls(pending_requests_ttl: Duration, accepted_txs_ttl: Duration) -> Arc<Self> {
        let state = Arc::new(Self {
            transaction_results: DashMap::new(),
            transaction_results_pending_requests: DashMap::new(),
            accepted_txs: DashMap::new(),
            result_event_sender: None,
            pending_requests_ttl,
            accepted_txs_ttl,
            bg_task_handle: Arc::new(Mutex::new(None)),
        });
        state.spawn_cleanup_task();
        state
    }

    /// Create a new `TransactionsState` with a result event sender.
    ///
    /// The sender will receive `TransactionResultEvent` for every transaction result,
    /// allowing external systems (e.g., transport layers) to stream results.
    pub fn with_result_sender(
        result_event_sender: flume::Sender<TransactionResultEvent>,
    ) -> Arc<Self> {
        Self::with_result_sender_and_ttls(
            result_event_sender,
            Duration::from_secs(600),
            Duration::from_secs(600),
        )
    }

    pub fn with_result_sender_and_ttls(
        result_event_sender: flume::Sender<TransactionResultEvent>,
        pending_requests_ttl: Duration,
        accepted_txs_ttl: Duration,
    ) -> Arc<Self> {
        let state = Arc::new(Self {
            transaction_results: DashMap::new(),
            transaction_results_pending_requests: DashMap::new(),
            accepted_txs: DashMap::new(),
            result_event_sender: Some(result_event_sender),
            pending_requests_ttl,
            accepted_txs_ttl,
            bg_task_handle: Arc::new(Mutex::new(None)),
        });
        state.spawn_cleanup_task();
        state
    }

    pub fn add_transaction_result(
        &self,
        tx_execution_id: TxExecutionId,
        result: &TransactionResult,
    ) {
        self.transaction_results
            .insert(tx_execution_id, result.clone());
        self.accepted_txs.remove(&tx_execution_id);
        self.process_pending_queries(tx_execution_id, result);

        // Send to external observers if configured.
        if let Some(ref sender) = self.result_event_sender
            && let Err(e) = sender.send(TransactionResultEvent {
                tx_execution_id,
                result: result.clone(),
            })
        {
            error!(target = "transactions_state", error = ?e, "Failed to send transaction result to result event sender");
        }
    }

    pub fn remove_transaction_result(&self, tx_execution_id: &TxExecutionId) {
        self.transaction_results.remove(tx_execution_id);
    }

    pub fn add_accepted_tx(&self, tx_queue_contents: &TxQueueContents) {
        if let TxQueueContents::Tx(tx, _) = tx_queue_contents {
            self.accepted_txs.insert(tx.tx_execution_id, Instant::now());
        }
    }

    pub fn get_all_lengths(&self) -> (usize, usize, usize) {
        (
            self.accepted_txs.len(),
            self.transaction_results_pending_requests.len(),
            self.transaction_results.len(),
        )
    }

    /// The transaction is processed if it is either accepted or it was already processed by the engine.
    pub fn is_tx_received(&self, tx_execution_id: &TxExecutionId) -> bool {
        self.accepted_txs.contains_key(tx_execution_id)
            || self.transaction_results.contains_key(tx_execution_id)
    }

    /// Check if there is a pending query for the processed result
    fn process_pending_queries(&self, tx_execution_id: TxExecutionId, result: &TransactionResult) {
        // O(1)
        let Some((_, pending)) = self
            .transaction_results_pending_requests
            .remove(&tx_execution_id)
        else {
            return;
        };
        // Purposedly ignore this error in case there is a race condition and the result is sent twice
        // O(1)
        let _ = pending.sender.send(result.clone()).map_err(|e| {
            error!(
                target = "transactions_state",
                error = ?e,
                tx_execution_id.block_number = %tx_execution_id.block_number,
                tx_execution_id.iteration_id = tx_execution_id.iteration_id,
                tx_hash = %tx_execution_id.tx_hash_hex(),
                "Failed to send transaction result to query sender"
            );
        });
    }

    pub fn get_transaction_result(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Option<Ref<'_, TxExecutionId, TransactionResult>> {
        self.transaction_results.get(tx_execution_id)
    }

    pub fn get_all_transaction_result(&self) -> &DashMap<TxExecutionId, TransactionResult> {
        &self.transaction_results
    }

    /// Requests a transaction result, if the result is available, it is returned immediately,
    /// if the result is not available, it is return an oneshot channel for receiving the
    /// result as soon as it is available.
    pub fn request_transaction_result(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> RequestTransactionResult {
        let result = self.get_transaction_result(tx_execution_id);
        if let Some(result) = result {
            RequestTransactionResult::Result(result.clone())
        } else {
            // If we have a channel with clients waiting, we can just create a new
            // receiver and pass it on
            if let Some(channel) = self
                .transaction_results_pending_requests
                .get(tx_execution_id)
            {
                return RequestTransactionResult::Channel(channel.sender.subscribe());
            }

            let (response_tx, response_rx) = broadcast::channel(1);
            self.transaction_results_pending_requests.insert(
                *tx_execution_id,
                PendingResultRequest {
                    sender: response_tx,
                    created_at: Instant::now(),
                },
            );

            // Check the race condition in which the engine is faster than the transport layer process:
            // Then it could happen:
            // 1. The engine is processing the requested transaction
            // 2. This process checks for the result and doesn't find it
            // 3. The engine adds the result to the state
            // 4. This process adds the query to pending_queries
            // 5. The engine already checked pending_queries just before it was written
            if let Some(result) = self.get_transaction_result(tx_execution_id) {
                self.transaction_results_pending_requests
                    .remove(tx_execution_id);
                return RequestTransactionResult::Result(result.clone());
            }
            RequestTransactionResult::Channel(response_rx)
        }
    }

    fn spawn_cleanup_task(self: &Arc<Self>) {
        if Handle::try_current().is_err() {
            return;
        }
        let Ok(mut handle_lock) = self.bg_task_handle.lock() else {
            warn!("cleanup task mutex poisoned; skipping cleanup spawn");
            return;
        };
        if handle_lock.is_some() {
            return;
        }

        let pending_requests = self.transaction_results_pending_requests.clone();
        let accepted_txs = self.accepted_txs.clone();
        let pending_requests_ttl = self.pending_requests_ttl;
        let accepted_txs_ttl = self.accepted_txs_ttl;

        let task = tokio::task::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                cleanup_interval.tick().await;

                pending_requests.retain(|_, pending| {
                    let age = pending.created_at.elapsed();
                    pending.sender.receiver_count() > 0 && age <= pending_requests_ttl
                });

                accepted_txs.retain(|_, created_at| created_at.elapsed() <= accepted_txs_ttl);
            }
        });

        *handle_lock = Some(task);
    }
}

pub enum RequestTransactionResult {
    Result(TransactionResult),
    Channel(broadcast::Receiver<TransactionResult>),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_truncation)]
    #![allow(clippy::cast_sign_loss)]
    use super::*;
    use crate::engine::queue::{
        NewIteration,
        QueueTransaction,
        TxQueueContents,
    };
    use alloy::primitives::U256;
    use assertion_executor::primitives::{
        Bytes,
        ExecutionResult,
        TxEnv,
    };
    use revm::{
        context::result::{
            Output,
            SuccessReason,
        },
        primitives::alloy_primitives::B256,
    };
    use std::sync::Arc;
    use tokio::time::{
        Duration,
        timeout,
    };
    use tracing::Span;

    /// Helper function to create a test transaction execution id
    fn create_test_tx_execution_id() -> TxExecutionId {
        let tx_hash = B256::from([1u8; 32]);
        TxExecutionId::new(U256::from(1), 0, tx_hash, 0)
    }

    /// Helper function to create another test transaction execution id
    fn create_test_tx_execution_id_2() -> TxExecutionId {
        let tx_hash = B256::from([2u8; 32]);
        TxExecutionId::new(U256::from(2), 1, tx_hash, 0)
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

    /// Helper function to create a test `TxQueueContents` with transaction
    fn create_test_tx_queue_contents(tx_execution_id: TxExecutionId) -> TxQueueContents {
        TxQueueContents::Tx(
            QueueTransaction {
                tx_execution_id,
                tx_env: TxEnv::default(),
                prev_tx_hash: None,
            },
            Span::current(),
        )
    }

    /// Helper function to create a test `TxQueueContents` with block
    fn create_test_block_queue_contents() -> TxQueueContents {
        TxQueueContents::NewIteration(NewIteration::default(), Span::current())
    }

    #[test]
    fn test_new_creates_empty_state() {
        let state = TransactionsState::new();

        assert!(state.transaction_results.is_empty());
        assert!(state.transaction_results_pending_requests.is_empty());
        assert!(state.accepted_txs.is_empty());
        assert!(state.result_event_sender.is_none());
    }

    #[test]
    fn test_with_result_sender_stores_sender() {
        let (tx, _rx) = flume::bounded(16);
        let state = TransactionsState::with_result_sender(tx);

        assert!(state.result_event_sender.is_some());
    }

    #[test]
    fn test_add_transaction_result_stores_result() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_execution_id, &result.clone());

        assert_eq!(state.transaction_results.len(), 1);
        let stored_result = state.get_transaction_result(&tx_execution_id).unwrap();
        assert_eq!(*stored_result, result);
    }

    #[test]
    fn test_add_transaction_result_sends_to_channel() {
        let (tx, rx) = flume::bounded(16);
        let state = TransactionsState::with_result_sender(tx);
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_execution_id, &result.clone());

        // Should receive event on the channel
        let event = rx.try_recv().expect("Should receive event");
        assert_eq!(event.tx_execution_id, tx_execution_id);
        assert_eq!(event.result, result);
    }

    #[test]
    fn test_add_transaction_result_does_not_error_without_sender() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        // Should not panic even without the sender
        state.add_transaction_result(tx_execution_id, &result);

        assert!(state.transaction_results.contains_key(&tx_execution_id));
    }

    #[test]
    fn test_add_transaction_result_removes_from_accepted_txs() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let tx_queue_contents = create_test_tx_queue_contents(tx_execution_id);

        // First add to accepted transactions
        state.add_accepted_tx(&tx_queue_contents);
        assert!(state.accepted_txs.contains_key(&tx_execution_id));

        // Then add the result, should remove from accepted_txs
        let result = create_test_transaction_result();
        state.add_transaction_result(tx_execution_id, &result);

        assert!(!state.accepted_txs.contains_key(&tx_execution_id));
        assert_eq!(state.transaction_results.len(), 1);
    }

    #[test]
    fn test_add_accepted_tx_with_transaction() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let tx_queue_contents = create_test_tx_queue_contents(tx_execution_id);

        state.add_accepted_tx(&tx_queue_contents);

        assert!(state.accepted_txs.contains_key(&tx_execution_id));
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
        let tx_execution_id = create_test_tx_execution_id();
        let tx_queue_contents = create_test_tx_queue_contents(tx_execution_id);

        state.add_accepted_tx(&tx_queue_contents);

        assert!(state.is_tx_received(&tx_execution_id));
    }

    #[test]
    fn test_is_tx_received_with_processed_result() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_execution_id, &result);

        assert!(state.is_tx_received(&tx_execution_id));
    }

    #[test]
    fn test_is_tx_received_returns_false_for_unknown_tx() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();

        assert!(!state.is_tx_received(&tx_execution_id));
    }

    #[test]
    fn test_get_transaction_result_returns_none_for_nonexistent() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();

        let result = state.get_transaction_result(&tx_execution_id);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_transaction_result_returns_stored_result() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_execution_id, &result.clone());

        let stored_result = state.get_transaction_result(&tx_execution_id).unwrap();
        assert_eq!(*stored_result, result);
    }

    #[test]
    fn test_get_all_transaction_result_returns_reference() {
        let state = TransactionsState::new();
        let tx_id1 = create_test_tx_execution_id();
        let tx_id2 = create_test_tx_execution_id_2();
        let result1 = create_test_transaction_result();
        let result2 = create_validation_error_result();

        state.add_transaction_result(tx_id1, &result1);
        state.add_transaction_result(tx_id2, &result2);

        let all_results = state.get_all_transaction_result();
        assert_eq!(all_results.len(), 2);
        assert!(all_results.contains_key(&tx_id1));
        assert!(all_results.contains_key(&tx_id2));
    }

    #[tokio::test]
    async fn test_request_transaction_result_returns_immediately_if_available() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_execution_id, &result.clone());

        match state.request_transaction_result(&tx_execution_id) {
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
        let tx_execution_id = create_test_tx_execution_id();

        match state.request_transaction_result(&tx_execution_id) {
            RequestTransactionResult::Result(_) => {
                panic!("Expected channel, got immediate result");
            }
            RequestTransactionResult::Channel(receiver) => {
                // Verify the pending request is stored
                assert!(
                    state
                        .transaction_results_pending_requests
                        .contains_key(&tx_execution_id)
                );

                // Clean up by dropping receiver to avoid hanging test
                drop(receiver);
            }
        }
    }

    #[tokio::test]
    async fn test_process_pending_queries_sends_result_to_waiting_receiver() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        // First request the transaction result (this will create a pending query)
        let mut receiver = match state.request_transaction_result(&tx_execution_id) {
            RequestTransactionResult::Channel(rx) => rx,
            RequestTransactionResult::Result(_) => panic!("Expected channel"),
        };

        // Verify the pending request exists
        assert!(
            state
                .transaction_results_pending_requests
                .contains_key(&tx_execution_id)
        );

        // Add the transaction result (this should trigger the pending query processing)
        state.add_transaction_result(tx_execution_id, &result.clone());

        // Verify the pending request was removed
        assert!(
            !state
                .transaction_results_pending_requests
                .contains_key(&tx_execution_id)
        );

        // Verify we can receive the result through the channel
        let received_result = timeout(Duration::from_millis(100), receiver.recv())
            .await
            .expect("Should receive result within timeout")
            .expect("Channel should not be closed");

        assert_eq!(received_result, result);
    }

    #[tokio::test]
    async fn test_race_condition_handling_in_request_transaction_result() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        // Simulate race condition: add result after request is made but before channel is returned
        let (tx, _rx) = broadcast::channel(1);
        state.transaction_results_pending_requests.insert(
            tx_execution_id,
            PendingResultRequest {
                sender: tx,
                created_at: Instant::now(),
            },
        );

        // Add the transaction result
        state.add_transaction_result(tx_execution_id, &result.clone());

        // Now request should return immediate result due to race condition handling
        match state.request_transaction_result(&tx_execution_id) {
            RequestTransactionResult::Result(returned_result) => {
                assert_eq!(returned_result, result);
                // Verify pending request was cleaned up
                assert!(
                    !state
                        .transaction_results_pending_requests
                        .contains_key(&tx_execution_id)
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
        let tx_id1 = create_test_tx_execution_id();
        let tx_id2 = create_test_tx_execution_id_2();
        let result1 = create_test_transaction_result();
        let result2 = create_validation_error_result();

        // Create pending queries for both transactions
        let mut receiver1 = match state.request_transaction_result(&tx_id1) {
            RequestTransactionResult::Channel(rx) => rx,
            RequestTransactionResult::Result(_) => panic!("Expected channel for tx1"),
        };

        let mut receiver2 = match state.request_transaction_result(&tx_id2) {
            RequestTransactionResult::Channel(rx) => rx,
            RequestTransactionResult::Result(_) => panic!("Expected channel for tx2"),
        };

        // Verify both pending requests exist
        assert!(
            state
                .transaction_results_pending_requests
                .contains_key(&tx_id1)
        );
        assert!(
            state
                .transaction_results_pending_requests
                .contains_key(&tx_id2)
        );

        // Add result for first transaction
        state.add_transaction_result(tx_id1, &result1.clone());

        // Verify first pending request was removed, second still exists
        assert!(
            !state
                .transaction_results_pending_requests
                .contains_key(&tx_id1)
        );
        assert!(
            state
                .transaction_results_pending_requests
                .contains_key(&tx_id2)
        );

        // Verify first receiver gets the result
        let received_result1 = timeout(Duration::from_millis(100), receiver1.recv())
            .await
            .expect("Should receive result1 within timeout")
            .expect("Channel should not be closed");
        assert_eq!(received_result1, result1);

        // Add result for second transaction
        state.add_transaction_result(tx_id2, &result2.clone());

        // Verify second pending request was removed
        assert!(
            !state
                .transaction_results_pending_requests
                .contains_key(&tx_id2)
        );

        // Verify second receiver gets the result
        let received_result2 = timeout(Duration::from_millis(100), receiver2.recv())
            .await
            .expect("Should receive result2 within timeout")
            .expect("Channel should not be closed");
        assert_eq!(received_result2, result2);
    }

    #[tokio::test]
    async fn test_multiple_receivers_get_broadcast_result() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        // Request the same transaction result twice before it is available so both share the broadcast sender.
        let mut receiver_one = match state.request_transaction_result(&tx_execution_id) {
            RequestTransactionResult::Channel(rx) => rx,
            RequestTransactionResult::Result(_) => panic!("Expected channel for first requester"),
        };

        let mut receiver_two = match state.request_transaction_result(&tx_execution_id) {
            RequestTransactionResult::Channel(rx) => rx,
            RequestTransactionResult::Result(_) => panic!("Expected channel for second requester"),
        };

        state.add_transaction_result(tx_execution_id, &result.clone());

        // Both receivers should pick up the broadcasted value.
        let received_one = timeout(Duration::from_millis(100), receiver_one.recv())
            .await
            .expect("Should receive result for first receiver")
            .expect("Channel should not be closed");
        let received_two = timeout(Duration::from_millis(100), receiver_two.recv())
            .await
            .expect("Should receive result for second receiver")
            .expect("Channel should not be closed");

        assert_eq!(received_one, result);
        assert_eq!(received_two, result);

        // After delivery, pending requests map should no longer contain the id.
        assert!(
            !state
                .transaction_results_pending_requests
                .contains_key(&tx_execution_id)
        );
    }

    #[tokio::test]
    async fn test_new_requester_after_result_gets_immediate_value() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        // First subscriber waits for the broadcasted result.
        let mut waiting_receiver = match state.request_transaction_result(&tx_execution_id) {
            RequestTransactionResult::Channel(rx) => rx,
            RequestTransactionResult::Result(_) => panic!("Expected channel"),
        };

        state.add_transaction_result(tx_execution_id, &result.clone());

        let received = timeout(Duration::from_millis(100), waiting_receiver.recv())
            .await
            .expect("Should receive broadcast")
            .expect("Channel should not be closed");
        assert_eq!(received, result);

        // A new transport request after the result is stored should receive it immediately.
        match state.request_transaction_result(&tx_execution_id) {
            RequestTransactionResult::Result(returned) => assert_eq!(returned, result),
            RequestTransactionResult::Channel(_) => {
                panic!("Expected immediate result for new requester")
            }
        }
    }

    #[test]
    fn test_process_pending_queries_handles_no_pending_query() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        // This should not panic even when there's no pending query
        state.add_transaction_result(tx_execution_id, &result);

        // Verify the result was still stored
        assert!(state.transaction_results.contains_key(&tx_execution_id));
    }

    #[tokio::test]
    async fn test_dropped_receiver_does_not_cause_issues() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id();
        let result = create_test_transaction_result();

        // Create and immediately drop the receiver
        match state.request_transaction_result(&tx_execution_id) {
            RequestTransactionResult::Channel(rx) => drop(rx),
            RequestTransactionResult::Result(_) => panic!("Expected channel"),
        }

        // Adding the result should not panic even though receiver was dropped
        state.add_transaction_result(tx_execution_id, &result);

        // Verify the result was stored and pending request was cleaned up
        assert!(state.transaction_results.contains_key(&tx_execution_id));
        assert!(
            !state
                .transaction_results_pending_requests
                .contains_key(&tx_execution_id)
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
                let tx_execution_id =
                    TxExecutionId::new(U256::from(i + 1), i % 3, B256::from([i as u8; 32]), i);
                let result = create_test_transaction_result();

                // Add accepted tx
                let tx_queue_contents = create_test_tx_queue_contents(tx_execution_id);
                state_clone.add_accepted_tx(&tx_queue_contents);

                // Check if received
                assert!(state_clone.is_tx_received(&tx_execution_id));

                // Add result
                state_clone.add_transaction_result(tx_execution_id, &result.clone());

                // Verify result exists
                let stored_result = state_clone
                    .get_transaction_result(&tx_execution_id)
                    .unwrap();
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
