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

#[derive(Debug)]
pub struct TransactionsState {
    transaction_results: DashMap<TxExecutionId, TransactionResult>,
    /// Accepted transactions which haven't produced a result yet.
    accepted_txs: DashMap<TxExecutionId, Instant>,
    /// Optional channel for streaming transaction results to external observers (e.g., transports).
    result_event_sender: Option<flume::Sender<TransactionResultEvent>>,
    accepted_txs_ttl: Duration,
    bg_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl TransactionsState {
    pub fn new() -> Arc<Self> {
        Self::new_with_ttls(Duration::from_secs(600))
    }

    pub fn new_with_ttls(accepted_txs_ttl: Duration) -> Arc<Self> {
        let state = Arc::new(Self {
            transaction_results: DashMap::new(),
            accepted_txs: DashMap::new(),
            result_event_sender: None,
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
        Self::with_result_sender_and_ttls(result_event_sender, Duration::from_secs(600))
    }

    pub fn with_result_sender_and_ttls(
        result_event_sender: flume::Sender<TransactionResultEvent>,
        accepted_txs_ttl: Duration,
    ) -> Arc<Self> {
        let state = Arc::new(Self {
            transaction_results: DashMap::new(),
            accepted_txs: DashMap::new(),
            result_event_sender: Some(result_event_sender),
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

    /// Clears only the in-flight tracking (accepted txs).
    ///
    /// Useful when invalidating an iteration while keeping historical results.
    pub fn clear_in_flight(&self) {
        self.accepted_txs.clear();
    }

    pub fn add_accepted_tx(&self, tx_queue_contents: &TxQueueContents) {
        if let TxQueueContents::Tx(tx) = tx_queue_contents {
            self.accepted_txs.insert(tx.tx_execution_id, Instant::now());
        }
    }

    pub fn get_all_lengths(&self) -> (usize, usize) {
        (self.accepted_txs.len(), self.transaction_results.len())
    }

    /// The transaction is processed if it is either accepted or it was already processed by the engine.
    pub fn is_tx_received(&self, tx_execution_id: &TxExecutionId) -> bool {
        self.accepted_txs.contains_key(tx_execution_id)
            || self.transaction_results.contains_key(tx_execution_id)
    }

    pub fn get_transaction_result(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Option<Ref<'_, TxExecutionId, TransactionResult>> {
        self.transaction_results.get(tx_execution_id)
    }

    #[inline]
    pub fn get_transaction_result_cloned(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Option<TransactionResult> {
        self.get_transaction_result(tx_execution_id)
            .map(|result| result.clone())
    }

    pub fn get_all_transaction_result(&self) -> &DashMap<TxExecutionId, TransactionResult> {
        &self.transaction_results
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

        let accepted_txs = self.accepted_txs.clone();
        let accepted_txs_ttl = self.accepted_txs_ttl;

        let task = tokio::task::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                cleanup_interval.tick().await;
                Self::cleanup_accepted_txs(&accepted_txs, accepted_txs_ttl);
            }
        });

        *handle_lock = Some(task);
    }

    fn cleanup_accepted_txs(accepted_txs: &DashMap<TxExecutionId, Instant>, ttl: Duration) {
        accepted_txs.retain(|_, created_at| created_at.elapsed() <= ttl);
    }

    #[cfg(test)]
    fn cleanup_accepted_txs_now(&self) {
        Self::cleanup_accepted_txs(&self.accepted_txs, self.accepted_txs_ttl);
    }
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
    use std::{
        sync::{
            Arc,
            atomic::{
                AtomicUsize,
                Ordering,
            },
        },
        thread,
    };

    fn create_test_tx_execution_id(byte: u8) -> TxExecutionId {
        let tx_hash = B256::from([byte; 32]);
        TxExecutionId::new(U256::from(byte), 0, tx_hash, 0)
    }

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

    fn create_test_tx_queue_contents(tx_execution_id: TxExecutionId) -> TxQueueContents {
        TxQueueContents::Tx(QueueTransaction {
            tx_execution_id,
            tx_env: TxEnv::default(),
            prev_tx_hash: None,
        })
    }

    #[test]
    fn new_creates_empty_state() {
        let state = TransactionsState::new();
        assert!(state.transaction_results.is_empty());
        assert!(state.accepted_txs.is_empty());
        assert!(state.result_event_sender.is_none());
    }

    #[test]
    fn with_result_sender_stores_sender() {
        let (tx, _rx) = flume::bounded(16);
        let state = TransactionsState::with_result_sender(tx);
        assert!(state.result_event_sender.is_some());
    }

    #[test]
    fn add_transaction_result_stores_and_is_immediately_readable() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id(1);
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_execution_id, &result);

        assert_eq!(
            state
                .get_transaction_result_cloned(&tx_execution_id)
                .expect("result should exist"),
            result
        );
    }

    #[test]
    fn add_transaction_result_sends_stream_event() {
        let (tx, rx) = flume::bounded(4);
        let state = TransactionsState::with_result_sender(tx);
        let tx_execution_id = create_test_tx_execution_id(2);
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_execution_id, &result);

        let event = rx.try_recv().expect("expected result event");
        assert_eq!(event.tx_execution_id, tx_execution_id);
        assert_eq!(event.result, result);
    }

    #[test]
    fn add_transaction_result_stores_even_if_stream_receiver_is_dropped() {
        let (tx, rx) = flume::bounded(1);
        drop(rx);
        let state = TransactionsState::with_result_sender(tx);
        let tx_execution_id = create_test_tx_execution_id(22);
        let result = create_test_transaction_result();

        state.add_transaction_result(tx_execution_id, &result);

        assert_eq!(
            state
                .get_transaction_result_cloned(&tx_execution_id)
                .expect("result should still be stored"),
            result
        );
    }

    #[test]
    fn add_transaction_result_clears_accepted_marker() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id(3);
        let tx_queue_contents = create_test_tx_queue_contents(tx_execution_id);
        state.add_accepted_tx(&tx_queue_contents);
        assert!(state.accepted_txs.contains_key(&tx_execution_id));

        let result = create_test_transaction_result();
        state.add_transaction_result(tx_execution_id, &result);
        assert!(!state.accepted_txs.contains_key(&tx_execution_id));
    }

    #[test]
    fn add_accepted_tx_ignores_non_transaction_events() {
        let state = TransactionsState::new();
        state.add_accepted_tx(&TxQueueContents::NewIteration(NewIteration::default()));
        assert!(state.accepted_txs.is_empty());
    }

    #[test]
    fn is_tx_received_reflects_accepted_and_result_maps() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id(4);
        assert!(!state.is_tx_received(&tx_execution_id));

        state.add_accepted_tx(&create_test_tx_queue_contents(tx_execution_id));
        assert!(state.is_tx_received(&tx_execution_id));
        assert!(
            state.get_transaction_result(&tx_execution_id).is_none(),
            "accepted tx should not expose a result before recording"
        );

        state.add_transaction_result(tx_execution_id, &create_test_transaction_result());
        assert!(state.is_tx_received(&tx_execution_id));
    }

    #[test]
    fn clear_in_flight_only_removes_accepted() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id(5);
        state.add_accepted_tx(&create_test_tx_queue_contents(tx_execution_id));
        state.add_transaction_result(tx_execution_id, &create_test_transaction_result());

        state.clear_in_flight();

        assert!(
            state.get_transaction_result(&tx_execution_id).is_some(),
            "historical result should be preserved"
        );
        assert!(state.accepted_txs.is_empty());
    }

    #[tokio::test]
    async fn accepted_entries_expire_after_ttl() {
        let state = TransactionsState::new_with_ttls(Duration::from_millis(50));
        let tx_execution_id = create_test_tx_execution_id(6);
        state.add_accepted_tx(&create_test_tx_queue_contents(tx_execution_id));
        assert!(state.accepted_txs.contains_key(&tx_execution_id));

        tokio::time::sleep(Duration::from_millis(80)).await;
        state.cleanup_accepted_txs_now();

        assert!(
            !state.accepted_txs.contains_key(&tx_execution_id),
            "accepted entry should be cleaned up by TTL"
        );
    }

    #[test]
    fn concurrent_access_is_thread_safe() {
        let state = Arc::new(TransactionsState::new());
        let mut handles = vec![];

        for i in 0..16 {
            let state_clone = Arc::clone(&state);
            handles.push(thread::spawn(move || {
                let tx_execution_id = create_test_tx_execution_id(u8::try_from(i + 1).unwrap());
                state_clone.add_accepted_tx(&create_test_tx_queue_contents(tx_execution_id));
                state_clone
                    .add_transaction_result(tx_execution_id, &create_test_transaction_result());
                assert!(
                    state_clone
                        .get_transaction_result(&tx_execution_id)
                        .is_some()
                );
            }));
        }

        for handle in handles {
            handle.join().expect("worker thread should complete");
        }

        assert_eq!(state.transaction_results.len(), 16);
        assert!(state.accepted_txs.is_empty());
    }

    #[tokio::test]
    async fn concurrent_pollers_observe_result_after_delayed_write() {
        let state = TransactionsState::new();
        let tx_execution_id = create_test_tx_execution_id(99);
        state.add_accepted_tx(&create_test_tx_queue_contents(tx_execution_id));

        let seen_result_counter = Arc::new(AtomicUsize::new(0));
        let pollers = 24usize;
        let mut tasks = Vec::with_capacity(pollers);

        for _ in 0..pollers {
            let state_clone = Arc::clone(&state);
            let seen_counter = Arc::clone(&seen_result_counter);
            tasks.push(tokio::spawn(async move {
                for _ in 0..100 {
                    if state_clone
                        .get_transaction_result_cloned(&tx_execution_id)
                        .is_some()
                    {
                        seen_counter.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                panic!("poller timed out before observing a result");
            }));
        }

        tokio::time::sleep(Duration::from_millis(15)).await;
        state.add_transaction_result(tx_execution_id, &create_test_transaction_result());

        for task in tasks {
            task.await.expect("poller task should complete");
        }

        assert_eq!(
            seen_result_counter.load(Ordering::Relaxed),
            pollers,
            "all pollers should eventually observe the inserted result"
        );
    }
}
