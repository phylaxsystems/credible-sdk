use crate::{
    cache::Cache,
    engine::queue::{
        TransactionQueueReceiver,
        TransactionQueueSender,
        TxQueueContents,
    },
};
use crossbeam::channel::TryRecvError;
use std::{
    collections::VecDeque,
    sync::Arc,
    time::Duration,
};
use tokio::{
    task::JoinHandle,
    time::sleep,
};
use tracing::{
    debug,
    trace,
    info,
    warn,
    error,
};

/// Buffers transaction events until at least one state source reports as synced.
///
/// The service sits between the transport and the core engine. While no sources
/// are synced, it stores transaction events in memory and periodically polls the
/// cache to check if a source becomes available. Block and reorg events continue
/// to flow so the engine can advance head information. Once synchronization is
/// confirmed, the buffered transactions are flushed in FIFO order and subsequent
/// events pass through without additional processing.
pub struct StateSyncService {
    cache: Arc<Cache>,
    inbound: TransactionQueueReceiver,
    outbound: TransactionQueueSender,
    poll_interval: Duration,
}

impl StateSyncService {
    pub fn new(
        cache: Arc<Cache>,
        inbound: TransactionQueueReceiver,
        outbound: TransactionQueueSender,
    ) -> Self {
        Self {
            cache,
            inbound,
            outbound,
            poll_interval: Duration::from_millis(250),
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(mut self) {
        let mut buffer = VecDeque::new();
        if self.cache.has_synced_source() {
            debug!(
                target = "state_sync_service",
                "Cache already synced, running passthrough"
            );
            self.passthrough().await;
            return;
        }

        info!(
            target = "state_sync_service",
            "Waiting for cached sources to sync"
        );

        loop {
            match self.inbound.try_recv() {
                Ok(event) => {
                    if matches!(event, TxQueueContents::Tx(..)) {
                        trace!(
                            target = "state_sync_service",
                            "Buffering transaction event while sources are unsynced"
                        );
                        buffer.push_back(event);
                    } else if self.outbound.send(event).is_err() {
                        return;
                    }
                }
                Err(TryRecvError::Empty) => {
                    if self.cache.has_synced_source() {
                        debug!(
                            target = "state_sync_service",
                            buffered_events = buffer.len(),
                            "State sources synced, releasing buffered transactions"
                        );

                        while let Some(event) = buffer.pop_front() {
                            if self.outbound.send(event).is_err() {
                                return;
                            }
                        }

                        break;
                    }

                    sleep(self.poll_interval).await;
                }
                Err(TryRecvError::Disconnected) => {
                    debug!(
                        target = "state_sync_service",
                        "Inbound transaction channel disconnected"
                    );
                    return;
                }
            }
        }

        self.passthrough().await;
    }

    async fn passthrough(&mut self) {
        loop {
            match self.inbound.try_recv() {
                Ok(event) => {
                    if self.outbound.send(event).is_err() {
                        error!(
                            target = "state_sync_service",
                            "Failed to forward event; engine down"
                        );
                        return;
                    }
                }
                Err(TryRecvError::Empty) => {
                    sleep(Duration::from_millis(10)).await;
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }

        debug!(
            target = "state_sync_service",
            "Inbound channel closed; shutting down state sync service"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cache::sources::{
            Source,
            SourceError,
        },
        engine::queue::{
            QueueBlockEnv,
            TxQueueContents,
        },
    };
    use assertion_executor::{
        db::DatabaseRef,
        primitives::{
            AccountInfo,
            Address,
            B256,
            Bytecode,
        },
    };
    use revm::primitives::{
        StorageKey,
        StorageValue,
    };
    use std::sync::atomic::{
        AtomicBool,
        Ordering,
    };
    use tracing::Span;

    #[derive(Debug)]
    struct ToggleSource {
        synced: AtomicBool,
    }

    impl ToggleSource {
        fn new() -> Self {
            Self {
                synced: AtomicBool::new(false),
            }
        }

        fn set_synced(&self, synced: bool) {
            self.synced.store(synced, Ordering::Release);
        }
    }

    impl Source for ToggleSource {
        fn is_synced(&self, _current_block_number: u64) -> bool {
            self.synced.load(Ordering::Acquire)
        }

        fn name(&self) -> &'static str {
            "toggle-source"
        }

        fn update_target_block(&self, _block_number: u64) {}
    }

    impl DatabaseRef for ToggleSource {
        type Error = SourceError;

        fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(None)
        }

        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            Err(SourceError::BlockNotFound)
        }

        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            Err(SourceError::CodeByHashNotFound)
        }

        fn storage_ref(
            &self,
            _address: Address,
            _index: StorageKey,
        ) -> Result<StorageValue, Self::Error> {
            Err(SourceError::CacheMiss)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn buffers_transactions_until_source_synced() {
        let source = Arc::new(ToggleSource::new());
        let cache = Arc::new(Cache::new(vec![source.clone()], 10));

        let (transport_sender, service_receiver) = crossbeam::channel::unbounded();
        let (engine_sender, engine_receiver) = crossbeam::channel::unbounded();

        let handle = StateSyncService::new(cache.clone(), service_receiver, engine_sender).spawn();

        // Block events pass through immediately even when unsynced.
        transport_sender
            .send(TxQueueContents::Block(
                QueueBlockEnv::default(),
                Span::none(),
            ))
            .unwrap();
        let block = tokio::task::spawn_blocking({
            let receiver = engine_receiver.clone();
            move || receiver.recv_timeout(Duration::from_millis(200))
        })
        .await
        .expect("spawn block failure")
        .expect("block event should pass through");
        assert!(matches!(block, TxQueueContents::Block(..)));

        // Transaction events are buffered while unsynced.
        let tx = TxQueueContents::Tx(
            crate::engine::queue::QueueTransaction {
                tx_hash: B256::ZERO,
                tx_env: revm::context::TxEnv::default(),
            },
            Span::none(),
        );
        transport_sender.send(tx).unwrap();
        let rx_res = tokio::task::spawn_blocking({
            let receiver = engine_receiver.clone();
            move || receiver.recv_timeout(Duration::from_millis(200))
        })
        .await
        .expect("spawn block failure");
        assert!(rx_res.is_err());

        // Flip source to synced and ensure buffered tx is released.
        source.set_synced(true);
        let released = tokio::task::spawn_blocking({
            let receiver = engine_receiver.clone();
            move || receiver.recv_timeout(Duration::from_secs(1))
        })
        .await
        .expect("spawn block failure")
        .expect("buffered transaction should be released once synced");
        assert!(matches!(released, TxQueueContents::Tx(..)));

        // Subsequent transactions flow immediately after release.
        let next_tx = TxQueueContents::Tx(
            crate::engine::queue::QueueTransaction {
                tx_hash: B256::from([0x01; 32]),
                tx_env: revm::context::TxEnv::default(),
            },
            Span::none(),
        );
        transport_sender.send(next_tx).unwrap();
        let subsequent = tokio::task::spawn_blocking({
            let receiver = engine_receiver.clone();
            move || receiver.recv_timeout(Duration::from_millis(200))
        })
        .await
        .expect("spawn block failure")
        .expect("transaction should flow once synced");
        assert!(matches!(subsequent, TxQueueContents::Tx(..)));

        drop(transport_sender);
        handle.await.expect("state sync service task should finish");
    }
}
