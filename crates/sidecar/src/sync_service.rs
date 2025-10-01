//! `sync_service`
//!
//! The sync service ensures that we do not forward transactions to the core engine unless
//! we have at least one state source synced.
//!
//! Once this is done we forward all txs and blockenvs directly to the core engine.
//!
//! If we receive a blockenv before we are synced, we will error out and restart.
//! We do this to not have outdated txs on an updated state when the state worker
//! catches up.

use crate::{
    cache::Cache,
    engine::queue::{
        TransactionQueueReceiver,
        TransactionQueueSender,
        TxQueueContents,
    },
    utils::ErrorRecoverability,
};
use crossbeam::channel::TryRecvError;
use std::{
    collections::VecDeque,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::time::sleep;
use tracing::{
    debug,
    info,
    warn,
};

/// Buffers transaction events until at least one state source reports as synced.
///
/// Once a source reports as synced, buffered transactions are
/// flushed and the service becomes a passthrough.
///
/// If a blockenv is received we exit early with an error.
pub struct StateSyncService {
    cache: Arc<Cache>,
    inbound: TransactionQueueReceiver,
    outbound: TransactionQueueSender,
    poll_interval: Duration,
}

#[derive(Debug, Error)]
pub enum StateSyncError {
    #[error("inbound transaction channel closed")]
    InboundChannelClosed,
    #[error("outbound transaction channel closed")]
    OutboundChannelClosed,
    #[error("BlockEnv received before state sources have been synced")]
    BlockEnvReceived,
}

impl From<&StateSyncError> for ErrorRecoverability {
    fn from(_: &StateSyncError) -> Self {
        ErrorRecoverability::Recoverable
    }
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
            poll_interval: Duration::from_millis(100),
        }
    }

    pub async fn run(mut self) -> Result<(), StateSyncError> {
        let mut buffer = VecDeque::new();
        if self.cache.has_synced_source() {
            debug!(
                target = "state_sync_service",
                "Cache already synced, running passthrough"
            );
            return self.passthrough().await;
        }

        info!(
            target = "state_sync_service",
            "Waiting for cached sources to sync"
        );

        loop {
            match self.inbound.try_recv() {
                Ok(event) => {
                    if matches!(event, TxQueueContents::Tx(..)) {
                        debug!(
                            target = "state_sync_service",
                            "Buffering transaction event while sources are unsynced"
                        );
                        buffer.push_back(event);
                    } else {
                        // Instead of storing blockenvs we crash on purpose if no state sources
                        // are synced.
                        //
                        // This is to prevent transactions from previous blockenvs being executed
                        // on an a version of the state higher than they were initially sent at.
                        // FIXME: the proper solution to this is probably to store our sidecar
                        // desired height in redis and have the state worker respect that and
                        // not sync higher than the desired height.
                        //
                        // We can also clear the buffer from outdated blockenvs
                        warn!(
                            target = "state_sync_service",
                            "Received BlockEnv before our state sources have been synced!"
                        );
                        return Err(StateSyncError::BlockEnvReceived);
                    }
                }
                Err(TryRecvError::Empty) => {
                    tokio::task::yield_now().await;

                    if self.cache.has_synced_source() {
                        debug!(
                            target = "state_sync_service",
                            buffered_events = buffer.len(),
                            "State sources synced, releasing buffered transactions"
                        );

                        while let Some(event) = buffer.pop_front() {
                            if self.outbound.send(event).is_err() {
                                warn!(
                                    target = "state_sync_service",
                                    "Unable to forward buffered event; outbound channel closed!"
                                );
                                return Err(StateSyncError::OutboundChannelClosed);
                            }
                        }

                        return self.passthrough().await;
                    }

                    sleep(self.poll_interval).await;
                }
                Err(TryRecvError::Disconnected) => {
                    warn!(
                        target = "state_sync_service",
                        "Inbound transaction channel disconnected!"
                    );
                    return Err(StateSyncError::InboundChannelClosed);
                }
            }
        }
    }

    /// Passes through transactions received from the sync service
    /// to the core engine.
    ///
    /// Will continue to do this until its killed.
    async fn passthrough(&mut self) -> Result<(), StateSyncError> {
        loop {
            match self.inbound.try_recv() {
                Ok(event) => {
                    if self.outbound.send(event).is_err() {
                        warn!(
                            target = "state_sync_service",
                            "Failed to forward event; engine down"
                        );
                        return Err(StateSyncError::OutboundChannelClosed);
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
        Ok(())
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
            QueueTransaction,
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

        let handle = tokio::spawn(
            StateSyncService::new(cache.clone(), service_receiver, engine_sender).run(),
        );

        // Transaction events are buffered while unsynced.
        let tx = TxQueueContents::Tx(
            QueueTransaction {
                tx_hash: B256::ZERO,
                tx_env: revm::context::TxEnv::default(),
            },
            Span::none(),
        );
        transport_sender.send(tx).unwrap();
        let buffered = tokio::task::spawn_blocking({
            let receiver = engine_receiver.clone();
            move || receiver.recv_timeout(Duration::from_millis(200))
        })
        .await
        .expect("spawn blocking");
        assert!(buffered.is_err());

        // Flip source to synced and ensure buffered tx is released.
        source.set_synced(true);
        let released = tokio::task::spawn_blocking({
            let receiver = engine_receiver.clone();
            move || receiver.recv_timeout(Duration::from_secs(1))
        })
        .await
        .expect("spawn blocking")
        .expect("buffered transaction should be released once synced");
        assert!(matches!(released, TxQueueContents::Tx(..)));

        // Subsequent transactions flow immediately after release.
        let next_tx = TxQueueContents::Tx(
            QueueTransaction {
                tx_hash: B256::from([0x01; 32]),
                tx_env: revm::context::TxEnv::default(),
            },
            Span::none(),
        );
        transport_sender.send(next_tx).unwrap();
        let subsequent = tokio::task::spawn_blocking({
            let receiver = engine_receiver;
            move || receiver.recv_timeout(Duration::from_millis(200))
        })
        .await
        .expect("spawn blocking")
        .expect("transaction should flow once synced");
        assert!(matches!(subsequent, TxQueueContents::Tx(..)));

        drop(transport_sender);
        handle
            .await
            .expect("state sync service join failed")
            .expect("state sync service should exit cleanly");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[should_panic]
    async fn does_not_bufer_blockenv() {
        let source = Arc::new(ToggleSource::new());
        let cache = Arc::new(Cache::new(vec![source.clone()], 10));

        let (transport_sender, service_receiver) = crossbeam::channel::unbounded();
        let (engine_sender, engine_receiver) = crossbeam::channel::unbounded();

        let _handle = tokio::spawn(
            StateSyncService::new(cache.clone(), service_receiver, engine_sender).run(),
        );

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
        .expect("spawn blocking")
        .expect("block event should pass through");
        assert!(matches!(block, TxQueueContents::Block(..)));
    }
}
