use crate::{
    metrics,
    worker::StateWorker,
};
use alloy::rpc::types::Header;
use alloy_provider::Provider;
use anyhow::{
    Result,
    anyhow,
};
use futures_util::StreamExt;
use mdbx::{
    BlockStateUpdate,
    Reader,
    Writer,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
    },
    time::Duration,
};
use tokio::{
    sync::{
        broadcast,
        watch,
    },
    time,
};
use tracing::{
    debug,
    info,
    warn,
};

const SUBSCRIPTION_RETRY_DELAY_SECS: u64 = 5;
const MAX_MISSING_BLOCK_RETRIES: u32 = 3;
const NONE_BLOCK_SENTINEL: u64 = u64::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerStatusSnapshot {
    pub latest_head_seen: Option<u64>,
    pub highest_staged_block: Option<u64>,
    pub mdbx_synced_through: Option<u64>,
    pub healthy: bool,
    pub restarting: bool,
}

#[derive(Debug)]
pub struct WorkerStatus {
    latest_head_seen: AtomicU64,
    highest_staged_block: AtomicU64,
    mdbx_synced_through: AtomicU64,
    healthy: AtomicBool,
    restarting: AtomicBool,
}

impl Default for WorkerStatus {
    fn default() -> Self {
        Self {
            latest_head_seen: AtomicU64::new(NONE_BLOCK_SENTINEL),
            highest_staged_block: AtomicU64::new(NONE_BLOCK_SENTINEL),
            mdbx_synced_through: AtomicU64::new(NONE_BLOCK_SENTINEL),
            healthy: AtomicBool::new(true),
            restarting: AtomicBool::new(false),
        }
    }
}

impl WorkerStatus {
    pub fn snapshot(&self) -> WorkerStatusSnapshot {
        WorkerStatusSnapshot {
            latest_head_seen: decode_block(self.latest_head_seen.load(Ordering::Acquire)),
            highest_staged_block: decode_block(self.highest_staged_block.load(Ordering::Acquire)),
            mdbx_synced_through: decode_block(self.mdbx_synced_through.load(Ordering::Acquire)),
            healthy: self.healthy.load(Ordering::Acquire),
            restarting: self.restarting.load(Ordering::Acquire),
        }
    }

    pub fn set_restarting(&self, restarting: bool) {
        self.restarting.store(restarting, Ordering::Release);
    }

    fn set_latest_head_seen(&self, block_number: Option<u64>) {
        self.latest_head_seen
            .store(encode_block(block_number), Ordering::Release);
    }

    fn set_highest_staged_block(&self, block_number: Option<u64>) {
        self.highest_staged_block
            .store(encode_block(block_number), Ordering::Release);
    }

    fn set_mdbx_synced_through(&self, block_number: Option<u64>) {
        self.mdbx_synced_through
            .store(encode_block(block_number), Ordering::Release);
    }

    fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Release);
    }
}

#[derive(Clone, Debug)]
pub struct CommitTargetHandle {
    tx: watch::Sender<Option<u64>>,
}

impl CommitTargetHandle {
    fn new() -> (Self, watch::Receiver<Option<u64>>) {
        let (tx, rx) = watch::channel(None);
        (Self { tx }, rx)
    }

    pub fn publish(&self, block_number: u64) {
        let _ = self.tx.send_if_modified(|current| {
            if current.map_or(true, |existing| block_number > existing) {
                *current = Some(block_number);
                true
            } else {
                false
            }
        });
    }

    pub fn current_target(&self) -> Option<u64> {
        *self.tx.borrow()
    }
}

#[derive(Clone, Debug)]
pub struct EmbeddedStateWorkerHandle {
    pub control: CommitTargetHandle,
    pub status: Arc<WorkerStatus>,
}

pub struct EmbeddedStateWorkerRuntime<WR>
where
    WR: Writer + Reader,
{
    worker: StateWorker<WR>,
    staged_updates: BTreeMap<u64, BlockStateUpdate>,
    handle: EmbeddedStateWorkerHandle,
    commit_target_rx: watch::Receiver<Option<u64>>,
    auto_advance_commit_target: bool,
}

impl<WR> EmbeddedStateWorkerRuntime<WR>
where
    WR: Writer + Reader + Send + Sync,
    <WR as Writer>::Error: std::error::Error + Send + Sync + 'static,
    <WR as Reader>::Error: std::error::Error + Send + Sync + 'static,
{
    pub fn new(worker: StateWorker<WR>) -> Self {
        let status = Arc::new(WorkerStatus::default());
        let (control, commit_target_rx) = CommitTargetHandle::new();

        Self {
            worker,
            staged_updates: BTreeMap::new(),
            handle: EmbeddedStateWorkerHandle { control, status },
            commit_target_rx,
            auto_advance_commit_target: false,
        }
    }

    pub fn with_auto_advance_commit_target(mut self) -> Self {
        self.auto_advance_commit_target = true;
        self
    }

    pub fn handle(&self) -> EmbeddedStateWorkerHandle {
        self.handle.clone()
    }

    pub async fn run(
        &mut self,
        start_override: Option<u64>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        let result = self.run_inner(start_override, &mut shutdown_rx).await;
        if result.is_err() {
            self.handle.status.set_healthy(false);
        }
        result
    }

    async fn run_inner(
        &mut self,
        start_override: Option<u64>,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<()> {
        self.handle
            .status
            .set_mdbx_synced_through(self.worker.current_synced_block()?);
        self.handle.status.set_healthy(true);
        self.handle.status.set_restarting(false);

        let mut next_block = self.worker.compute_start_block(start_override)?;
        let mut missing_block_retries = 0;

        loop {
            if shutdown_rx.try_recv().is_ok() {
                info!("Shutdown signal received");
                return Ok(());
            }

            self.handle.status.set_healthy(true);
            self.handle.status.set_restarting(false);

            if self.catch_up(&mut next_block, shutdown_rx).await? {
                info!("Shutdown signal received during catch-up");
                return Ok(());
            }

            match self.stream_blocks(&mut next_block, shutdown_rx).await {
                Ok(()) => {
                    info!("Shutdown signal received during streaming");
                    return Ok(());
                }
                Err(err) => {
                    let err_str = err.to_string();
                    let is_missing_block = err_str.contains("Missing block");

                    if is_missing_block {
                        missing_block_retries += 1;

                        if missing_block_retries >= MAX_MISSING_BLOCK_RETRIES {
                            critical!(
                                error = %err,
                                retries = missing_block_retries,
                                "failed to recover from missing block after multiple retries"
                            );
                        }
                    } else {
                        missing_block_retries = 0;
                    }

                    self.handle.status.set_healthy(false);
                    self.handle.status.set_restarting(true);
                    warn!(error = %err, "block subscription ended, retrying");
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Shutdown signal received during retry sleep");
                            return Ok(());
                        }
                        () = time::sleep(Duration::from_secs(SUBSCRIPTION_RETRY_DELAY_SECS)) => {}
                    }
                }
            }
        }
    }

    async fn catch_up(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<bool> {
        loop {
            let head = self.worker.provider_head().await?;
            self.observe_head(head);

            if *next_block > head {
                self.flush_ready_blocks().await?;
                return Ok(false);
            }

            while *next_block <= head {
                self.prepare_and_stage_block(*next_block).await?;
                self.flush_ready_blocks().await?;
                *next_block += 1;
                self.observe_head(head);

                if shutdown_rx.try_recv().is_ok() {
                    return Ok(true);
                }
            }
        }
    }

    async fn stream_blocks(
        &mut self,
        next_block: &mut u64,
        shutdown_rx: &mut broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut stream = self
            .worker
            .provider()
            .subscribe_blocks()
            .await?
            .into_stream();
        metrics::set_syncing(false);
        metrics::set_following_head(true);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received during block streaming");
                    return Ok(());
                }
                commit_target_changed = self.commit_target_rx.changed() => {
                    match commit_target_changed {
                        Ok(()) => self.flush_ready_blocks().await?,
                        Err(_) => return Err(anyhow!("commit target channel closed")),
                    }
                }
                maybe_header = stream.next() => {
                    match maybe_header {
                        Some(header) => {
                            let Header { hash: _, inner, .. } = header;
                            let block_number = inner.number;

                            if block_number + 1 < *next_block {
                                debug!(block_number, next_block, "skipping stale header");
                                continue;
                            }

                            self.observe_head(block_number);

                            if block_number > *next_block {
                                warn!("Missing block {block_number} (next block: {next_block})");
                                return Err(anyhow!(
                                    "Missing block {block_number} (next block: {next_block})"
                                ));
                            }

                            while *next_block <= block_number {
                                self.prepare_and_stage_block(*next_block).await?;
                                self.flush_ready_blocks().await?;
                                *next_block += 1;
                                self.observe_head(block_number);
                            }
                        }
                        None => return Err(anyhow!("block subscription completed")),
                    }
                }
            }
        }
    }

    async fn prepare_and_stage_block(&mut self, block_number: u64) -> Result<()> {
        if let Some(update) = self.worker.prepare_block_update(block_number).await? {
            self.staged_updates.insert(update.block_number, update);
            self.handle.status.set_highest_staged_block(
                self.staged_updates
                    .last_key_value()
                    .map(|(block, _)| *block),
            );

            if self.auto_advance_commit_target {
                self.handle.control.publish(block_number);
            }
        }
        Ok(())
    }

    async fn flush_ready_blocks(&mut self) -> Result<()> {
        let Some(commit_target) = self.handle.control.current_target() else {
            self.handle.status.set_highest_staged_block(
                self.staged_updates
                    .last_key_value()
                    .map(|(block, _)| *block),
            );
            return Ok(());
        };

        while let Some((&block_number, _)) = self.staged_updates.first_key_value() {
            if block_number > commit_target {
                break;
            }

            let update = self
                .staged_updates
                .get(&block_number)
                .cloned()
                .expect("staged block should exist");
            self.worker.commit_prepared_update(update).await?;
            self.staged_updates.remove(&block_number);
            self.handle
                .status
                .set_mdbx_synced_through(Some(block_number));
        }

        self.handle.status.set_highest_staged_block(
            self.staged_updates
                .last_key_value()
                .map(|(block, _)| *block),
        );
        self.refresh_sync_metrics();
        Ok(())
    }

    fn observe_head(&self, head_block: u64) {
        self.handle.status.set_latest_head_seen(Some(head_block));
        self.refresh_sync_metrics();

        if self.auto_advance_commit_target {
            self.handle.control.publish(head_block);
        }
    }

    fn refresh_sync_metrics(&self) {
        let snapshot = self.handle.status.snapshot();
        let head_block = snapshot
            .latest_head_seen
            .or(snapshot.mdbx_synced_through)
            .unwrap_or(0);
        let next_block = snapshot
            .mdbx_synced_through
            .map_or(0, |block| block.saturating_add(1));

        StateWorker::<WR>::update_sync_metrics(next_block, head_block);
    }
}

fn encode_block(block_number: Option<u64>) -> u64 {
    block_number.unwrap_or(NONE_BLOCK_SENTINEL)
}

fn decode_block(block_number: u64) -> Option<u64> {
    if block_number == NONE_BLOCK_SENTINEL {
        None
    } else {
        Some(block_number)
    }
}

#[cfg(test)]
mod tests {
    use super::EmbeddedStateWorkerRuntime;
    use crate::{
        connect_provider,
        integration_tests::mdbx_fixture::MdbxTestDir,
        state,
        system_calls::SystemCalls,
        worker::StateWorker,
    };
    use alloy::primitives::{
        B256,
        U256,
    };
    use anyhow::Result;
    use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
    use mdbx::{
        AccountState,
        AddressHash,
        BlockStateUpdate,
        StateWriter,
    };
    use std::{
        collections::HashMap,
        time::Duration,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_failure_keeps_staged_block() -> Result<()> {
        let _mdbx_dir = MdbxTestDir::new().map_err(anyhow::Error::msg)?;
        let http_server_mock = DualProtocolMockServer::new()
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        let provider = connect_provider(&http_server_mock.ws_url()).await?;
        let writer_reader = StateWriter::new(_mdbx_dir.path_str().map_err(anyhow::Error::msg)?)?;
        let trace_provider =
            state::create_trace_provider(provider.clone(), Duration::from_secs(30));
        let worker = StateWorker::new(
            provider,
            trace_provider,
            writer_reader,
            None,
            SystemCalls::default(),
        );
        let mut runtime = EmbeddedStateWorkerRuntime::new(worker);

        let duplicate_address = AddressHash::from_hash(B256::repeat_byte(0x11));
        let duplicate_account = AccountState {
            address_hash: duplicate_address,
            balance: U256::ZERO,
            nonce: 0,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        };
        runtime.staged_updates.insert(
            1,
            BlockStateUpdate {
                block_number: 1,
                block_hash: B256::repeat_byte(0x22),
                state_root: B256::repeat_byte(0x33),
                accounts: vec![duplicate_account.clone(), duplicate_account],
            },
        );
        runtime.handle.control.publish(1);

        let _err = runtime.flush_ready_blocks().await.unwrap_err();
        assert!(runtime.staged_updates.contains_key(&1));
        assert_eq!(runtime.handle.status.snapshot().mdbx_synced_through, None);
        Ok(())
    }
}
