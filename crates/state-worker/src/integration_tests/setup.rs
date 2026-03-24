//! Test setup and instance management for integration tests.

use crate::{
    connect_provider,
    embedded::{
        EmbeddedStateWorkerHandle,
        EmbeddedStateWorkerRuntime,
        WorkerStatusSnapshot,
    },
    genesis::GenesisState,
    integration_tests::mdbx_fixture::MdbxTestDir,
    state,
    system_calls::SystemCalls,
    worker::StateWorker,
};
use alloy::primitives::{
    B256,
    U256,
};
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use mdbx::{
    AddressHash,
    Reader,
};
use std::time::Duration;
use tokio::{
    sync::broadcast,
    time::sleep,
};
use tracing::error;

pub struct TestInstance {
    pub http_server_mock: DualProtocolMockServer,
    pub handle_worker: tokio::task::JoinHandle<()>,
    mdbx_dir: MdbxTestDir,
    mdbx_reader: mdbx::StateReader,
    worker_handle: EmbeddedStateWorkerHandle,
}

pub struct EmbeddedWorkerHarness {
    pub http_server_mock: DualProtocolMockServer,
    pub handle_worker: tokio::task::JoinHandle<()>,
    mdbx_dir: MdbxTestDir,
    worker_handle: EmbeddedStateWorkerHandle,
}

impl TestInstance {
    pub fn mdbx_path(&self) -> Result<&str, String> {
        self.mdbx_dir.path_str()
    }

    pub async fn new_mdbx() -> Result<Self, String> {
        Self::new_mdbx_internal(|_| {}, None).await
    }

    pub async fn new_mdbx_with_setup<F>(setup: F) -> Result<Self, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        Self::new_mdbx_internal(setup, None).await
    }

    pub async fn new_mdbx_with_genesis(genesis: GenesisState) -> Result<Self, String> {
        Self::new_mdbx_internal(|_| {}, Some(genesis)).await
    }

    pub async fn new_mdbx_with_setup_and_genesis<F>(
        setup: F,
        genesis: Option<GenesisState>,
    ) -> Result<Self, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        Self::new_mdbx_internal(setup, genesis).await
    }

    async fn new_mdbx_internal<F>(
        setup: F,
        genesis_state: Option<GenesisState>,
    ) -> Result<Self, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        let runtime = WorkerRuntimeParts::spawn(setup, genesis_state, true).await?;
        let reader = runtime.mdbx_reader.clone();

        Ok(Self {
            http_server_mock: runtime.http_server_mock,
            handle_worker: runtime.handle_worker,
            mdbx_dir: runtime.mdbx_dir,
            mdbx_reader: reader,
            worker_handle: runtime.worker_handle,
        })
    }

    pub fn create_reader(&self) -> Box<dyn ReaderHelper> {
        Box::new(MdbxReaderWrapper(self.mdbx_reader.clone()))
    }

    pub fn send_new_head(&self) {
        self.http_server_mock.send_new_head();
    }

    pub fn send_new_head_with_block_number(&self, block_number: u64) {
        self.http_server_mock
            .send_new_head_with_block_number(block_number);
    }

    pub fn publish_commit_target(&self, block_number: u64) {
        self.worker_handle.control.publish(block_number);
    }

    pub fn status(&self) -> Result<WorkerStatusSnapshot, String> {
        Ok(self.worker_handle.status.snapshot())
    }
}

impl EmbeddedWorkerHarness {
    pub async fn new() -> Result<Self, String> {
        let runtime = WorkerRuntimeParts::spawn(|_| {}, None, false).await?;
        Ok(Self {
            http_server_mock: runtime.http_server_mock,
            handle_worker: runtime.handle_worker,
            mdbx_dir: runtime.mdbx_dir,
            worker_handle: runtime.worker_handle,
        })
    }

    pub fn mdbx_path(&self) -> Result<&str, String> {
        self.mdbx_dir.path_str()
    }

    pub fn push_new_head(&self) {
        self.http_server_mock.send_new_head();
    }

    pub fn publish_commit_target(&self, block_number: u64) {
        self.worker_handle.control.publish(block_number);
    }

    pub fn status(&self) -> WorkerStatusSnapshot {
        self.worker_handle.status.snapshot()
    }

    pub async fn wait_for_staged(&self, block_number: u64) -> Result<WorkerStatusSnapshot, String> {
        self.wait_for_status(Duration::from_secs(2), |status| {
            status.highest_staged_block == Some(block_number)
        })
        .await
    }

    pub async fn wait_for_flushed(
        &self,
        block_number: u64,
    ) -> Result<WorkerStatusSnapshot, String> {
        self.wait_for_status(Duration::from_secs(2), |status| {
            status.mdbx_synced_through == Some(block_number)
        })
        .await
    }

    async fn wait_for_status<F>(
        &self,
        timeout: Duration,
        mut predicate: F,
    ) -> Result<WorkerStatusSnapshot, String>
    where
        F: FnMut(&WorkerStatusSnapshot) -> bool,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let status = self.status();
            if predicate(&status) {
                return Ok(status);
            }

            if tokio::time::Instant::now() >= deadline {
                return Ok(status);
            }

            sleep(Duration::from_millis(50)).await;
        }
    }
}

struct WorkerRuntimeParts {
    http_server_mock: DualProtocolMockServer,
    handle_worker: tokio::task::JoinHandle<()>,
    mdbx_dir: MdbxTestDir,
    mdbx_reader: mdbx::StateReader,
    worker_handle: EmbeddedStateWorkerHandle,
}

impl WorkerRuntimeParts {
    async fn spawn<F>(
        setup: F,
        genesis_state: Option<GenesisState>,
        auto_advance_commit_target: bool,
    ) -> Result<Self, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        use mdbx::StateWriter;

        let mdbx_dir = MdbxTestDir::new()?;
        let http_server_mock = DualProtocolMockServer::new()
            .await
            .map_err(|e| format!("Failed to create the http server mock: {e}"))?;
        setup(&http_server_mock);

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .map_err(|e| format!("Failed to connect to provider: {e}"))?;

        let mdbx_path = mdbx_dir.path_str()?;
        let writer_reader = StateWriter::new(mdbx_path)
            .map_err(|e| format!("Failed to initialize MDBX writer: {e}"))?;
        let mdbx_reader = writer_reader.reader().clone();

        let trace_provider =
            state::create_trace_provider(provider.clone(), Duration::from_secs(30));

        let system_calls = genesis_state
            .as_ref()
            .map(|g| {
                SystemCalls::new(
                    Some(g.config().cancun_time.unwrap_or_default()),
                    Some(g.config().prague_time.unwrap_or_default()),
                )
            })
            .unwrap_or_default();

        let worker = StateWorker::new(
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
        );
        let runtime = if auto_advance_commit_target {
            EmbeddedStateWorkerRuntime::new(worker).with_auto_advance_commit_target()
        } else {
            EmbeddedStateWorkerRuntime::new(worker)
        };

        let worker_handle = runtime.handle();
        let mut runtime = runtime;
        let (shutdown_tx, _) = broadcast::channel(1);
        let handle_worker = tokio::spawn(async move {
            if let Err(err) = runtime.run(Some(0), shutdown_tx.subscribe()).await {
                error!("worker runtime error: {}", err);
            }
        });

        Ok(Self {
            http_server_mock,
            handle_worker,
            mdbx_dir,
            mdbx_reader,
            worker_handle,
        })
    }
}

pub trait ReaderHelper: Send + Sync {
    fn get_account_boxed(
        &self,
        address_hash: AddressHash,
        block: u64,
    ) -> Result<Option<mdbx::AccountInfo>, String>;

    fn get_storage_boxed(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block: u64,
    ) -> Result<Option<U256>, String>;

    fn latest_block_number_boxed(&self) -> Result<Option<u64>, String>;
}

struct MdbxReaderWrapper(mdbx::StateReader);

impl ReaderHelper for MdbxReaderWrapper {
    fn get_account_boxed(
        &self,
        address_hash: AddressHash,
        block: u64,
    ) -> Result<Option<mdbx::AccountInfo>, String> {
        self.0
            .get_account(address_hash, block)
            .map_err(|e| e.to_string())
    }

    fn get_storage_boxed(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block: u64,
    ) -> Result<Option<U256>, String> {
        self.0
            .get_storage(address_hash, slot_hash, block)
            .map_err(|e| e.to_string())
    }

    fn latest_block_number_boxed(&self) -> Result<Option<u64>, String> {
        self.0.latest_block_number().map_err(|e| e.to_string())
    }
}
