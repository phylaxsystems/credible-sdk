//! Test setup and instance management for integration tests.
//!
//! This module provides the `TestInstance` struct which abstracts over
//! different database backends (Redis, MDBX) for running integration tests.

#![allow(dead_code)]

use crate::{
    connect_provider,
    genesis::GenesisState,
    integration_tests::{
        mdbx_fixture::MdbxTestDir,
        redis_fixture::get_shared_redis,
    },
    state,
    system_calls::SystemCalls,
    worker::StateWorker,
};
use alloy::primitives::{
    B256,
    U256,
};
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use state_store::{
    AddressHash,
    Reader,
    Writer,
};
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::error;

/// Enum to identify the database backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestBackend {
    Redis,
    Mdbx,
}

/// Unified test instance that works with any database backend.
pub struct TestInstance {
    /// Mock server for HTTP/WS protocol simulation
    pub http_server_mock: DualProtocolMockServer,
    /// Handle to the running worker task
    pub handle_worker: tokio::task::JoinHandle<()>,
    /// Which backend this instance is using
    pub backend: TestBackend,
    /// Namespace for test isolation
    pub namespace: String,
    // Backend-specific handles for cleanup
    mdbx_dir: Option<MdbxTestDir>,
    redis_url: Option<String>,
    // Pre-created reader for MDBX (shares the same db connection)
    mdbx_reader: Option<state_store::mdbx::StateReader>,
}

impl TestInstance {
    /// Get Redis URL (only available for Redis backend)
    pub fn redis_url(&self) -> Option<&str> {
        self.redis_url.as_deref()
    }

    /// Get MDBX path (only available for MDBX backend)
    pub fn mdbx_path(&self) -> Option<&str> {
        self.mdbx_dir.as_ref().map(MdbxTestDir::path_str)
    }

    pub async fn new_redis() -> Result<Self, String> {
        Self::new_redis_internal(|_| {}, None).await
    }

    pub async fn new_redis_with_setup<F>(setup: F) -> Result<Self, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        Self::new_redis_internal(setup, None).await
    }

    pub async fn new_redis_with_genesis(genesis: GenesisState) -> Result<Self, String> {
        Self::new_redis_internal(|_| {}, Some(genesis)).await
    }

    pub async fn new_redis_with_setup_and_genesis<F>(
        setup: F,
        genesis: Option<GenesisState>,
    ) -> Result<Self, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        Self::new_redis_internal(setup, genesis).await
    }

    async fn new_redis_internal<F>(
        setup: F,
        genesis_state: Option<GenesisState>,
    ) -> Result<Self, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        use state_store::redis::{
            CircularBufferConfig,
            StateWriter,
        };

        let namespace = format!("state_worker_test:{}", uuid::Uuid::new_v4());

        let http_server_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create the http server mock");

        setup(&http_server_mock);

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let redis = get_shared_redis().await;
        let redis_url = redis.url.clone();

        let writer = StateWriter::new(
            &redis_url,
            &namespace,
            CircularBufferConfig::new(3).map_err(|e| e.to_string())?,
        )
        .map_err(|e| format!("Failed to initialize redis writer: {e}"))?;

        let trace_provider =
            state::create_trace_provider(provider.clone(), Duration::from_secs(30));

        // Extract fork timestamps from genesis if available, otherwise use defaults
        let system_calls = genesis_state
            .as_ref()
            .map(|g| SystemCalls::new(g.config().cancun_time, g.config().prague_time))
            .unwrap_or_default();

        let mut worker = StateWorker::new(
            provider,
            trace_provider,
            writer,
            genesis_state,
            system_calls,
        );
        let (shutdown_tx, _) = broadcast::channel(1);
        let handle_worker = tokio::spawn(async move {
            if let Err(e) = worker.run(Some(0), shutdown_tx.subscribe()).await {
                error!("worker server error: {}", e);
            }
        });

        Ok(Self {
            http_server_mock,
            handle_worker,
            backend: TestBackend::Redis,
            namespace,
            mdbx_dir: None,
            redis_url: Some(redis_url),
            mdbx_reader: None,
        })
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
        use state_store::mdbx::{
            StateWriter,
            common::CircularBufferConfig,
        };

        let mdbx_dir = MdbxTestDir::new()?;
        let namespace = format!("test_{}", uuid::Uuid::new_v4());

        let http_server_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create the http server mock");

        setup(&http_server_mock);

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let config = CircularBufferConfig::new(3).map_err(|e| e.to_string())?;

        // For MDBX, StateWriter implements both Reader and Writer
        let writer_reader = StateWriter::new(mdbx_dir.path_str(), config.clone())
            .map_err(|e| format!("Failed to initialize MDBX writer: {e}"))?;

        writer_reader
            .ensure_dump_index_metadata()
            .map_err(|e| format!("Failed to ensure dump index metadata: {e}"))?;

        // Clone the reader BEFORE the worker takes ownership of the writer.
        // This works because StateDb uses Arc<DatabaseEnv> internally,
        // so the cloned reader shares the same underlying database connection.
        let mdbx_reader = writer_reader.reader().clone();

        let trace_provider =
            state::create_trace_provider(provider.clone(), Duration::from_secs(30));

        // Extract fork timestamps from genesis if available, otherwise use defaults
        let system_calls = genesis_state
            .as_ref()
            .map(|g| {
                SystemCalls::new(
                    Some(g.config().cancun_time.unwrap_or_default()),
                    Some(g.config().prague_time.unwrap_or_default()),
                )
            })
            .unwrap_or_default();

        let mut worker = StateWorker::new(
            provider,
            trace_provider,
            writer_reader,
            genesis_state,
            system_calls,
        );
        let (shutdown_tx, _) = broadcast::channel(1);
        let handle_worker = tokio::spawn(async move {
            if let Err(e) = worker.run(Some(0), shutdown_tx.subscribe()).await {
                error!("worker server error: {}", e);
            }
        });

        Ok(Self {
            http_server_mock,
            handle_worker,
            backend: TestBackend::Mdbx,
            namespace,
            mdbx_dir: Some(mdbx_dir),
            redis_url: None,
            mdbx_reader: Some(mdbx_reader),
        })
    }

    /// Create a reader for this instance's backend.
    ///
    /// For MDBX, this returns a clone of the reader that shares the same
    /// database connection as the writer (via Arc<DatabaseEnv>).
    /// For Redis, this creates a new connection.
    pub fn create_reader(&self) -> Result<Box<dyn ReaderHelper>, String> {
        match self.backend {
            TestBackend::Redis => {
                use state_store::redis::{
                    CircularBufferConfig,
                    StateReader,
                };
                let reader = StateReader::new(
                    self.redis_url.as_ref().unwrap(),
                    &self.namespace,
                    CircularBufferConfig::new(3).map_err(|e| e.to_string())?,
                )
                .map_err(|e| format!("Failed to create Redis reader: {e}"))?;
                Ok(Box::new(RedisReaderWrapper(reader)))
            }
            TestBackend::Mdbx => {
                // Use the pre-cloned reader that shares the database connection
                let reader = self
                    .mdbx_reader
                    .as_ref()
                    .ok_or("MDBX reader not available")?
                    .clone();
                Ok(Box::new(MdbxReaderWrapper(reader)))
            }
        }
    }
}

/// Helper trait to abstract over reader types for common test operations.
pub trait ReaderHelper: Send + Sync {
    fn get_account_boxed(
        &self,
        address_hash: AddressHash,
        block: u64,
    ) -> Result<Option<state_store::AccountInfo>, String>;

    fn get_storage_boxed(
        &self,
        address_hash: AddressHash,
        slot_hash: B256,
        block: u64,
    ) -> Result<Option<U256>, String>;

    fn latest_block_number_boxed(&self) -> Result<Option<u64>, String>;
}

struct RedisReaderWrapper(state_store::redis::StateReader);

impl ReaderHelper for RedisReaderWrapper {
    fn get_account_boxed(
        &self,
        address_hash: AddressHash,
        block: u64,
    ) -> Result<Option<state_store::AccountInfo>, String> {
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

// For MDBX, use StateReader which shares the database connection via Arc
struct MdbxReaderWrapper(state_store::mdbx::StateReader);

impl ReaderHelper for MdbxReaderWrapper {
    fn get_account_boxed(
        &self,
        address_hash: AddressHash,
        block: u64,
    ) -> Result<Option<state_store::AccountInfo>, String> {
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
