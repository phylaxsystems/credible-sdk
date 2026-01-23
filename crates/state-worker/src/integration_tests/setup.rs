//! Test setup and instance management for integration tests.
//!
//! This module provides the `TestInstance` struct which abstracts over
//! the MDBX database backend for running integration tests.

#![allow(dead_code)]

use crate::{
    connect_provider,
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
use tokio::sync::broadcast;
use tracing::error;

/// Unified test instance for MDBX-backed integration tests.
pub struct TestInstance {
    /// Mock server for HTTP/WS protocol simulation.
    pub http_server_mock: DualProtocolMockServer,
    /// Handle to the running worker task.
    pub handle_worker: tokio::task::JoinHandle<()>,
    // Backend-specific handles for cleanup.
    mdbx_dir: MdbxTestDir,
    // Pre-created reader for MDBX (shares the same db connection).
    mdbx_reader: mdbx::StateReader,
}

impl TestInstance {
    /// Get MDBX path for this instance.
    pub fn mdbx_path(&self) -> &str {
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
        use mdbx::{
            StateWriter,
            common::CircularBufferConfig,
        };

        let mdbx_dir = MdbxTestDir::new()?;

        let http_server_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create the http server mock");

        setup(&http_server_mock);

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let config = CircularBufferConfig::new(3).map_err(|e| e.to_string())?;

        // For MDBX, StateWriter implements both Reader and Writer.
        let writer_reader = StateWriter::new(mdbx_dir.path_str(), config.clone())
            .map_err(|e| format!("Failed to initialize MDBX writer: {e}"))?;

        // Clone the reader BEFORE the worker takes ownership of the writer.
        // This works because StateDb uses Arc<DatabaseEnv> internally,
        // so the cloned reader shares the same underlying database connection.
        let mdbx_reader = writer_reader.reader().clone();

        let trace_provider =
            state::create_trace_provider(provider.clone(), Duration::from_secs(30));

        // Extract fork timestamps from genesis if available, otherwise use defaults.
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
            mdbx_dir,
            mdbx_reader,
        })
    }

    /// Create a reader for this instance's MDBX backend.
    ///
    /// Returns a clone of the reader that shares the same
    /// database connection as the writer (via Arc<DatabaseEnv>).
    pub fn create_reader(&self) -> Box<dyn ReaderHelper> {
        let reader = self.mdbx_reader.clone();
        Box::new(MdbxReaderWrapper(reader))
    }
}

/// Helper trait to abstract over reader types for common test operations.
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

// For MDBX, use StateReader which shares the database connection via Arc.
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
