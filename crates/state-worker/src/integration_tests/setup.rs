#![allow(dead_code)]
use crate::{
    cli::ProviderType,
    connect_provider,
    genesis::GenesisState,
    integration_tests::redis_fixture::get_shared_redis,
    state,
    worker::StateWorker,
};
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use state_store::{
    CircularBufferConfig,
    StateReader,
    StateWriter,
};
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::error;

pub(in crate::integration_tests) struct LocalInstance {
    pub http_server_mock: DualProtocolMockServer,
    pub handle_worker: tokio::task::JoinHandle<()>,
    pub redis_url: String,
    pub namespace: String,
}

impl LocalInstance {
    pub(in crate::integration_tests) async fn new() -> Result<LocalInstance, String> {
        Self::new_with_setup_and_genesis(|_| {}, None).await
    }

    pub(in crate::integration_tests) async fn new_with_setup<F>(
        setup: F,
    ) -> Result<LocalInstance, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        Self::new_with_setup_and_genesis(setup, None).await
    }

    pub(in crate::integration_tests) async fn new_with_setup_and_genesis<F>(
        setup: F,
        genesis_state: Option<GenesisState>,
    ) -> Result<LocalInstance, String>
    where
        F: FnOnce(&DualProtocolMockServer),
    {
        // Create unique namespace per test to avoid state pollution
        let namespace = format!("state_worker_test:{}", uuid::Uuid::new_v4());

        // Create the mock transport
        let http_server_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create the http server mock");

        setup(&http_server_mock);

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        // Get shared Redis container (reused across all tests)
        let redis = get_shared_redis().await;
        let redis_url = redis.url.clone();

        let writer = StateWriter::new(
            &redis_url,
            &namespace,
            CircularBufferConfig::new(3).map_err(|e| e.to_string())?,
        )
        .map_err(|e| format!("Failed to initialize redis client: {e}"))?;

        let reader = StateReader::new(
            &redis_url,
            &namespace,
            CircularBufferConfig::new(3).map_err(|e| e.to_string())?,
        )
        .map_err(|e| format!("Failed to initialize redis client: {e}"))?;

        // Create trace provider (using Parity for the mock server)
        let trace_provider = state::create_trace_provider(
            ProviderType::Parity,
            provider.clone(),
            Duration::from_secs(30),
        );

        let mut worker = StateWorker::new(provider, trace_provider, writer, reader, genesis_state);
        let (shutdown_tx, _) = broadcast::channel(1);
        let handle_worker = tokio::spawn(async move {
            if let Err(e) = worker.run(Some(0), shutdown_tx.subscribe()).await {
                error!("worker server error: {}", e);
            }
        });

        Ok(LocalInstance {
            http_server_mock,
            handle_worker,
            redis_url,
            namespace,
        })
    }
}
