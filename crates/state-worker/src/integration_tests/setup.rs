#![allow(dead_code)]
use crate::{
    connect_provider,
    genesis::GenesisState,
    redis::{
        CircularBufferConfig,
        RedisStateWriter,
    },
    worker::StateWorker,
};
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use testcontainers::{
    ContainerAsync,
    runners::AsyncRunner,
};
use testcontainers_modules::redis::Redis;
use tracing::error;

pub(in crate::integration_tests) struct LocalInstance {
    pub http_server_mock: DualProtocolMockServer,
    pub handle_worker: tokio::task::JoinHandle<()>,
    pub redis_url: String,
    // Keep the container alive for the duration of the test
    _redis_container: ContainerAsync<Redis>,
}

impl LocalInstance {
    const NAMESPACE: &'static str = "state_worker_test";

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
        // Create the mock transport
        let http_server_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create the http server mock");

        setup(&http_server_mock);

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        // Start Redis container
        let redis_container = Redis::default()
            .start()
            .await
            .map_err(|e| format!("Failed to start Redis container: {e}"))?;

        let host = redis_container
            .get_host()
            .await
            .map_err(|e| format!("Failed to get Redis host: {e}"))?;

        let port = redis_container
            .get_host_port_ipv4(6379)
            .await
            .map_err(|e| format!("Failed to get Redis port: {e}"))?;

        let redis_url = format!("redis://{host}:{port}");

        let redis = RedisStateWriter::new(
            &redis_url,
            Self::NAMESPACE.to_string(),
            CircularBufferConfig::new(3),
        )
        .map_err(|e| format!("Failed to initialize redis client: {e}"))?;

        let mut worker = StateWorker::new(provider, redis, genesis_state);
        let handle_worker = tokio::spawn(async move {
            if let Err(e) = worker.run(Some(0)).await {
                error!("worker server error: {}", e);
            }
        });

        Ok(LocalInstance {
            http_server_mock,
            handle_worker,
            redis_url,
            _redis_container: redis_container,
        })
    }
}
