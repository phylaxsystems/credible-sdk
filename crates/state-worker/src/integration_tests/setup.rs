#![allow(dead_code)]
use crate::{
    connect_provider,
    redis::RedisStateWriter,
    worker::StateWorker,
};
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::error;

pub(in crate::integration_tests) struct LocalInstance {
    pub http_server_mock: DualProtocolMockServer,
    pub handle_redis: tokio::task::JoinHandle<()>,
    pub handle_worder: tokio::task::JoinHandle<()>,
    pub redis_url: String,
}

impl LocalInstance {
    const NAMESPACE: &'static str = "state_worker_test";

    pub(in crate::integration_tests) async fn new() -> Result<LocalInstance, String> {
        // Create the mock transport
        let http_server_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create the http server mock");

        let provider = connect_provider(&http_server_mock.ws_url())
            .await
            .expect("Failed to connect to provider");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let redis_url = format!("redis://127.0.0.1:{}", addr.port());

        let handle_redis = tokio::spawn(async move {
            if let Err(e) = mini_redis::server::run(listener, tokio::signal::ctrl_c()).await {
                error!("Mini-redis server error: {}", e);
            }
        });

        let redis = RedisStateWriter::new(&redis_url, Self::NAMESPACE.to_string())
            .expect("failed to initialize redis client");

        let mut worker = StateWorker::new(provider, redis, Duration::from_secs(30));
        let handle_worder = tokio::spawn(async move {
            if let Err(e) = worker.run(Some(0)).await {
                error!("worker server error: {}", e);
            }
        });

        Ok(LocalInstance {
            http_server_mock,
            handle_redis,
            handle_worder,
            redis_url,
        })
    }
}
