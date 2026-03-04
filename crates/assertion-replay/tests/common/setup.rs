use alloy::providers::WsConnect;
use alloy_provider::{
    Provider,
    ProviderBuilder,
    RootProvider,
};
use assertion_replay::{
    config::Config,
    server::{
        AppState,
        app_router,
    },
    services::replay::ReplayDurationTuning,
};
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use reqwest::{
    Client,
    Response,
};
use serde_json::Value;
use std::{
    net::{
        SocketAddr,
        TcpListener,
    },
    sync::{
        Arc,
        atomic::AtomicU64,
    },
    time::Duration,
};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub struct TestInstance {
    pub mock_node: DualProtocolMockServer,
    pub client: Client,
    pub base_url: String,
    app_handle: tokio::task::JoinHandle<()>,
}

impl TestInstance {
    pub async fn try_new() -> Result<Self, String> {
        let mock_node = DualProtocolMockServer::new()
            .await
            .map_err(|error| format!("failed to start mock node: {error}"))?;
        let bind_addr = available_bind_addr()?;
        let head_provider = connect_head_provider(&mock_node.ws_url()).await?;

        let config = Arc::new(Config {
            bind_addr,
            archive_ws_url: mock_node.ws_url(),
            archive_http_url: mock_node.http_url(),
            replay_window: 1,
            chain_id: 1,
            assertion_gas_limit: 1_000_000_000,
            replay_duration_min_minutes: 10.0,
            replay_duration_target_minutes: 12.5,
            replay_duration_max_minutes: 15.0,
        });
        let replay_duration_tuning = ReplayDurationTuning::from_config(config.as_ref());
        let state = AppState {
            config,
            head_provider,
            replay_window: Arc::new(AtomicU64::new(1)),
            replay_duration_tuning,
        };
        let app = app_router(state);
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|error| format!("failed to bind app listener: {error}"))?;

        let app_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|error| format!("failed to build reqwest client: {error}"))?;
        let base_url = format!("http://{bind_addr}");

        let instance = Self {
            mock_node,
            client,
            base_url,
            app_handle,
        };
        instance.wait_until_healthy().await?;
        Ok(instance)
    }

    pub async fn get(&self, path: &str) -> Response {
        self.client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .await
            .expect("GET request should succeed")
    }

    pub async fn post_json(&self, path: &str, body: Value) -> Response {
        self.client
            .post(format!("{}{}", self.base_url, path))
            .json(&body)
            .send()
            .await
            .expect("POST request should succeed")
    }

    pub async fn post_raw_json(&self, path: &str, body: &str) -> Response {
        self.client
            .post(format!("{}{}", self.base_url, path))
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .expect("POST request should succeed")
    }

    async fn wait_until_healthy(&self) -> Result<(), String> {
        let start = tokio::time::Instant::now();
        while start.elapsed() < STARTUP_TIMEOUT {
            let response = self
                .client
                .get(format!("{}/health", self.base_url))
                .send()
                .await;
            if let Ok(response) = response
                && response.status().is_success()
            {
                return Ok(());
            }
            tokio::time::sleep(STARTUP_POLL_INTERVAL).await;
        }
        Err("assertion-replay app did not become healthy in time".to_string())
    }
}

impl Drop for TestInstance {
    fn drop(&mut self) {
        self.app_handle.abort();
    }
}

async fn connect_head_provider(ws_url: &str) -> Result<Arc<RootProvider>, String> {
    let ws = WsConnect::new(ws_url.to_string());
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .map_err(|error| format!("failed to connect websocket provider: {error}"))?;
    Ok(Arc::new(provider.root().clone()))
}

fn available_bind_addr() -> Result<SocketAddr, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|error| format!("failed to bind ephemeral port: {error}"))?;
    let addr = listener
        .local_addr()
        .map_err(|error| format!("failed to read ephemeral socket addr: {error}"))?;
    drop(listener);
    Ok(addr)
}
