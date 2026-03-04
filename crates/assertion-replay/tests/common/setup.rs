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
    services::replay::{
        ReplayDurationTuning,
        notifier::ReplayResultNotifier,
    },
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
use tokio::sync::Mutex;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CALLBACK_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const CALLBACK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Clone)]
pub struct ReplayCallbackConfig {
    pub url: String,
    pub api_key: String,
}

#[derive(Debug, Clone)]
pub struct RecordedCallbackRequest {
    pub api_key: Option<String>,
    pub request_id: Option<String>,
    pub body: Value,
}

#[derive(Clone)]
struct CallbackState {
    recorded: Arc<Mutex<Vec<RecordedCallbackRequest>>>,
    status: axum::http::StatusCode,
}

pub struct CallbackCaptureServer {
    pub url: String,
    state: CallbackState,
    handle: tokio::task::JoinHandle<()>,
}

impl CallbackCaptureServer {
    pub async fn try_new(status: axum::http::StatusCode) -> Result<Self, String> {
        let bind_addr = available_bind_addr()?;
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|error| format!("failed to bind callback listener: {error}"))?;
        let state = CallbackState {
            recorded: Arc::new(Mutex::new(Vec::new())),
            status,
        };
        let app = axum::Router::new()
            .route(
                "/replay/result",
                axum::routing::post(callback_capture_handler),
            )
            .with_state(state.clone());
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Ok(Self {
            url: format!("http://{bind_addr}/replay/result"),
            state,
            handle,
        })
    }

    pub async fn wait_for_call_count(&self, expected_calls: usize) -> bool {
        let start = tokio::time::Instant::now();
        while start.elapsed() < CALLBACK_WAIT_TIMEOUT {
            if self.recorded().await.len() >= expected_calls {
                return true;
            }
            tokio::time::sleep(CALLBACK_WAIT_POLL_INTERVAL).await;
        }
        false
    }

    pub async fn recorded(&self) -> Vec<RecordedCallbackRequest> {
        self.state.recorded.lock().await.clone()
    }
}

impl Drop for CallbackCaptureServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub struct TestInstance {
    pub mock_node: DualProtocolMockServer,
    pub client: Client,
    pub base_url: String,
    _callback_server: Option<CallbackCaptureServer>,
    app_handle: tokio::task::JoinHandle<()>,
}

impl TestInstance {
    pub async fn try_new_with_callback(
        callback: Option<ReplayCallbackConfig>,
    ) -> Result<Self, String> {
        let mock_node = DualProtocolMockServer::new()
            .await
            .map_err(|error| format!("failed to start mock node: {error}"))?;
        let bind_addr = available_bind_addr()?;
        let head_provider = connect_head_provider(&mock_node.ws_url()).await?;
        let (callback, callback_server) = if let Some(callback) = callback {
            (callback, None)
        } else {
            let callback_server = CallbackCaptureServer::try_new(axum::http::StatusCode::OK)
                .await
                .map_err(|error| format!("failed to start callback test server: {error}"))?;
            (
                ReplayCallbackConfig {
                    url: callback_server.url.clone(),
                    api_key: "integration-callback-key".to_string(),
                },
                Some(callback_server),
            )
        };

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
            replay_result_callback_url: callback.url,
            replay_result_callback_api_key: callback.api_key,
        });
        let replay_duration_tuning = ReplayDurationTuning::from_config(config.as_ref());
        let replay_result_notifier = ReplayResultNotifier::from_config(config.as_ref())
            .map_err(|error| format!("failed to initialize replay result notifier: {error}"))?;
        let state = AppState {
            config,
            head_provider,
            replay_window: Arc::new(AtomicU64::new(1)),
            replay_duration_tuning,
            replay_result_notifier: Arc::new(replay_result_notifier),
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
            _callback_server: callback_server,
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

pub async fn require_test_instance(test_name: &str) -> Option<TestInstance> {
    require_test_instance_with_callback(test_name, None).await
}

pub async fn require_test_instance_with_callback(
    test_name: &str,
    callback: Option<ReplayCallbackConfig>,
) -> Option<TestInstance> {
    match TestInstance::try_new_with_callback(callback).await {
        Ok(instance) => Some(instance),
        Err(error) => {
            if error.contains("Operation not permitted") {
                eprintln!("{test_name}: skipped due to sandbox socket restrictions ({error})");
                None
            } else {
                panic!("{test_name}: failed to start integration test instance: {error}");
            }
        }
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

async fn callback_capture_handler(
    axum::extract::State(state): axum::extract::State<CallbackState>,
    headers: axum::http::HeaderMap,
    axum::extract::Json(body): axum::extract::Json<Value>,
) -> axum::http::StatusCode {
    let recorded = RecordedCallbackRequest {
        api_key: headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string),
        request_id: headers
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string),
        body,
    };
    state.recorded.lock().await.push(recorded);
    state.status
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
