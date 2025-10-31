use alloy::primitives::Address;
use axum::{
    Json,
    Router,
    extract::{
        WebSocketUpgrade,
        ws::{
            Message,
            WebSocket,
        },
    },
    response::{
        IntoResponse,
        Response,
    },
    routing::{
        get,
        post,
    },
};
use dashmap::DashMap;
use serde_json::{
    Value,
    json,
};
use std::sync::{
    Arc,
    atomic::{
        AtomicU64,
        Ordering,
    },
};
use tokio::{
    net::TcpListener,
    sync::broadcast,
};

/// Mock server that supports both HTTP and WebSocket JSON-RPC connections
#[derive(Debug, Clone)]
pub struct DualProtocolMockServer {
    responses: Arc<DashMap<String, Value>>,
    pub eth_balance_counter: Arc<DashMap<Address, u64>>,
    http_port: u16,
    current_block: Arc<AtomicU64>,
    ws_port: u16,
    notification_tx: broadcast::Sender<Value>,
}

impl DualProtocolMockServer {
    /// Create a new dual protocol mock server
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let http_listener = TcpListener::bind("127.0.0.1:0").await?;
        let ws_listener = TcpListener::bind("127.0.0.1:0").await?;

        let http_port = http_listener.local_addr()?.port();
        let ws_port = ws_listener.local_addr()?.port();

        let (notification_tx, _) = broadcast::channel(100);

        let server = Self {
            responses: Arc::new(DashMap::new()),
            eth_balance_counter: Arc::new(DashMap::new()),
            http_port,
            current_block: Arc::new(AtomicU64::new(0)),
            ws_port,
            notification_tx,
        };

        // Configure default responses for all RPC methods
        server.setup_default_responses();

        // Start HTTP server
        server.start_http_server(http_listener);

        // Start WebSocket server
        server.start_ws_server(ws_listener);

        Ok(server)
    }

    /// Send a new head with a specific block number
    /// This does NOT increment the current block number
    pub fn send_new_head_with_block_number(&self, new_block: u64) {
        // Alloy's subscribe_blocks expects a full Block object, not just header
        let block = json!({
            // Header fields
            "hash": format!("0x{:064x}", new_block),
            "parentHash": format!("0x{:064x}", new_block.saturating_sub(1)),
            "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            "miner": "0x0000000000000000000000000000000000000000",
            "stateRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "logsBloom": format!("0x{:0512}", 0),
            "difficulty": "0x0",
            "number": format!("0x{:x}", new_block),
            "gasLimit": "0x1c9c380",
            "gasUsed": "0x0",
            "timestamp": format!("0x{:x}", new_block * 12),
            "extraData": "0x",
            "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "nonce": "0x0000000000000000",

            // Block body fields (required by Alloy)
            "totalDifficulty": "0x0",
            "baseFeePerGas": "0x7",
            "size": "0x21c",
            "transactions": [],
            "uncles": []
        });

        // Notification with the full block
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": "SUBSCRIPTION_ID_PLACEHOLDER",
                "result": block
            }
        });

        if let Err(e) = self.notification_tx.send(notification) {
            println!("Failed to broadcast notification: {e:?}");
        } else {
            println!("Mock server: New head sent, block number: {new_block}");
        }
    }

    /// Send a new head (simulate new block arrival)
    /// This increments the current block number and broadcasts to WebSocket subscribers
    pub fn send_new_head(&self) {
        let new_block = self.current_block.fetch_add(1, Ordering::Relaxed) + 1;
        self.update_block_responses();

        // Alloy's subscribe_blocks expects a full Block object, not just header
        let block = json!({
            // Header fields
            "hash": format!("0x{:064x}", new_block),
            "parentHash": format!("0x{:064x}", new_block.saturating_sub(1)),
            "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            "miner": "0x0000000000000000000000000000000000000000",
            "stateRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "logsBloom": format!("0x{:0512}", 0),
            "difficulty": "0x0",
            "number": format!("0x{:x}", new_block),
            "gasLimit": "0x1c9c380",
            "gasUsed": "0x0",
            "timestamp": format!("0x{:x}", new_block * 12),
            "extraData": "0x",
            "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "nonce": "0x0000000000000000",

            // Block body fields (required by Alloy)
            "totalDifficulty": "0x0",
            "baseFeePerGas": "0x7",
            "size": "0x21c",
            "transactions": [],
            "uncles": []
        });

        // Notification with the full block
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": "SUBSCRIPTION_ID_PLACEHOLDER",
                "result": block
            }
        });

        if let Err(e) = self.notification_tx.send(notification) {
            println!("Failed to broadcast notification: {e:?}");
        } else {
            println!("Mock server: New head sent, block number: {new_block}");
        }
    }

    /// Setup default empty/valid responses for all RPC methods
    fn setup_default_responses(&self) {
        // Default balance: 0
        let balance_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0x0"
        });
        self.responses
            .insert("eth_getBalance".to_string(), balance_response);

        // Default block number response
        let block_number_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": format!("0x{:x}", self.current_block.load(Ordering::Acquire))
        });

        self.responses
            .insert("eth_blockNumber".to_string(), block_number_response);

        // Default nonce: 0
        let nonce_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0x0"
        });
        self.responses
            .insert("eth_getTransactionCount".to_string(), nonce_response);

        // Default code: empty (0x)
        let code_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0x"
        });
        self.responses
            .insert("eth_getCode".to_string(), code_response);

        // Default storage: zero value
        let storage_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": "0x0000000000000000000000000000000000000000000000000000000000000000"
        });
        self.responses
            .insert("eth_getStorageAt".to_string(), storage_response);

        let trace_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": []  // Empty array for blocks with no transactions
        });
        self.responses
            .insert("debug_traceBlockByHash".to_string(), trace_response.clone());
        self.responses
            .insert("debug_traceBlockByNumber".to_string(), trace_response);

        let replay_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": []
        });
        self.responses
            .insert("trace_replayBlockTransactions".to_string(), replay_response);

        // Default block: genesis-like block
        let block_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                "miner": "0x0000000000000000000000000000000000000000",
                "number": "0x0",
                "stateRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                "logsBloom": format!("0x{:0512}", 0),
                "difficulty": "0x0",
                "number": "0x0",
                "timestamp": "0x0",
                "extraData": "0x",
                "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "nonce": "0x0000000000000000",
                "gasLimit": "0x1c9c380",
                "gasUsed": "0x0",
                "transactions": []
            }
        });
        self.responses
            .insert("eth_getBlockByNumber".to_string(), block_response.clone());

        // Also handle the block by hash variant
        self.responses
            .insert("eth_getBlockByHash".to_string(), block_response.clone());
    }

    fn update_block_responses(&self) {
        let current_block = self.current_block.load(Ordering::Acquire);

        // Update block number response
        let block_number_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": format!("0x{:x}", current_block)
        });
        self.responses
            .insert("eth_blockNumber".to_string(), block_number_response);

        // Update block by number response
        let block_response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "hash": format!("0x{:064x}", current_block),
                "parentHash": format!("0x{:064x}", current_block.saturating_sub(1)),
                "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                "miner": "0x0000000000000000000000000000000000000000",
                "stateRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                "logsBloom": format!("0x{:0512}", 0),
                "difficulty": "0x0",
                "number": format!("0x{:x}", current_block),
                "gasLimit": "0x1c9c380",
                "gasUsed": "0x0",
                "timestamp": format!("0x{:x}", current_block * 12),
                "extraData": "0x",
                "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "nonce": "0x0000000000000000",
                "totalDifficulty": "0x0",
                "baseFeePerGas": "0x7",
                "size": "0x21c",
                "transactions": [],
                "uncles": []
            }
        });
        self.responses
            .insert("eth_getBlockByNumber".to_string(), block_response.clone());

        // Also handle the block by hash variant
        self.responses
            .insert("eth_getBlockByHash".to_string(), block_response);
    }

    /// Get HTTP server URL
    pub fn http_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }

    /// Get WebSocket server URL
    pub fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.ws_port)
    }

    /// Add a mock response for a specific method
    pub fn add_response(&self, method: &str, response: Value) {
        self.responses.insert(method.to_string(), response);
    }

    /// Mock an RPC error response
    pub fn mock_rpc_error(&self, method: &str, error_code: i32, error_message: &str) {
        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": error_code,
                "message": error_message
            }
        });
        self.add_response(method, response);
    }

    /// Start HTTP server
    fn start_http_server(&self, listener: TcpListener) {
        let responses = Arc::clone(&self.responses);
        let basic_ref_counter = Arc::clone(&self.eth_balance_counter);

        tokio::spawn(async move {
            let app = Router::new()
                .route("/", post(Self::handle_http_request))
                .with_state((responses, basic_ref_counter));

            let server = axum::serve(listener, app);

            if let Err(e) = server.await {
                eprintln!("HTTP server error: {e}");
            }
        });
    }

    /// Start WebSocket server
    fn start_ws_server(&self, listener: TcpListener) {
        let responses = Arc::clone(&self.responses);
        let basic_ref_counter = Arc::clone(&self.eth_balance_counter);
        let notification_tx = self.notification_tx.clone();

        tokio::spawn(async move {
            let app = Router::new()
                .route("/", get(Self::handle_ws_upgrade))
                .with_state((responses, basic_ref_counter, notification_tx));

            let server = axum::serve(listener, app);

            if let Err(e) = server.await {
                eprintln!("WebSocket server error: {e}");
            }
        });
    }

    /// Handle HTTP requests
    #[allow(clippy::type_complexity)]
    async fn handle_http_request(
        axum::extract::State((responses, basic_ref_counter)): axum::extract::State<(
            Arc<DashMap<String, Value>>,
            Arc<DashMap<Address, u64>>,
        )>,
        Json(request): Json<Value>,
    ) -> Response {
        let response = Self::generate_response(&request, &responses, &basic_ref_counter);
        Json(response).into_response()
    }

    /// Handle WebSocket connections
    async fn handle_ws_connection(
        mut socket: WebSocket,
        responses: Arc<DashMap<String, Value>>,
        basic_ref_counter: Arc<DashMap<Address, u64>>,
        notification_tx: broadcast::Sender<Value>,
    ) {
        let mut notification_rx = notification_tx.subscribe();
        let mut subscription_id: Option<String> = None;

        loop {
            tokio::select! {
                msg = socket.recv() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(request) = serde_json::from_str::<Value>(&text) {
                                let method = request.get("method").and_then(|m| m.as_str()).unwrap_or("");

                                // Handle subscription requests
                                if method == "eth_subscribe" {
                                    let params = request.get("params").and_then(|p| p.as_array());
                                    let subscription_type = params
                                        .and_then(|p| p.first())
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("");

                                    println!("Subscription request for: {subscription_type}");

                                    if subscription_type == "newHeads" {
                                        // Generate a unique subscription ID
                                        let sub_id = format!("0x{:x}", rand::random::<u64>());
                                        subscription_id = Some(sub_id.clone());

                                        // Send successful subscription response
                                        let response = json!({
                                            "jsonrpc": "2.0",
                                            "id": request.get("id").cloned().unwrap_or(json!(1)),
                                            "result": sub_id
                                        });

                                        let response_text = serde_json::to_string(&response).unwrap();
                                        if let Err(e) = socket.send(Message::Text(response_text.into())).await {
                                            eprintln!("WebSocket send error: {e}");
                                            break;
                                        }

                                        println!("Subscription confirmed with ID: {sub_id}");
                                    }
                                } else {
                                    // Handle regular RPC requests
                                    let response = Self::generate_response(&request, &responses, &basic_ref_counter);
                                    let response_text = serde_json::to_string(&response).unwrap();

                                    if let Err(e) = socket.send(Message::Text(response_text.into())).await {
                                        eprintln!("WebSocket send error: {e}");
                                        break;
                                    }
                                }
                            }
                        }
                        _ => break,
                    }
                }

                // Handle notifications to broadcast
                notification = notification_rx.recv() => {
                    if let Some(ref sub_id) = subscription_id {
                        match notification {
                            Ok(mut notification_value) => {
                                // Update the subscription ID in the notification
                                if let Some(params) = notification_value.get_mut("params") && let Some(params_obj) = params.as_object_mut() {
                                        params_obj.insert("subscription".to_string(), json!(sub_id));
                                }

                                println!("Broadcasting notification with subscription ID: {sub_id}");
                                let notification_text = serde_json::to_string(&notification_value).unwrap();
                                if let Err(e) = socket.send(Message::Text(notification_text.into())).await {
                                    eprintln!("WebSocket notification send error: {e}");
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Notification receive error: {e:?}");
                            }
                        }
                    }
                }
            }
        }
    }

    /// Handle WebSocket upgrade
    async fn handle_ws_upgrade(
        ws: WebSocketUpgrade,
        axum::extract::State((responses, basic_ref_counter, notification_tx)): axum::extract::State<
            (
                Arc<DashMap<String, Value>>,
                Arc<DashMap<Address, u64>>,
                broadcast::Sender<Value>,
            ),
        >,
    ) -> Response {
        ws.on_upgrade(move |socket| {
            Self::handle_ws_connection(socket, responses, basic_ref_counter, notification_tx)
        })
    }

    /// Generate JSON-RPC response based on request and track `basic_ref` calls
    fn generate_response(
        request: &Value,
        responses: &Arc<DashMap<String, Value>>,
        basic_ref_get_balance: &Arc<DashMap<Address, u64>>,
    ) -> Value {
        let method = request.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let id = request.get("id").cloned().unwrap_or(json!(1));

        // Track calls that correspond to basic_ref operations
        if matches!(method, "eth_getBalance" | "eth_getProof") {
            if let Some(params) = request.get("params").and_then(|p| p.as_array()) {
                if let Some(address_str) = params.first().and_then(|addr| addr.as_str()) {
                    if address_str.starts_with("0x") && address_str.len() == 42 {
                        if let Ok(address_bytes) = hex::decode(&address_str[2..]) {
                            if address_bytes.len() == 20 {
                                let mut address_array = [0u8; 20];
                                address_array.copy_from_slice(&address_bytes);
                                let address = Address::from(address_array);

                                basic_ref_get_balance
                                    .entry(address)
                                    .and_modify(|count| *count += 1)
                                    .or_insert(1);
                            }
                        }
                    }
                }
            }
        }

        if let Some(response) = responses.get(method) {
            let mut response = response.clone();
            // Ensure the response has the correct ID from the request
            if let Some(obj) = response.as_object_mut() {
                obj.insert("id".to_string(), id);
            }
            response
        } else {
            // Return default empty responses for unknown methods instead of errors
            match method {
                "eth_getBalance" => {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": "0x0"
                    })
                }
                "eth_getTransactionCount" => {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": "0x0"
                    })
                }
                "eth_getCode" => {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": "0x"
                    })
                }
                "eth_getStorageAt" => {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": "0x0000000000000000000000000000000000000000000000000000000000000000"
                    })
                }
                "eth_getBlockByNumber" | "eth_getBlockByHash" => {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                            "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                            "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                            "miner": "0x0000000000000000000000000000000000000000",
                            "stateRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                            "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                            "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                            "logsBloom": format!("0x{:0512}", 0),
                            "difficulty": "0x0",
                            "number": "0x0",
                            "timestamp": "0x0",
                            "extraData": "0x",
                            "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                            "nonce": "0x0000000000000000",
                            "gasLimit": "0x1c9c380",
                            "gasUsed": "0x0",
                            "totalDifficulty": "0x0",
                            "baseFeePerGas": "0x7",
                            "size": "0x21c",
                            "transactions": [],
                            "uncles": []
                        }
                    })
                }
                _ => {
                    json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": null
                    })
                }
            }
        }
    }
}
