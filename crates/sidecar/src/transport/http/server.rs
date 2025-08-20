//! JSON-RPC server handlers for HTTP transport

use axum::{
    extract::{
        Json,
        State,
    },
    http::StatusCode,
    response::Json as ResponseJson,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::sync::{
    Arc,
    atomic::{
        AtomicBool,
        Ordering,
    },
};
use tracing::{
    debug,
    instrument,
    trace,
};

#[derive(Debug, Deserialize)]
pub struct TransactionEnv {
    pub caller: String,
    pub gas_limit: u64,
    pub gas_price: String,
    pub transact_to: String,
    pub value: String,
    pub data: String,
    pub nonce: u64,
    pub chain_id: u64,
    pub access_list: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "txEnv")]
    pub tx_env: TransactionEnv,
    pub hash: String,
}

#[derive(Debug, Deserialize)]
pub struct SendTransactionsParams {
    pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<serde_json::Value>,
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

impl JsonRpcResponse {
    pub fn block_not_available(request: &JsonRpcRequest) -> Self {
        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code: -32003,
                message: "Block environment not available".to_string(),
            }),
            id: request.id.clone(),
        }
    }
}

/// Server state containing shared data
// FIXME: i dont like how we have to have a seprate data structure
// for holding server state but i dont have a better solution if we
// use axum. we can use other frameworks but id rather not
#[derive(Clone, Debug)]
pub struct ServerState {
    pub has_blockenv: Arc<AtomicBool>,
}

impl ServerState {
    pub fn new(has_blockenv: Arc<AtomicBool>) -> Self {
        Self { has_blockenv }
    }
}

/// Handle JSON-RPC requests for transactions
#[instrument(
    name = "http_server::handle_transaction_rpc",
    skip(state),
    fields(
        method = %request.method,
        jsonrpc = %request.jsonrpc,
        request_id = ?request.id
    ),
    level = "debug"
)]
pub async fn handle_transaction_rpc(
    State(state): State<ServerState>,
    Json(request): Json<JsonRpcRequest>,
) -> Result<ResponseJson<JsonRpcResponse>, StatusCode> {
    debug!("Processing JSON-RPC request");

    let response = match request.method.as_str() {
        "sendTransactions" => {
            trace!("Processing sendTransactions request");

            // Check if we have block environment before processing transactions
            if !state.has_blockenv.load(Ordering::Relaxed) {
                debug!("Rejecting transaction - no block environment available");
                JsonRpcResponse::block_not_available(&request)
            } else {
                trace!("Block environment available, parsing transaction parameters");

                // Parse the params to validate the schema
                match request.params {
                    Some(params) => {
                        match serde_json::from_value::<SendTransactionsParams>(params) {
                            Ok(send_params) => {
                                let transaction_count = send_params.transactions.len();
                                debug!(
                                    transaction_count = transaction_count,
                                    "Successfully parsed transaction batch"
                                );

                                // TODO: Process the transactions

                                JsonRpcResponse {
                                    jsonrpc: "2.0".to_string(),
                                    result: Some(serde_json::json!({
                                        "status": "accepted",
                                        "transaction_count": transaction_count,
                                        "message": format!("Successfully received {} transactions", transaction_count)
                                    })),
                                    error: None,
                                    id: request.id,
                                }
                            }
                            Err(e) => {
                                debug!(
                                    error = %e,
                                    "Failed to parse sendTransactions parameters"
                                );

                                JsonRpcResponse {
                                    jsonrpc: "2.0".to_string(),
                                    result: None,
                                    error: Some(JsonRpcError {
                                        code: -32602,
                                        message: format!("Invalid params: {}", e),
                                    }),
                                    id: request.id,
                                }
                            }
                        }
                    }
                    None => {
                        debug!("sendTransactions request missing required parameters");

                        JsonRpcResponse {
                            jsonrpc: "2.0".to_string(),
                            result: None,
                            error: Some(JsonRpcError {
                                code: -32602,
                                message: "Missing params for sendTransactions".to_string(),
                            }),
                            id: request.id,
                        }
                    }
                }
            }
        }
        _ => {
            debug!(
                method = %request.method,
                "Unknown JSON-RPC method requested"
            );

            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(JsonRpcError {
                    code: -32601,
                    message: "Method not found".to_string(),
                }),
                id: request.id,
            }
        }
    };

    debug!(
        has_error = response.error.is_some(),
        "Sending JSON-RPC response"
    );

    Ok(ResponseJson(response))
}
