//! JSON-RPC server handlers for HTTP transport

use crate::{
    engine::queue::{
        TransactionQueueSender,
        TxQueueContents,
    },
    transport::decoder::{
        Decoder,
        HttpTransactionDecoder,
    },
};
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
    error,
    instrument,
    trace,
};

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
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

    pub fn internal_error(request: &JsonRpcRequest, message: &str) -> Self {
        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code: -32603,
                message: message.to_string(),
            }),
            id: request.id.clone(),
        }
    }

    pub fn invalid_params(request: &JsonRpcRequest, message: &str) -> Self {
        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code: -32602,
                message: message.to_string(),
            }),
            id: request.id.clone(),
        }
    }

    pub fn method_not_found(request: &JsonRpcRequest) -> Self {
        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code: -32601,
                message: "Method not found".to_string(),
            }),
            id: request.id.clone(),
        }
    }

    pub fn success(request: &JsonRpcRequest, result: serde_json::Value) -> Self {
        JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
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
    pub tx_sender: TransactionQueueSender,
}

impl ServerState {
    pub fn new(has_blockenv: Arc<AtomicBool>, tx_sender: TransactionQueueSender) -> Self {
        Self {
            has_blockenv,
            tx_sender,
        }
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
                // Parse the params to validate the schema
                match &request.params {
                    Some(params) => {
                        match serde_json::from_value::<SendTransactionsParams>(params.clone()) {
                            Ok(send_params) => {
                                let transaction_count = send_params.transactions.len();
                                let mut processed_count = 0;

                                // Process each transaction individually
                                for _raw_tx in &send_params.transactions {
                                    // Create a temporary JsonRpcRequest for each transaction
                                    let tx_request = JsonRpcRequest {
                                        jsonrpc: request.jsonrpc.clone(),
                                        method: request.method.clone(),
                                        params: Some(serde_json::json!({
                                            "transactions": vec![_raw_tx]
                                        })),
                                        id: request.id.clone(),
                                    };

                                    match HttpTransactionDecoder::to_transaction(tx_request) {
                                        Ok(queue_tx) => {
                                            // Send the decoded transaction to the queue
                                            if let Err(e) =
                                                state.tx_sender.send(TxQueueContents::Tx(queue_tx))
                                            {
                                                error!(
                                                    error = %e,
                                                    "Failed to send transaction to queue from transport server"
                                                );
                                                return Ok(ResponseJson(
                                                    JsonRpcResponse::internal_error(
                                                        &request,
                                                        "Internal error: failed to queue transaction",
                                                    ),
                                                ));
                                            }
                                            processed_count += 1;
                                        }
                                        Err(e) => {
                                            error!(
                                                error = %e,
                                                "Failed to decode transaction"
                                            );
                                            return Ok(ResponseJson(
                                                JsonRpcResponse::invalid_params(
                                                    &request,
                                                    &format!("Failed to decode transaction: {}", e),
                                                ),
                                            ));
                                        }
                                    }
                                }

                                debug!(
                                    transaction_count = transaction_count,
                                    processed_count = processed_count,
                                    "Successfully processed transaction batch"
                                );

                                JsonRpcResponse::success(
                                    &request,
                                    serde_json::json!({
                                        "status": "accepted",
                                        "transaction_count": processed_count,
                                        "message": format!("Successfully processed {} transactions", processed_count)
                                    }),
                                )
                            }
                            Err(e) => {
                                debug!(
                                    error = %e,
                                    "Failed to parse sendTransactions parameters"
                                );

                                JsonRpcResponse::invalid_params(
                                    &request,
                                    &format!("Invalid params: {}", e),
                                )
                            }
                        }
                    }
                    None => {
                        debug!("sendTransactions request missing required parameters");

                        JsonRpcResponse::invalid_params(
                            &request,
                            "Missing params for sendTransactions",
                        )
                    }
                }
            }
        }
        _ => {
            debug!(
                method = %request.method,
                "Unknown JSON-RPC method requested"
            );

            JsonRpcResponse::method_not_found(&request)
        }
    };

    debug!(
        has_error = response.error.is_some(),
        "Sending JSON-RPC response"
    );

    Ok(ResponseJson(response))
}
