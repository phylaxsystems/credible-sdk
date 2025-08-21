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

#[derive(Debug, Deserialize, Serialize)]
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

const JSON_RPC_METHOD_NOT_FOUND: i32 = -32601;
const INTERNAL_ERROR: i32 = -32003;

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

lazy_static::lazy_static! {
    static ref JSONRPCVER: String = "2.0".to_string();
}

impl JsonRpcResponse {
    pub fn block_not_available(request: &JsonRpcRequest) -> Self {
        JsonRpcResponse {
            jsonrpc: JSONRPCVER.clone(),
            result: None,
            error: Some(JsonRpcError {
                code: INTERNAL_ERROR,
                message: "Block environment not available".to_string(),
            }),
            id: request.id.clone(),
        }
    }

    pub fn internal_error(request: &JsonRpcRequest, message: &str) -> Self {
        JsonRpcResponse {
            jsonrpc: JSONRPCVER.clone(),
            result: None,
            error: Some(JsonRpcError {
                code: INTERNAL_ERROR,
                message: message.to_string(),
            }),
            id: request.id.clone(),
        }
    }

    pub fn invalid_params(request: &JsonRpcRequest, message: &str) -> Self {
        JsonRpcResponse {
            jsonrpc: JSONRPCVER.clone(),
            result: None,
            error: Some(JsonRpcError {
                code: INTERNAL_ERROR,
                message: message.to_string(),
            }),
            id: request.id.clone(),
        }
    }

    pub fn method_not_found(request: &JsonRpcRequest) -> Self {
        JsonRpcResponse {
            jsonrpc: JSONRPCVER.clone(),
            result: None,
            error: Some(JsonRpcError {
                code: JSON_RPC_METHOD_NOT_FOUND,
                message: "Method not found".to_string(),
            }),
            id: request.id.clone(),
        }
    }

    pub fn success(request: &JsonRpcRequest, result: serde_json::Value) -> Self {
        JsonRpcResponse {
            jsonrpc: JSONRPCVER.clone(),
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

    let response = if request.method != "sendTransactions" {
        debug!(
            method = %request.method,
            "Unknown JSON-RPC method requested"
        );
        JsonRpcResponse::method_not_found(&request)
    } else {
        handle_send_transactions(&state, &request).await?
    };

    debug!(
        has_error = response.error.is_some(),
        "Sending JSON-RPC response"
    );

    Ok(ResponseJson(response))
}

async fn handle_send_transactions(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    trace!("Processing sendTransactions request");

    // Check if we have block environment before processing transactions
    if !state.has_blockenv.load(Ordering::Relaxed) {
        debug!("Rejecting transaction - no block environment available");
        return Ok(JsonRpcResponse::block_not_available(request));
    }

    let Some(_) = &request.params else {
        debug!("sendTransactions request missing required parameters");
        return Ok(JsonRpcResponse::internal_error(
            request,
            "Missing params for sendTransactions",
        ));
    };

    let queue_transactions = match HttpTransactionDecoder::to_transaction(request.clone()) {
        Ok(transactions) => transactions,
        Err(e) => {
            error!(
                error = %e,
                "Failed to decode transactions"
            );
            return Ok(JsonRpcResponse::internal_error(
                request,
                &format!("Failed to decode transactions: {}", e),
            ));
        }
    };

    let transaction_count = queue_transactions.len();
    let mut processed_count = 0;

    // Send each decoded transaction to the queue
    for queue_tx in queue_transactions {
        if let Err(e) = state.tx_sender.send(TxQueueContents::Tx(queue_tx)) {
            error!(
                error = %e,
                "Failed to send transaction to queue from transport server"
            );
            return Ok(JsonRpcResponse::internal_error(
                request,
                "Internal error: failed to queue transaction",
            ));
        }
        processed_count += 1;
    }

    debug!(
        transaction_count = transaction_count,
        processed_count = processed_count,
        "Successfully processed transaction batch"
    );

    Ok(JsonRpcResponse::success(
        request,
        serde_json::json!({
            "status": "accepted",
            "transaction_count": processed_count,
            "message": format!("Successfully processed {} transactions", processed_count)
        }),
    ))
}
