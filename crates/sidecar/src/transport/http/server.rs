//! JSON-RPC server handlers for HTTP transport

use crate::{
    engine::{
        TransactionResult,
        queue::{
            TransactionQueueSender,
            TxQueueContents,
        },
    },
    transactions_state::RequestTransactionResult,
    transport::{
        decoder::{
            Decoder,
            HttpTransactionDecoder,
        },
        http::{
            block_context::BlockContext,
            tracing_middleware::trace_tx_queue_contents,
            transactions_results::QueryTransactionsResults,
        },
    },
};
use alloy::rpc::types::error::EthRpcErrorCode;
use assertion_executor::primitives::ExecutionResult;
use axum::{
    extract::{
        Json,
        State,
    },
    http::StatusCode,
    response::Json as ResponseJson,
};
use revm::primitives::alloy_primitives::TxHash;
use serde::{
    Deserialize,
    Serialize,
};
use std::sync::{
    Arc,
    LazyLock,
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
    warn,
};

pub(in crate::transport) const METHOD_SEND_TRANSACTIONS: &str = "sendTransactions";
pub(in crate::transport) const METHOD_BLOCK_ENV: &str = "sendBlockEnv";
pub(in crate::transport) const METHOD_GET_TRANSACTION: &str = "getTransactions";

#[derive(Debug, Deserialize, Serialize)]
pub struct TransactionEnv {
    pub caller: String,
    pub gas_limit: u64,
    pub gas_price: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transact_to: Option<String>,
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

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

static JSONRPCVER: LazyLock<String> = LazyLock::new(|| "2.0".to_string());

impl JsonRpcResponse {
    pub fn block_not_available(request: &JsonRpcRequest) -> Self {
        JsonRpcResponse {
            jsonrpc: JSONRPCVER.clone(),
            result: None,
            error: Some(JsonRpcError {
                code: EthRpcErrorCode::UnknownBlock.code(),
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
                code: EthRpcErrorCode::TransactionRejected.code(),
                message: message.to_string(),
            }),
            id: request.id.clone(),
        }
    }

    pub fn invalid_request(request: &JsonRpcRequest, message: &str) -> Self {
        JsonRpcResponse {
            jsonrpc: JSONRPCVER.clone(),
            result: None,
            error: Some(JsonRpcError {
                code: EthRpcErrorCode::InvalidInput.code(),
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
                code: EthRpcErrorCode::InvalidInput.code(),
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
                code: EthRpcErrorCode::InvalidInput.code(),
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
    transactions_results: QueryTransactionsResults,
    /// Block context for tracing
    block_context: BlockContext,
}

impl ServerState {
    pub fn new(
        has_blockenv: Arc<AtomicBool>,
        tx_sender: TransactionQueueSender,
        transactions_results: QueryTransactionsResults,
        block_context: BlockContext,
    ) -> Self {
        Self {
            has_blockenv,
            tx_sender,
            transactions_results,
            block_context,
        }
    }
}

/// Handle JSON-RPC requests for transactions
#[instrument(
    name = "http_server::handle_transaction_rpc",
    skip(state),
    fields(
        method = %request.method,
    ),
    level = "debug"
)]
pub async fn handle_transaction_rpc(
    State(state): State<ServerState>,
    Json(request): Json<JsonRpcRequest>,
) -> Result<ResponseJson<JsonRpcResponse>, StatusCode> {
    debug!("Processing JSON-RPC request");

    let response = match request.method.as_str() {
        METHOD_SEND_TRANSACTIONS => handle_send_transactions(&state, &request).await?,
        METHOD_BLOCK_ENV => handle_block_env(&state, &request).await?,
        METHOD_GET_TRANSACTION => handle_get_transactions(&state, &request).await?,
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

#[instrument(name = "http_server::handle_block_env", skip_all, level = "debug")]
async fn handle_block_env(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    trace!("Processing blockEnv request");

    let response = process_request(state, request).await?;
    // If the `process_request` call was successful, we can mark the block environment as received
    if !state.has_blockenv.load(Ordering::Relaxed) {
        state.has_blockenv.store(true, Ordering::Release);
    }

    Ok(response)
}

#[instrument(
    name = "http_server::handle_send_transactions",
    skip_all,
    level = "debug"
)]
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
    process_request(state, request).await
}

#[instrument(name = "http_server::process_message", skip_all, level = "debug")]
async fn process_request(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    trace!("Processing incoming request and sending to the queue");

    let Some(_) = &request.params else {
        debug!("request missing required parameters");
        return Ok(JsonRpcResponse::invalid_params(
            request,
            "Missing params for the incoming request",
        ));
    };

    let tx_queue_contents = match HttpTransactionDecoder::to_tx_queue_contents(request) {
        Ok(tx_queue_contents) => tx_queue_contents,
        Err(e) => {
            error!(
                error = %e,
                "Failed to decode transactions"
            );
            return Ok(JsonRpcResponse::invalid_request(
                request,
                &format!("Failed to decode transactions: {e}"),
            ));
        }
    };

    let request_count = tx_queue_contents.len();

    // Send each decoded transaction to the queue
    for queue_tx in tx_queue_contents {
        trace_tx_queue_contents(&state.block_context, &queue_tx);
        if let TxQueueContents::Tx(tx, _) = &queue_tx
            && state.transactions_results.is_tx_received(&tx.tx_hash)
        {
            warn!(
                tx_hash = %tx.tx_hash,
                "TX hash already received, skipping"
            );
            continue;
        }
        state.transactions_results.add_accepted_tx(&queue_tx);
        if let Err(e) = state.tx_sender.send(queue_tx) {
            error!(
                error = %e,
                "Failed to send tx queue content to queue from transport server"
            );
            return Ok(JsonRpcResponse::internal_error(
                request,
                "Internal error: failed to queue transaction",
            ));
        }
    }

    debug!(
        request_count = request_count,
        "Successfully processed request batch"
    );

    Ok(JsonRpcResponse::success(
        request,
        serde_json::json!({
            "status": "accepted",
            "request_count": request_count,
            "message": "Requests processed successfully".to_string(),
        }),
    ))
}

async fn handle_get_transactions(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    trace!("Processing getTransactions request");

    // Check if we have block environment before processing transactions
    // NOTE: This can be dropped once we implement the "not_found" feature, the result will be "not_found" by default because there cannot be any tx hash consumed if no BlockEnv was received first
    if !state.has_blockenv.load(Ordering::Relaxed) {
        debug!("Rejecting transaction - no block environment available");
        return Ok(JsonRpcResponse::block_not_available(request));
    }

    let Some(params) = &request.params else {
        debug!("getTransactions request missing required parameters");
        return Ok(JsonRpcResponse::invalid_params(
            request,
            "Missing params for getTransactions",
        ));
    };

    let Ok(tx_hashes) = serde_json::from_value::<Vec<TxHash>>(params.clone()) else {
        debug!("getTransactions request invalid tx hash format");
        return Ok(JsonRpcResponse::invalid_request(
            request,
            "Invalid params for getTransactions",
        ));
    };

    let (received_tx_hashes, not_found_hashes): (Vec<_>, Vec<_>) = tx_hashes
        .into_iter()
        .partition(|tx_hash| state.transactions_results.is_tx_received(tx_hash));

    // Can you write me here the sending to the queue + waiting for the result?
    let mut results = Vec::with_capacity(received_tx_hashes.len());

    // Process each transaction hash
    for tx_hash in received_tx_hashes {
        let result = match state
            .transactions_results
            .request_transaction_result(&tx_hash)
        {
            RequestTransactionResult::Result(result) => result,
            RequestTransactionResult::Channel(receiver) => {
                if let Ok(result) = receiver.await {
                    result
                } else {
                    error!(
                    tx_hash = %tx_hash,
                    "Engine dropped response channel for transaction query"
                    );
                    return Ok(JsonRpcResponse::internal_error(
                        request,
                        "Internal error: engine unavailable",
                    ));
                }
            }
        };

        // Convert result to JSON format
        results.push(into_transaction_result_response(
            tx_hash.to_string(),
            &result,
        ));
    }

    debug!(
        result_count = results.len(),
        "Successfully processed transaction queries"
    );

    Ok(JsonRpcResponse::success(
        request,
        serde_json::json!({
            "results": results,
            "not_found": not_found_hashes,
        }),
    ))
}

/// Helper function to determine transaction status and error message
fn into_transaction_result_response(
    hash: String,
    result: &TransactionResult,
) -> TransactionResultResponse {
    match result {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => {
            let gas_used = Some(execution_result.gas_used());
            if !*is_valid {
                // Transaction failed assertion validation
                return TransactionResultResponse {
                    hash,
                    status: "assertion_failed".to_string(),
                    gas_used,
                    error: None,
                };
            }
            match execution_result {
                ExecutionResult::Success { .. } => {
                    TransactionResultResponse {
                        hash,
                        status: "success".to_string(),
                        gas_used,
                        error: None,
                    }
                }
                ExecutionResult::Revert { .. } => {
                    TransactionResultResponse {
                        hash,
                        status: "reverted".to_string(),
                        gas_used,
                        error: None,
                    }
                }
                ExecutionResult::Halt { reason, .. } => {
                    TransactionResultResponse {
                        hash,
                        status: "halted".to_string(),
                        gas_used,
                        error: Some(format!("Transaction halted: {reason:?}")),
                    }
                }
            }
        }
        TransactionResult::ValidationError(error) => {
            TransactionResultResponse {
                hash,
                status: "failed".to_string(),
                gas_used: None,
                error: Some(format!("Validation error: {error}")),
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct TransactionResultResponse {
    pub hash: String,
    pub status: String,
    pub gas_used: Option<u64>,
    pub error: Option<String>,
}
