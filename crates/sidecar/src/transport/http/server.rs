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
use revm::{
    context::TxEnv,
    primitives::alloy_primitives::TxHash,
};
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    de::{
        self,
        MapAccess,
        Visitor,
    },
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
pub(in crate::transport) const METHOD_REORG: &str = "reorg";
pub(in crate::transport) const METHOD_GET_TRANSACTIONS: &str = "getTransactions";
pub(in crate::transport) const METHOD_GET_TRANSACTION: &str = "getTransaction";

#[derive(Debug, Serialize)]
pub struct Transaction {
    #[serde(rename = "txEnv")]
    pub tx_env: TxEnv,
    pub hash: String,
}

impl<'de> Deserialize<'de> for Transaction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "camelCase")]
        enum Field {
            TxEnv,
            Hash,
        }

        struct TransactionVisitor;

        impl<'de> Visitor<'de> for TransactionVisitor {
            type Value = Transaction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Transaction")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Transaction, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut tx_env = None;
                let mut hash = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::TxEnv => {
                            if tx_env.is_some() {
                                return Err(de::Error::duplicate_field("txEnv"));
                            }
                            tx_env =
                                Some(map.next_value().map_err(|e| {
                                    de::Error::custom(format!("invalid txEnv: {e}"))
                                })?);
                        }
                        Field::Hash => {
                            if hash.is_some() {
                                return Err(de::Error::duplicate_field("hash"));
                            }
                            hash = Some(
                                map.next_value()
                                    .map_err(|_| de::Error::custom("invalid hash field"))?,
                            );
                        }
                    }
                }

                let tx_env = tx_env.ok_or_else(|| de::Error::missing_field("txEnv"))?;
                let hash = hash.ok_or_else(|| de::Error::missing_field("hash"))?;

                Ok(Transaction { tx_env, hash })
            }
        }

        deserializer.deserialize_struct("Transaction", &["txEnv", "hash"], TransactionVisitor)
    }
}

#[derive(Debug, Serialize)]
pub struct SendTransactionsParams {
    pub transactions: Vec<Transaction>,
}

impl<'de> Deserialize<'de> for SendTransactionsParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Transactions,
        }

        struct SendTransactionsParamsVisitor;

        impl<'de> Visitor<'de> for SendTransactionsParamsVisitor {
            type Value = SendTransactionsParams;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct SendTransactionsParams")
            }

            fn visit_map<V>(self, mut map: V) -> Result<SendTransactionsParams, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut transactions = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Transactions => {
                            if transactions.is_some() {
                                return Err(de::Error::duplicate_field("transactions"));
                            }
                            transactions = Some(map.next_value().map_err(|e| {
                                de::Error::custom(format!("invalid transactions array: {e}"))
                            })?);
                        }
                    }
                }

                let transactions =
                    transactions.ok_or_else(|| de::Error::missing_field("transactions"))?;

                Ok(SendTransactionsParams { transactions })
            }
        }

        deserializer.deserialize_struct(
            "SendTransactionsParams",
            &["transactions"],
            SendTransactionsParamsVisitor,
        )
    }
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
        METHOD_REORG => handle_reorg(&state, &request).await?,
        METHOD_GET_TRANSACTIONS => handle_get_transactions(&state, &request).await?,
        METHOD_GET_TRANSACTION => handle_get_transaction(&state, &request).await?,
        _ => {
            debug!(
                method = %request.method,
                "Unknown JSON-RPC method requested"
            );
            JsonRpcResponse::method_not_found(&request)
        }
    };

    debug!(
        has_failure = response.error.is_some(),
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

#[instrument(name = "http_server::handle_reorg", skip_all, level = "debug")]
async fn handle_reorg(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    trace!("Processing reorg request");
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
                error = ?e,
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
                error = ?e,
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

    let mut results = Vec::with_capacity(received_tx_hashes.len());
    for tx_hash in received_tx_hashes {
        match resolve_transaction_result(state, request, tx_hash).await {
            Ok(result) => results.push(result),
            Err(error_response) => return Ok(error_response),
        }
    }

    debug!(
        result_count = results.len(),
        "Successfully processed transaction queries"
    );

    Ok(JsonRpcResponse::success(
        request,
        serde_json::json!({
            "results": results,
            "not_found": not_found_hashes.into_iter().map(|hash| hash.to_string()).collect::<Vec<_>>(),
        }),
    ))
}

async fn handle_get_transaction(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    trace!("Processing getTransaction request");

    if !state.has_blockenv.load(Ordering::Relaxed) {
        debug!("Rejecting transaction - no block environment available");
        return Ok(JsonRpcResponse::block_not_available(request));
    }

    let Some(params) = &request.params else {
        debug!("getTransaction request missing required parameters");
        return Ok(JsonRpcResponse::invalid_params(
            request,
            "Missing params for getTransaction",
        ));
    };

    let Ok(mut tx_hashes) = serde_json::from_value::<Vec<TxHash>>(params.clone()) else {
        debug!("getTransaction request invalid tx hash format");
        return Ok(JsonRpcResponse::invalid_request(
            request,
            "Invalid params for getTransaction",
        ));
    };

    if tx_hashes.len() != 1 {
        debug!(
            hash_count = tx_hashes.len(),
            "getTransaction requires exactly one transaction hash"
        );
        return Ok(JsonRpcResponse::invalid_params(
            request,
            "getTransaction expects exactly one transaction hash",
        ));
    }

    let tx_hash = tx_hashes.remove(0);

    if !state.transactions_results.is_tx_received(&tx_hash) {
        return Ok(JsonRpcResponse::success(
            request,
            serde_json::json!({
                "not_found": tx_hash.to_string(),
            }),
        ));
    }

    let result = match resolve_transaction_result(state, request, tx_hash).await {
        Ok(result) => result,
        Err(error_response) => return Ok(error_response),
    };

    Ok(JsonRpcResponse::success(
        request,
        serde_json::json!({
            "result": result,
        }),
    ))
}

async fn resolve_transaction_result(
    state: &ServerState,
    request: &JsonRpcRequest,
    tx_hash: TxHash,
) -> Result<TransactionResultResponse, JsonRpcResponse> {
    let hash_string = tx_hash.to_string();

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
                    tx_hash = %hash_string,
                    "Engine dropped response channel for transaction query"
                );
                return Err(JsonRpcResponse::internal_error(
                    request,
                    "Internal error: engine unavailable",
                ));
            }
        }
    };

    Ok(into_transaction_result_response(hash_string, &result))
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
