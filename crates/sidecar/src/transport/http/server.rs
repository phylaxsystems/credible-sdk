//! JSON-RPC server handlers for HTTP transport

use crate::{
    engine::{
        TransactionResult,
        queue::{
            TransactionQueueSender,
            TxQueueContents,
        },
    },
    execution_ids::TxExecutionId,
    transactions_state::RequestTransactionResult,
    transport::{
        decoder::{
            Decoder,
            HttpTransactionDecoder,
        },
        http::{
            block_context::BlockContext,
            tracing_middleware::trace_tx_queue_contents,
        },
        rpc_metrics::RpcRequestDuration,
        transactions_results::QueryTransactionsResults,
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
use revm::context::TxEnv;
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
use std::{
    str::FromStr,
    sync::{
        Arc,
        LazyLock,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
};
use tracing::{
    debug,
    error,
    instrument,
    warn,
};

pub(in crate::transport) const METHOD_SEND_TRANSACTIONS: &str = "sendTransactions";
pub(in crate::transport) const METHOD_SEND_EVENTS: &str = "sendEvents";
pub(in crate::transport) const METHOD_REORG: &str = "reorg";
pub(in crate::transport) const METHOD_GET_TRANSACTIONS: &str = "getTransactions";
pub(in crate::transport) const METHOD_GET_TRANSACTION: &str = "getTransaction";

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Transaction {
    pub tx_execution_id: TxExecutionId,
    pub tx_env: TxEnv,
}

impl<'de> Deserialize<'de> for Transaction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            TxExecutionId,
            TxEnv,
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
                let mut tx_execution_id = None;
                let mut tx_env = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::TxExecutionId => {
                            if tx_execution_id.is_some() {
                                return Err(de::Error::duplicate_field("tx_execution_id"));
                            }
                            tx_execution_id = Some(map.next_value().map_err(|e| {
                                de::Error::custom(format!("invalid tx_execution_id: {e}"))
                            })?);
                        }
                        Field::TxEnv => {
                            if tx_env.is_some() {
                                return Err(de::Error::duplicate_field("tx_env"));
                            }
                            tx_env =
                                Some(map.next_value().map_err(|e| {
                                    de::Error::custom(format!("invalid tx_env: {e}"))
                                })?);
                        }
                    }
                }

                let tx_execution_id =
                    tx_execution_id.ok_or_else(|| de::Error::missing_field("tx_execution_id"))?;
                let tx_env = tx_env.ok_or_else(|| de::Error::missing_field("tx_env"))?;

                Ok(Transaction {
                    tx_execution_id,
                    tx_env,
                })
            }
        }

        deserializer.deserialize_struct(
            "Transaction",
            &["tx_execution_id", "tx_env"],
            TransactionVisitor,
        )
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
    commit_head_seen: Arc<AtomicBool>,
    pub tx_sender: TransactionQueueSender,
    transactions_results: QueryTransactionsResults,
    /// Block context for tracing
    block_context: BlockContext,
}

impl ServerState {
    pub fn new(
        tx_sender: TransactionQueueSender,
        transactions_results: QueryTransactionsResults,
        block_context: BlockContext,
    ) -> Self {
        Self {
            commit_head_seen: Arc::new(AtomicBool::new(false)),
            tx_sender,
            transactions_results,
            block_context,
        }
    }

    fn has_commit_head(&self) -> bool {
        self.commit_head_seen.load(Ordering::Acquire)
    }

    fn mark_commit_head_seen(&self) {
        self.commit_head_seen.store(true, Ordering::Release);
    }
}

/// Handle JSON-RPC requests for transactions
#[instrument(
    name = "http_server::handle_transaction_rpc",
    skip(state, request),
    fields(
        method = %request.method,
    ),
    level = "debug"
)]
pub async fn handle_transaction_rpc(
    State(state): State<ServerState>,
    Json(request): Json<JsonRpcRequest>,
) -> Result<ResponseJson<JsonRpcResponse>, StatusCode> {
    debug!(request = ?request, "Processing JSON-RPC request");

    let response = match request.method.as_str() {
        METHOD_SEND_TRANSACTIONS => handle_send_transactions(&state, &request).await?,
        METHOD_SEND_EVENTS => handle_send_events(&state, &request).await?,
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

#[instrument(
    name = "http_server::handle_send_transactions",
    skip_all,
    level = "debug"
)]
async fn handle_send_transactions(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    let _rpc_timer = RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "sendTransactions"));
    if let Some(response) = ensure_commit_head_seen(state, request) {
        return Ok(response);
    }
    process_request(state, request).await
}

#[instrument(name = "http_server::handle_send_events", skip_all, level = "debug")]
async fn handle_send_events(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    let _rpc_timer = RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "sendEvents"));
    process_request(state, request).await
}

#[instrument(name = "http_server::handle_reorg", skip_all, level = "debug")]
async fn handle_reorg(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    let _rpc_timer = RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "reorg"));
    process_request(state, request).await
}

#[instrument(name = "http_server::process_message", skip_all, level = "debug")]
async fn process_request(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
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
        match &queue_tx {
            TxQueueContents::CommitHead(_, _) => {
                state.mark_commit_head_seen();
            }
            TxQueueContents::NewIteration(_, _)
            | TxQueueContents::Tx(_, _)
            | TxQueueContents::Reorg(_, _) => {
                if !state.has_commit_head() {
                    debug!("Rejecting request without prior commit head");
                    return Ok(JsonRpcResponse::invalid_request(
                        request,
                        "commit head must be received before other events",
                    ));
                }
            }
        }

        trace_tx_queue_contents(&state.block_context, &queue_tx);
        if let TxQueueContents::Tx(tx, _) = &queue_tx
            && state
                .transactions_results
                .is_tx_received(&tx.tx_execution_id)
        {
            warn!(
                tx_hash = %tx.tx_execution_id.tx_hash_hex(),
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

fn ensure_commit_head_seen(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Option<JsonRpcResponse> {
    if state.has_commit_head() {
        None
    } else {
        debug!("Rejecting request - commit head not yet received");
        Some(JsonRpcResponse::block_not_available(request))
    }
}

#[allow(clippy::result_large_err)]
fn parse_tx_execution_ids(
    request: &JsonRpcRequest,
    method_name: &str,
) -> Result<Vec<TxExecutionId>, JsonRpcResponse> {
    let Some(params) = &request.params else {
        debug!("{method_name} request missing required parameters");
        let message = format!("Missing params for {method_name}");
        return Err(JsonRpcResponse::invalid_params(request, &message));
    };

    let Ok(tx_execution_ids) = serde_json::from_value::<Vec<TxExecutionId>>(params.clone()) else {
        debug!("{method_name} request invalid tx hash format");
        let message = format!("Invalid params for {method_name}");
        return Err(JsonRpcResponse::invalid_request(request, &message));
    };

    Ok(tx_execution_ids)
}

async fn handle_get_transactions(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    let _rpc_timer = RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "getTransactions"));
    // Check if we have block environment before processing transactions
    // NOTE: This can be dropped once we implement the "not_found" feature, the result will be "not_found" by default because there cannot be any tx hash consumed if no BlockEnv was received first
    if let Some(response) = ensure_commit_head_seen(state, request) {
        return Ok(response);
    }

    let tx_execution_ids = match parse_tx_execution_ids(request, "getTransactions") {
        Ok(tx_execution_ids) => tx_execution_ids,
        Err(error_response) => return Ok(error_response),
    };

    let (received_tx_execution_ids, not_found_tx_execution_ids): (Vec<_>, Vec<_>) =
        tx_execution_ids.into_iter().partition(|tx_execution_id| {
            state.transactions_results.is_tx_received(tx_execution_id)
        });

    let mut results = Vec::with_capacity(received_tx_execution_ids.len());
    for tx_execution_id in received_tx_execution_ids {
        match resolve_transaction_result(state, request, tx_execution_id).await {
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
            "not_found": not_found_tx_execution_ids,
        }),
    ))
}

async fn handle_get_transaction(
    state: &ServerState,
    request: &JsonRpcRequest,
) -> Result<JsonRpcResponse, StatusCode> {
    let _rpc_timer = RpcRequestDuration::new(concat!("sidecar_rpc_duration_", "getTransaction"));
    if let Some(response) = ensure_commit_head_seen(state, request) {
        return Ok(response);
    }

    let mut tx_execution_ids = match parse_tx_execution_ids(request, "getTransaction") {
        Ok(tx_execution_ids) => tx_execution_ids,
        Err(error_response) => return Ok(error_response),
    };

    if tx_execution_ids.len() != 1 {
        debug!(
            hash_count = tx_execution_ids.len(),
            "getTransaction requires exactly one transaction hash"
        );
        return Ok(JsonRpcResponse::invalid_params(
            request,
            "getTransaction expects exactly one transaction hash",
        ));
    }

    let tx_execution_id = tx_execution_ids.remove(0);

    if !state.transactions_results.is_tx_received(&tx_execution_id) {
        return Ok(JsonRpcResponse::success(
            request,
            serde_json::json!({
                "not_found": tx_execution_id
            }),
        ));
    }

    let result = match resolve_transaction_result(state, request, tx_execution_id).await {
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
    tx_execution_id: TxExecutionId,
) -> Result<TransactionResultResponse, JsonRpcResponse> {
    let result = match state
        .transactions_results
        .request_transaction_result(&tx_execution_id)
    {
        RequestTransactionResult::Result(result) => result,
        RequestTransactionResult::Channel(receiver) => {
            if let Ok(result) = receiver.await {
                result
            } else {
                error!(
                    tx_hash = %tx_execution_id.tx_hash_hex(),
                    "Engine dropped response channel for transaction query"
                );
                return Err(JsonRpcResponse::internal_error(
                    request,
                    "Internal error: engine unavailable",
                ));
            }
        }
    };

    Ok(into_transaction_result_response(tx_execution_id, &result))
}

/// Helper function to determine transaction status and error message
fn into_transaction_result_response(
    tx_execution_id: TxExecutionId,
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
                    tx_execution_id,
                    status: "assertion_failed".to_string(),
                    gas_used,
                    error: None,
                };
            }
            match execution_result {
                ExecutionResult::Success { .. } => {
                    TransactionResultResponse {
                        tx_execution_id,
                        status: "success".to_string(),
                        gas_used,
                        error: None,
                    }
                }
                ExecutionResult::Revert { .. } => {
                    TransactionResultResponse {
                        tx_execution_id,
                        status: "reverted".to_string(),
                        gas_used,
                        error: None,
                    }
                }
                ExecutionResult::Halt { reason, .. } => {
                    TransactionResultResponse {
                        tx_execution_id,
                        status: "halted".to_string(),
                        gas_used,
                        error: Some(format!("Transaction halted: {reason:?}")),
                    }
                }
            }
        }
        TransactionResult::ValidationError(error) => {
            TransactionResultResponse {
                tx_execution_id,
                status: "failed".to_string(),
                gas_used: None,
                error: Some(format!("Validation error: {error}")),
            }
        }
    }
}

#[derive(Debug, Serialize)]
struct TransactionResultResponse {
    pub tx_execution_id: TxExecutionId,
    pub status: String,
    pub gas_used: Option<u64>,
    pub error: Option<String>,
}
