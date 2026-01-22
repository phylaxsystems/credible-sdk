//! HTTP JSON-RPC transport
#![doc = include_str!("./README.md")]

use crate::{
    engine::queue::TransactionQueueSender,
    health::health_router,
    transactions_state::TransactionsState,
    transport::{
        Transport,
        http::{
            block_context::BlockContext,
            config::HttpTransportConfig,
            tracing_middleware::tracing_middleware,
        },
        transactions_results::QueryTransactionsResults,
    },
    utils::ErrorRecoverability,
};
use axum::{
    Router,
    middleware,
    routing::post,
};
use std::{
    net::SocketAddr,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;
use tracing::{
    debug,
    error,
    info,
    instrument,
    warn,
};

mod block_context;
pub mod config;
pub mod server;
mod tracing_middleware;

#[derive(thiserror::Error, Debug)]
pub enum HttpTransportError {
    #[error("Server error: {0}")]
    ServerError(String),
    #[error("Failed to bind the address: {0}")]
    BindAddress(String),
    #[error("Client error: {0}")]
    ClientError(#[source] reqwest::Error),
}

impl From<&HttpTransportError> for ErrorRecoverability {
    fn from(e: &HttpTransportError) -> Self {
        match e {
            HttpTransportError::ServerError(_) | HttpTransportError::BindAddress(_) => {
                Self::Unrecoverable
            }
            HttpTransportError::ClientError(_) => Self::Recoverable,
        }
    }
}

/// Implementation of the HTTP `Transport`.
/// Contains server for accepting transaction events, and server to call
/// the driver when missing state.
///
/// ## Initializing
///
/// Initializing the transport should be done via the `new()` function.
/// One of the arguments is the `HttpTransportConfig`. The config contains
/// endpoints to which the transport should either listen on or connect to.
/// The transport will error on init if it tries to open a server on an occupied port.
///
/// ## Running
///
/// After initialization, the transport should be ran via `run`. It will proceed to spawn
/// multiple tasks needed for both the server, client and for its own operational purposes.
///
/// ## Cleaning up
///
/// The transport will attempt to clean up as well as it can when gracefully shutting down with
/// `stop`. This means severing client/server connections and performing database flushes if aplicable.
#[derive(Debug)]
pub struct HttpTransport {
    /// Core engine queue sender.
    tx_sender: TransactionQueueSender,
    /// Server bind address
    bind_addr: SocketAddr,
    /// Shutdown cancellation token
    shutdown_token: CancellationToken,
    /// Shared transaction results state
    transactions_results: QueryTransactionsResults,
    /// Block context for tracing
    block_context: BlockContext,
}

/// Create transaction submission routes
fn transaction_routes(state: server::ServerState, block_context: &BlockContext) -> Router {
    Router::new()
        .route("/tx", post(server::handle_transaction_rpc))
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            block_context.clone(),
            tracing_middleware,
        ))
}

impl Transport for HttpTransport {
    type Error = HttpTransportError;
    type Config = HttpTransportConfig;

    #[instrument(name = "http_transport::new", skip_all, level = "debug")]
    fn new(
        config: HttpTransportConfig,
        tx_sender: TransactionQueueSender,
        state_results: Arc<TransactionsState>,
        _event_id_buffer_capacity: usize,
    ) -> Result<Self, Self::Error> {
        debug!(
            bind_addr = %config.bind_addr,
            "Creating HTTP transport"
        );
        Ok(Self {
            tx_sender,
            bind_addr: config.bind_addr,
            shutdown_token: CancellationToken::new(),
            transactions_results: QueryTransactionsResults::new(
                state_results,
                config.pending_receive_ttl,
            ),
            block_context: BlockContext::default(),
        })
    }

    #[instrument(
        name = "http_transport::run",
        skip(self),
        fields(bind_addr = %self.bind_addr),
        level = "info"
    )]
    async fn run(&self) -> Result<(), Self::Error> {
        let state = server::ServerState::new(
            self.tx_sender.clone(),
            self.transactions_results.clone(),
            self.block_context.clone(),
        );
        let app = Router::new()
            .merge(health_router())
            .merge(transaction_routes(state, &self.block_context));

        let listener = tokio::net::TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| {
                error!(
                    bind_addr = %self.bind_addr,
                    error = ?e,
                    "Failed to bind HTTP transport listener"
                );
                HttpTransportError::BindAddress(self.bind_addr.to_string())
            })?;

        info!(
            bind_addr = %self.bind_addr,
            "HTTP transport server starting"
        );

        let shutdown_token = self.shutdown_token.clone();
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown_token.cancelled().await })
            .await
            .map_err(|e| {
                error!(error = ?e, "HTTP server failed");
                HttpTransportError::ServerError(e.to_string())
            })?;
        Ok(())
    }

    #[instrument(name = "http_transport::stop", skip(self), level = "info")]
    fn stop(&mut self) {
        info!("Stopping HTTP transport");
        self.shutdown_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::queue::CommitHead;
    use alloy::primitives::B256;
    use assertion_executor::primitives::{
        Bytes,
        U256,
    };
    use reqwest::Client;
    use serde_json::{
        Value,
        json,
    };
    use std::{
        net::SocketAddr,
        time::Duration,
    };
    use tracing::debug;

    #[crate::utils::engine_test(http)]
    async fn test_invalid_block_env_request(mut instance: crate::utils::LocalInstance) {
        instance.new_block().await.unwrap();
        instance.new_iteration(1).await.unwrap();
        // Send a valid transaction
        let _ = instance.send_reverting_create_tx().await.unwrap();

        let local_address = instance.local_address.unwrap();

        // Send an invalid blockenv request
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendBlockEnv",
            "params": {
                "block_env": {
                    "number": "invalid_number",  // String instead of u64
                    "beneficiary": "0x0000000000000000000000000000000000000000",
                    "timestamp": 0u64,
                    "gas_limit": 0u64,
                    "basefee": 0u64,
                    "difficulty": "0x0"
                }
            },
            "id": 1
        });

        let res = send_raw_request(invalid_request, local_address)
            .await
            .unwrap();
        assert!(res.contains(
            r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"Method not found"},"id":1}"#
        ));
    }

    #[crate::utils::engine_test(http)]
    async fn test_invalid_reorg_request(mut instance: crate::utils::LocalInstance) {
        instance.new_block().await.unwrap();
        instance.new_iteration(1).await.unwrap();

        // Send a valid transaction
        let _ = instance.send_reverting_create_tx().await.unwrap();

        let local_address = instance.local_address.unwrap();

        // Send an invalid blockenv request
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": {
                "invalid": "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            },
            "id": 1
        });

        let res = send_raw_request(invalid_request, local_address)
            .await
            .unwrap();
        assert!(res.contains("Failed to decode transactions: Reorg validation error"));
    }

    #[crate::utils::engine_test(http)]
    async fn test_invalid_transaction_request(mut instance: crate::utils::LocalInstance) {
        instance.new_block().await.unwrap();
        instance.new_iteration(1).await.unwrap();

        // Send a valid transaction
        let _ = instance.send_reverting_create_tx().await.unwrap();

        let local_address = instance.local_address.unwrap();

        // Send an invalid blockenv request
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendTransactions",
            "params": {
                "transactions": [
                    {
                        "tx_env": {
                            "caller": "0x742d35Cc6634C0532925a3b8D23b7E07e3E23eF4",
                            "gas_limit": 21000,
                            "gas_price": "1000",
                            "transact_to": "0x8ba1f109551bD432803012645Hac136c2D29",
                            "value": "0x0",
                            "data": "0x",
                            "chain_id": 1,
                            "access_list": []
                        }
                    }
                ]
            },
            "id": 1
        });

        let res = send_raw_request(invalid_request, local_address)
            .await
            .unwrap();
        assert!(res.contains("Failed to decode transactions: Invalid transaction format: invalid transactions array: invalid tx_env: invalid type: string \\\"1000\\\", expected u128\""));
    }

    #[crate::utils::engine_test(http)]
    async fn test_get_transaction_returns_successful_result(
        mut instance: crate::utils::LocalInstance,
    ) {
        instance.new_block().await.unwrap();
        instance.new_iteration(1).await.unwrap();

        let tx_execution_id = instance
            .send_successful_create_tx(U256::from(0u64), Bytes::new())
            .await
            .expect("failed to send transaction");

        assert!(
            instance
                .is_transaction_successful(&tx_execution_id)
                .await
                .expect("transaction query failed"),
            "transaction expected to succeed"
        );

        let local_address = instance
            .local_address
            .expect("http transport should expose an address");

        let request = json!({
            "jsonrpc": "2.0",
            "method": "getTransaction",
            "params": [
                {
                    "block_number": tx_execution_id.block_number,
                    "iteration_id": tx_execution_id.iteration_id,
                    "tx_hash": tx_execution_id.tx_hash.to_string(),
                    "index": 0
                }
            ],
            "id": 1
        });

        let response_body = send_raw_request(request, local_address)
            .await
            .expect("HTTP request should succeed");

        let response_json: Value =
            serde_json::from_str(&response_body).expect("response must be valid json");
        assert!(
            response_json.get("error").is_none(),
            "unexpected RPC error: {response_json:?}"
        );

        let payload = response_json
            .get("result")
            .expect("missing result payload in response");
        let result = payload
            .get("result")
            .expect("missing transaction result object");

        let hash_str = result
            .get("tx_execution_id")
            .unwrap()
            .get("tx_hash")
            .and_then(serde_json::Value::as_str)
            .expect("hash field missing");
        let parsed_hash = hash_str.parse::<B256>().expect("invalid hash encoding");
        assert_eq!(
            parsed_hash, tx_execution_id.tx_hash,
            "queried hash should match"
        );

        let status = result
            .get("status")
            .and_then(serde_json::Value::as_str)
            .expect("status field missing");
        assert_eq!(status, "success");

        assert!(
            result
                .get("gas_used")
                .and_then(serde_json::Value::as_u64)
                .is_some(),
            "gas_used expected to be populated"
        );

        if let Some(error) = result.get("error") {
            assert!(
                error.is_null(),
                "error field should be null for successful transactions"
            );
        }
    }

    #[crate::utils::engine_test(http)]
    async fn test_get_transaction_reports_not_found(mut instance: crate::utils::LocalInstance) {
        instance
            .new_block()
            .await
            .expect("failed to announce new block");
        instance.new_iteration(1).await.unwrap();

        let missing_hash = B256::repeat_byte(0x42);

        let local_address = instance
            .local_address
            .expect("http transport should expose an address");

        let request = json!({
            "jsonrpc": "2.0",
            "method": "getTransaction",
            "params": [
                {
                    "block_number": 0u64,
                    "iteration_id": 0u64,
                    "tx_hash": missing_hash.to_string(),
                    "index": 0
                }
            ],
            "id": 2
        });

        let response_body = send_raw_request(request, local_address)
            .await
            .expect("HTTP request should succeed");

        let response_json: Value =
            serde_json::from_str(&response_body).expect("response must be valid json");
        assert!(
            response_json.get("error").is_none(),
            "unexpected RPC error: {response_json:?}"
        );

        let payload = response_json
            .get("result")
            .expect("missing result payload in response");

        let not_found = payload
            .get("not_found")
            .unwrap()
            .get("tx_hash")
            .and_then(serde_json::Value::as_str)
            .expect("missing not_found field");
        assert_eq!(not_found, missing_hash.to_string());
    }

    #[crate::utils::engine_test(http)]
    async fn test_commit_head_invalid_block_hash_length(mut instance: crate::utils::LocalInstance) {
        // First, set up a valid state
        instance.new_block().await.unwrap();
        instance.new_iteration(1).await.unwrap();
        let _ = instance.send_reverting_create_tx().await.unwrap();

        let local_address = instance.local_address.unwrap();

        // Send CommitHead with invalid block_hash (wrong length - 20 bytes instead of 32)
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendEvents",
            "params": {
                "events": [
                    {
                        "commit_head": {
                            "block_number": 2u64,
                            "selected_iteration_id": 1u64,
                            "last_tx_hash": null,
                            "n_transactions": 0u64,
                            "block_hash": "0x0000000000000000000000000000000000000000"  // 20 bytes, should be 32
                        }
                    }
                ]
            },
            "id": 1
        });

        let res = send_raw_request(invalid_request, local_address)
            .await
            .unwrap();

        // Should return an error about invalid hash length
        assert!(
            res.contains("error") || res.contains("invalid"),
            "expected error for invalid block_hash length: {res}"
        );
    }

    #[crate::utils::engine_test(http)]
    async fn test_commit_head_invalid_beacon_root_length(
        mut instance: crate::utils::LocalInstance,
    ) {
        // First, set up a valid state
        instance.new_block().await.unwrap();
        instance.new_iteration(1).await.unwrap();
        let _ = instance.send_reverting_create_tx().await.unwrap();

        let local_address = instance.local_address.unwrap();

        // Send CommitHead with invalid parent_beacon_block_root (wrong length)
        let invalid_request = json!({
            "jsonrpc": "2.0",
            "method": "sendEvents",
            "params": {
                "events": [
                    {
                        "commit_head": {
                            "block_number": 2u64,
                            "selected_iteration_id": 1u64,
                            "last_tx_hash": null,
                            "n_transactions": 0u64,
                            "parent_beacon_block_root": "0x00000000"  // 4 bytes, should be 32
                        }
                    }
                ]
            },
            "id": 1
        });

        let res = send_raw_request(invalid_request, local_address)
            .await
            .unwrap();

        // Should return an error about invalid hash length
        assert!(
            res.contains("error") || res.contains("invalid"),
            "expected error for invalid parent_beacon_block_root length: {res}"
        );
    }

    #[crate::utils::engine_test(http)]
    async fn test_commit_head_empty_hashes_treated_as_none(
        mut instance: crate::utils::LocalInstance,
    ) {
        // First, set up a valid state
        instance.new_block().await.unwrap();
        instance.new_iteration(1).await.unwrap();
        let _ = instance.send_reverting_create_tx().await.unwrap();

        let local_address = instance.local_address.unwrap();

        // Send CommitHead with empty strings for parent hashes (should be treated as None)
        let request = json!({
            "jsonrpc": "2.0",
            "method": "sendEvents",
            "params": {
                "events": [
                    {
                        "commit_head": {
                            "block_number": 2u64,
                            "selected_iteration_id": 1u64,
                            "last_tx_hash": null,
                            "n_transactions": 0u64,
                            "block_hash": "",
                            "parent_beacon_block_root": ""
                        }
                    }
                ]
            },
            "id": 1
        });

        let res = send_raw_request(request, local_address).await.unwrap();

        // Empty strings should be filtered out and treated as None
        // This should either succeed or return a valid response (not a parse error)
        debug!("Response for empty parent hashes: {}", res);

        // The request should be processed (may succeed or fail for other reasons,
        // but should not fail due to empty string parsing)
        let response_json: Value = serde_json::from_str(&res).expect("response must be valid json");
        assert!(
            response_json.get("jsonrpc").is_some(),
            "should return valid JSON-RPC response"
        );
    }

    #[crate::utils::engine_test(http)]
    async fn test_commit_head_with_zero_hashes(mut instance: crate::utils::LocalInstance) {
        // Test that zero hashes (all zeros) are valid and processed correctly
        let zero_hash = B256::ZERO;

        instance
            .new_block_with_hashes(zero_hash, Some(zero_hash))
            .await
            .expect("new_block with zero hashes should succeed");

        instance.new_iteration(1).await.unwrap();

        let tx_execution_id = instance
            .send_successful_create_tx(U256::from(0u64), Bytes::new())
            .await
            .expect("failed to send transaction");

        assert!(
            instance
                .is_transaction_successful(&tx_execution_id)
                .await
                .expect("transaction query failed"),
            "transaction expected to succeed with zero parent hashes"
        );
    }

    async fn send_raw_request(
        request: serde_json::Value,
        address: SocketAddr,
    ) -> Result<String, String> {
        let mut last_error = String::new();
        let mut attempts = 0;

        while attempts < 3 {
            attempts += 1;

            match Client::new()
                .post(format!("http://{address}/tx"))
                .header("content-type", "application/json")
                .json(&request)
                .send()
                .await
            {
                Ok(response) => {
                    if !response.status().is_success() {
                        return Err(format!("HTTP error: {}", response.status()));
                    }

                    return Ok(response.text().await.unwrap_or_default());
                }
                Err(e) => {
                    last_error = format!("HTTP request failed: {e}");
                    if attempts < 3 {
                        debug!(target: "LocalInstanceHttpDriver", "HTTP request failed (attempt {}/3), retrying...", attempts);
                        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;
                    }
                }
            }
        }

        Err(last_error)
    }
}
