//! # `api`
//!
//! The `api` mod contains the logic for serving clients with
//! data indexed by the `assertion-loader`.
//!
//! ## JSON-RPC Methods
//!
//! ### `da_submit_assertion`
//!
//! Submits an assertion to the assertion DA.
//!
//! #### Request
//!
//! ```json
//! {
//!     "jsonrpc": "2.0",
//!     "method": "da_submit_assertion",
//!     "params": ["0xabcd1234", "0x9876543210fedcba..."],
//!     "id": 1
//! }
//! ```
//!
//! Parameters:
//! 1. `id`: Hex-encoded assertion ID
//! 2. `bytecode`: Hex-encoded assertion bytecode
//!
//! #### Success Response
//!
//! ```json
//! {
//!     "jsonrpc": "2.0",
//!     "result": null,
//!     "id": 1
//! }
//! ```
//!
//! #### Error Response
//!
//! ```json
//! {
//!     "jsonrpc": "2.0",
//!     "error": {
//!         "code": 500,
//!         "message": "Failed to decode hex"
//!     },
//!     "id": 1
//! }
//! ```
//!
//! ### `da_get_assertion`
//!
//! Returns the bytecode of an assertion by ID.
//!
//! #### Request
//!
//! ```json
//! {
//!     "jsonrpc": "2.0",
//!     "method": "da_get_assertion",
//!     "params": ["0xabcd1234"],
//!     "id": 1
//! }
//! ```
//!
//! Parameters:
//! 1. `id`: Hex-encoded assertion ID
//!
//! #### Success Response
//!
//! ```json
//! {
//!     "jsonrpc": "2.0",
//!     "result": "9876543210fedcba...",
//!     "id": 1
//! }
//! ```
//!
//! #### Error Response
//!
//! ```json
//! {
//!     "jsonrpc": "2.0",
//!     "error": {
//!         "code": 404,
//!         "message": "Assertion not found"
//!     },
//!     "id": 1
//! }
//! ```
//!
//! ## Error Codes
//!
//! - 500: Failed to decode hex input
//! - 404: Assertion not found
//! - -32601: Method not found
//! - -32602: Invalid parameters
//! - -32603: Internal error

pub mod accept;
pub mod assertion_submission;
pub mod db;
pub mod process_request;
pub mod source_compilation;
pub mod types;

use bollard::Docker;
use std::{
    net::SocketAddr,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

pub use crate::config::Config;

use crate::api::types::DbRequest;

use hyper_util::rt::TokioIo;
use tokio::{
    net::{
        TcpListener,
        TcpStream,
    },
    sync::mpsc,
};

use anyhow::Result;

use alloy::signers::local::PrivateKeySigner;

/// Start the API server
///
/// # Panics
///
/// Will panic if the private key is invalid
pub async fn serve(
    listener: TcpListener,
    db_tx: mpsc::UnboundedSender<DbRequest>,
    docker: Arc<Docker>,
    pk_str: String,
    cancel_token: CancellationToken,
) -> Result<()> {
    let pk_bytes = pk_str.parse().expect("Invalid Private Key");
    let signer =
        PrivateKeySigner::from_bytes(&pk_bytes).expect("Failed to create private key signer");

    // We start a loop to continuously accept incoming connections
    loop {
        tokio::select! {
                () = cancel_token.cancelled() => {
                    tracing::info!("Api received cancellation signal, shutting down...");
                    break;
                }
                res = listener.accept() => {
                    match res {
                        Ok((stream, socketaddr)) => {
                            serve_connection(
                                socketaddr,
                                &db_tx,
                                docker.clone(),
                                signer.clone(),
                                cancel_token.clone(),
                                stream,
                            );
                        }
                        Err(err) => {
                            tracing::error!(?err, "Error accepting connection");
                        }
                    }
                }
        }
    }

    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn serve_connection(
    socketaddr: SocketAddr,
    db_tx: &mpsc::UnboundedSender<DbRequest>,
    docker: Arc<Docker>,
    signer: PrivateKeySigner,
    shutdown_token: CancellationToken,
    stream: TcpStream,
) {
    tracing::info!("Connection from: {}", socketaddr);

    // Use an adapter to access something implementing `tokio::io` traits as if they implement
    // `hyper::rt` IO traits.
    let io = TokioIo::new(stream);

    let db_clone = db_tx.clone();

    let docker_clone = docker.clone();
    let shutdown_token_clone = shutdown_token.clone();

    // Spawn a tokio task to serve multiple connections concurrently
    tokio::task::spawn(async move {
        crate::accept!(
            io,
            db_clone,
            &signer,
            docker_clone.clone(),
            socketaddr,
            shutdown_token_clone
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::signers::local::PrivateKeySigner;
    use bollard::Docker;
    use std::sync::Arc;
    use tokio::{
        net::TcpListener,
        sync::mpsc,
    };
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_serve() {
        // Setup a test listener
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Create channels and components
        let (db_tx, _) = mpsc::unbounded_channel();
        let docker = Arc::new(Docker::connect_with_local_defaults().unwrap());
        let signer = PrivateKeySigner::random();
        let pk = hex::encode(signer.to_bytes());

        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        // Start server in background
        let server_handle =
            tokio::spawn(
                async move { serve(listener, db_tx, docker, pk, cancel_token_clone).await },
            );

        // Create client and send request
        let client = reqwest::Client::new();
        let response = client
            .post(format!("http://{addr}"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "da_get_assertion",
                "params": ["0x1234"],
                "id": 1
            }))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let health_response = client
            .get(format!("http://{addr}/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(health_response.status(), 200);

        let ready_response = client
            .get(format!("http://{addr}/ready"))
            .send()
            .await
            .unwrap();
        assert_eq!(ready_response.status(), 503);

        // Cleanup
        cancel_token.cancel();
        server_handle.await.unwrap().unwrap();
    }
}
