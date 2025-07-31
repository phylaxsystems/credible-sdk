//! JSON-RPC server implementation for the sidecar

use axum::{
    extract::Json,
    http::StatusCode,
    response::Json as ResponseJson,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<serde_json::Value>,
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<JsonRpcError>,
    pub id: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

async fn handle_rpc_request(Json(request): Json<JsonRpcRequest>) -> Result<ResponseJson<JsonRpcResponse>, StatusCode> {
    let response = match request.method.as_str() {
        "ping" => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: Some(serde_json::json!("pong")),
            error: None,
            id: request.id,
        },
        _ => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code: -32601,
                message: "Method not found".to_string(),
            }),
            id: request.id,
        },
    };

    Ok(ResponseJson(response))
}

/// Start the JSON-RPC server
pub async fn start_rpc_server(addr: SocketAddr) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", post(handle_rpc_request));

    println!("Attempting to bind to address: {}", addr);
    
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => {
            println!("Successfully bound to address: {}", addr);
            listener
        }
        Err(e) => {
            eprintln!("Failed to bind to address {}: {}", addr, e);
            eprintln!("Error details: {:#?}", e);

            return Err(anyhow::anyhow!("Failed to bind to {}: {}", addr, e));
        }
    };

    println!("JSON-RPC server started successfully at {}", addr);
    axum::serve(listener, app).await?;
    
    Ok(())
}
