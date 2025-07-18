use crate::api::assertion_submission::raw_bytecode_assertion;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

use crate::{
    api::{
        source_compilation::compile_solidity,
        types::{
            DbOperation,
            DbRequest,
            DbRequestSender,
            DbResponse,
        },
    },
    encode_args::encode_constructor_args,
};

use assertion_da_core::{
    DaFetchResponse,
    DaSubmission,
    DaSubmissionResponse,
};

use alloy::{
    primitives::{
        B256,
        Bytes,
        keccak256,
    },
    signers::{
        Signature,
        Signer,
        local::PrivateKeySigner,
    },
};
use anyhow::Result;

use bollard::Docker;
use metrics::{
    counter,
    gauge,
    histogram,
};
use serde::{
    Deserialize,
    Serialize,
};
use serde_json::{
    Value,
    json,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use http_body_util::BodyExt;
use hyper::{
    Error,
    Request,
};
use tracing::{
    debug,
    error,
    info,
    trace,
    warn,
};

/// Filter requests for malformed JSON and other issues like being too large.
///
/// Returns the validated request or a JSON-RPC error response.
#[tracing::instrument(
    level = "trace",
    skip_all,
    target = "api::verify_request",
    fields(request_id, client_addr)
)]
async fn verify_request<B: hyper::body::Body<Error = Error>>(
    req: Request<B>,
    request_id: &Uuid,
    client_ip: &str,
) -> Result<Value, String> {
    // Read body and parse as JSON-RPC request
    let body = match req.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(err) => {
            warn!(target: "json_rpc", "Failed to read request body: {}", err);
            return Err(rpc_error(
                &json!({}),
                -32600,
                "Invalid JSON-RPC request format",
            ));
        }
    };

    // Limit body size to 10MB
    if body.len() > 10 * 1024 * 1024 {
        warn!(target: "json_rpc", %request_id, %client_ip, "Request body too large");
        return Err(rpc_error_with_request_id(
            &json!(""),
            -32600,
            "Request body too large",
            &request_id,
        ));
    }

    let json_rpc: Value = match serde_json::from_slice(&body) {
        Ok(json_rpc) => json_rpc,
        Err(err) => {
            warn!(target: "json_rpc", %request_id, %client_ip, "Failed to parse JSON-RPC request: {}", err);
            return Err(rpc_error_with_request_id(
                &json!(""),
                -32600,
                "Invalid JSON-RPC request format",
                &request_id,
            ));
        }
    };

    // define json schema to validate against
    let schema = json!({
        "type": "object",
        "properties": {
            "jsonrpc": {
                "type": "string",
                "const": "2.0"
            },
            "method": {
                "type": "string"
            },
            "params": {
                "type": "array"
            },
            "id": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "number"},
                    {"type": "null"}
                ]
            }
        },
        "required": ["jsonrpc", "method", "id"],
        "additionalProperties": false
    });

    if !jsonschema::is_valid(&schema, &json_rpc) {
        warn!(target: "json_rpc", %request_id, %client_ip, "Invalid JSON-RPC request format");
        return Err(rpc_error_with_request_id(
            &json_rpc,
            -32600,
            "Invalid JSON-RPC request format",
            &request_id,
        ));
    }

    Ok(json_rpc)
}

/// Matches the incoming method sent by a client to a corresponding function.
#[tracing::instrument(
    level = "debug",
    skip_all,
    target = "api::match_method",
    fields(request_id, client_addr)
)]
pub async fn match_method<B>(
    req: Request<B>,
    db: &DbRequestSender,
    signer: &PrivateKeySigner,
    docker: Arc<Docker>,
    client_addr: SocketAddr,
) -> Result<String>
where
    B: hyper::body::Body<Error = Error>,
{
    // Generate unique request ID for correlation
    let request_id = Uuid::new_v4();
    let client_ip = client_addr.ip().to_string();

    // Add request context to the current tracing span
    tracing::Span::current().record("request_id", tracing::field::display(&request_id));
    tracing::Span::current().record("client_addr", tracing::field::display(&client_addr));

    let headers = req.headers().clone();

    let json_rpc = match verify_request(req, &request_id, &client_ip).await {
        Ok(json_rpc) => json_rpc,
        Err(err) => {
            // Log the error and return it
            warn!(target: "json_rpc", %request_id, %client_ip, "Request verification failed: {}", err);
            return Ok(err);
        }
    };

    let method = json_rpc["method"].as_str().unwrap_or_default();
    let json_rpc_id = json_rpc["id"].clone();
    let labels = [("http_method", method.to_string())];

    gauge!("api_requests_active", &labels).increment(1);
    counter!("api_requests_count", &labels).increment(1);

    info!(
        target: "json_rpc",
        %method,
        %request_id,
        %client_ip,
        json_rpc_id = %json_rpc_id,
        ?headers,
        "Received request"
    );

    let req_start = Instant::now();
    let result = match method {
        #[cfg(feature = "debug_assertions")]
        "da_submit_assertion" => {
            let rax = raw_bytecode_assertion(
                json_rpc.clone(),
                db,
                signer,
                &request_id,
                &json_rpc_id,
                &client_ip,
            )
            .await;

            histogram!(
                "da_request_duration_seconds",
                "method" => "submit_solidity_assertion",
            )
            .record(req_start.elapsed().as_secs_f64());
            gauge!("api_requests_active", &labels).decrement(1);

            rax
        }
        "da_submit_solidity_assertion" => {
            let da_submission: DaSubmission = match serde_json::from_value(
                json_rpc["params"][0].clone(),
            ) {
                Ok(da_submission) => da_submission,
                Err(err) => {
                    warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, "Failed to parse DaSubmission payload");
                    return Ok(rpc_error_with_request_id(
                        &json_rpc,
                        -32602,
                        format!("Invalid params: Failed to parse payload {err:?}").as_str(),
                        &request_id,
                    ));
                }
            };

            debug!(target: "json_rpc", compiler_version = da_submission.compiler_version, da_submission.solidity_source , "Compiling solidity source");

            let bytecode = match compile_solidity(
                da_submission.assertion_contract_name.as_str(),
                da_submission.solidity_source.as_str(),
                da_submission.compiler_version.as_str(),
                docker,
            )
            .await
            {
                Ok(bytecode) => bytecode,
                Err(err) => {
                    warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, compiler_version = da_submission.compiler_version, contract_name = da_submission.assertion_contract_name, "Solidity compilation failed");
                    return Ok(rpc_error_with_request_id(
                        &json_rpc,
                        -32603,
                        &format!("Solidity Compilation Error: {err}"),
                        &request_id,
                    ));
                }
            };

            let encoded_constructor_args = match encode_constructor_args(
                &da_submission.constructor_abi_signature,
                da_submission.constructor_args,
            ) {
                Ok(encoded_args) => encoded_args,
                Err(err) => {
                    warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, constructor_abi = da_submission.constructor_abi_signature, "Constructor args ABI encoding failed");
                    return Ok(rpc_error_with_request_id(
                        &json_rpc,
                        -32603,
                        &format!("Constructor args ABI Encoding Error: {err}"),
                        &request_id,
                    ));
                }
            };

            let mut deployment_data = bytecode.clone();
            deployment_data.extend_from_slice(&encoded_constructor_args);

            // Hash to get ID
            let id = keccak256(&deployment_data);
            let prover_signature = match signer.sign_hash(&id).await {
                Ok(sig) => sig,
                Err(err) => {
                    warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, "Failed to sign assertion");
                    return Ok(rpc_error_with_request_id(
                        &json_rpc,
                        -32604,
                        "Internal Error: Failed to sign Assertion",
                        &request_id,
                    ));
                }
            };
            trace!(target: "json_rpc", ?id, ?prover_signature, bytecode_hex = ?deployment_data, "Compiled solidity source");

            let stored_assertion = StoredAssertion::new(
                da_submission.assertion_contract_name.clone(),
                da_submission.compiler_version.clone(),
                da_submission.solidity_source,
                deployment_data,
                prover_signature,
                da_submission.constructor_abi_signature,
                encoded_constructor_args,
            );

            let res = process_add_assertion(
                id,
                stored_assertion,
                db,
                &json_rpc,
                request_id,
                &client_ip,
                &json_rpc_id,
            )
            .await;

            if let Ok(ref response) = res {
                if !response.contains("\"error\"") {
                    info!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, contract_name = da_submission.assertion_contract_name, compiler_version = da_submission.compiler_version, "Successfully compiled Solidity assertion");
                } else {
                    warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, contract_name = da_submission.assertion_contract_name, compiler_version = da_submission.compiler_version, "Failed to process Solidity assertion");
                }
            }

            histogram!(
                "da_request_duration_seconds",
                "method" => "submit_solidity_assertion",
            )
            .record(req_start.elapsed().as_secs_f64());
            gauge!("api_requests_active", &labels).decrement(1);
            res
        }
        "da_get_assertion" => {
            let id = match json_rpc["params"][0].as_str() {
                Some(id) => id,
                None => {
                    warn!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, "Invalid params: missing id parameter");
                    return Ok(rpc_error_with_request_id(
                        &json_rpc,
                        -32602,
                        "Invalid params: Didn't find id",
                        &request_id,
                    ));
                }
            };

            // Validate hex input
            let id: B256 = match id.trim_start_matches("0x").parse() {
                Ok(id) => id,
                _ => {
                    warn!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, id = id, "Failed to decode hex ID");
                    return Ok(rpc_error_with_request_id(
                        &json_rpc,
                        -32605,
                        "Internal Error: Failed to decode hex of id",
                        &request_id,
                    ));
                }
            };

            debug!(target: "json_rpc", ?id, "Getting assertion");

            let res =
                process_get_assertion(id, db, &json_rpc, request_id, &client_ip, &json_rpc_id)
                    .await;

            // Log success for get_assertion if not an error response
            if let Ok(ref response) = res
                && !response.contains("\"error\"")
            {
                info!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Successfully retrieved assertion");
            }
            histogram!(
                "da_request_duration_seconds",
                "method" => "get_assertion",
            )
            .record(req_start.elapsed().as_secs_f64());
            gauge!("api_requests_active", &labels).decrement(1);
            res
        }
        _ => {
            warn!(target: "json_rpc", method = method, %request_id, %client_ip, json_rpc_id = %json_rpc_id, "Unknown JSON-RPC method");
            gauge!("api_requests_active", &labels).decrement(1);
            counter!("api_requests_error_count", &labels).increment(1);
            histogram!(
                "da_request_duration_seconds",
                "method" => "unknown"
            )
            .record(req_start.elapsed().as_secs_f64());
            Ok(rpc_error_with_request_id(
                &json_rpc,
                -32601,
                "Method not found",
                &request_id,
            ))
        }
    };

    // Log final request completion status
    match &result {
        Ok(response) => {
            if response.contains("\"error\"") {
                debug!(target: "json_rpc", %method, %request_id, %client_ip, json_rpc_id = %json_rpc_id, duration_ms = req_start.elapsed().as_millis(), "Request completed with error");
            } else {
                info!(target: "json_rpc", %method, %request_id, %client_ip, json_rpc_id = %json_rpc_id, duration_ms = req_start.elapsed().as_millis(), "Request completed successfully");
            }
        }
        Err(err) => {
            warn!(target: "json_rpc", %method, %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, duration_ms = req_start.elapsed().as_millis(), "Request failed with internal error");
        }
    }

    result
}

/// Struct to store an assertion in the database.
#[derive(Debug, Deserialize, Serialize)]
pub struct StoredAssertion {
    /// The name of the assertion contract.
    pub assertion_contract_name: String,
    /// The compiler version that was used to compile the assertion
    pub compiler_version: String,
    /// The solidity source code of the assertion.
    pub solidity_source: String,
    /// The bytecode of the assertion.
    pub bytecode: Vec<u8>,
    /// The prover's signature of the assertion.
    pub prover_signature: Signature,
    /// The abi signature of the constructor.
    pub constructor_abi_signature: String,
    /// The encoded constructor arguments.
    pub encoded_constructor_args: Bytes,
}

impl StoredAssertion {
    pub fn new(
        assertion_contract_name: impl Into<String>,
        compiler_version: impl Into<String>,
        solidity_source: impl Into<String>,
        bytecode: impl Into<Vec<u8>>,
        prover_signature: Signature,
        constructor_abi_signature: String,
        encoded_constructor_args: Bytes,
    ) -> Self {
        Self {
            assertion_contract_name: assertion_contract_name.into(),
            compiler_version: compiler_version.into(),
            solidity_source: solidity_source.into(),
            bytecode: bytecode.into(),
            prover_signature,
            constructor_abi_signature,
            encoded_constructor_args,
        }
    }
}

pub async fn process_add_assertion(
    id: B256,
    stored_assertion: StoredAssertion,
    db: &DbRequestSender,
    json_rpc: &Value,
    request_id: Uuid,
    client_ip: &str,
    json_rpc_id: &Value,
) -> Result<String> {
    // Store in database
    let (tx, rx) = oneshot::channel();

    let ser_assertion = match bincode::serialize(&stored_assertion) {
        Ok(ser) => ser,
        Err(err) => {
            error!(target: "json_rpc", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, "Failed to serialize assertion for database storage");
            return Ok(rpc_error_with_request_id(
                json_rpc,
                -32603,
                "Failed to deserialize assertion internally.",
                &request_id,
            ));
        }
    };

    let req = DbRequest {
        request: DbOperation::Insert(id.to_vec(), ser_assertion),
        response: tx,
    };

    db.send(req)?;

    let result = DaSubmissionResponse {
        id,
        prover_signature: stored_assertion.prover_signature.as_bytes().into(),
    };

    match rx.await {
        Ok(_) => {
            debug!(target: "json_rpc", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Successfully stored assertion in database");
            Ok(rpc_response(json_rpc, result))
        }
        Err(err) => {
            error!(target: "json_rpc", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, "Database operation failed for assertion storage");
            Ok(rpc_error_with_request_id(
                json_rpc,
                -32603,
                "Failed to write to database. Please try again later.",
                &request_id,
            ))
        }
    }
}

async fn process_get_assertion(
    id: B256,
    db: &DbRequestSender,
    json_rpc: &Value,
    request_id: Uuid,
    client_ip: &str,
    json_rpc_id: &Value,
) -> Result<String> {
    let (tx, rx) = oneshot::channel();
    let req = DbRequest {
        request: DbOperation::Get(id.to_vec()),
        response: tx,
    };

    db.send(req)?;
    let res = rx.await?;

    match res {
        Some(DbResponse::Value(val)) => {
            let stored_assertion: StoredAssertion = bincode::deserialize(&val)?;

            let result = DaFetchResponse {
                solidity_source: stored_assertion.solidity_source,
                bytecode: stored_assertion.bytecode.into(),
                prover_signature: stored_assertion.prover_signature.as_bytes().into(),
                encoded_constructor_args: stored_assertion.encoded_constructor_args,
                constructor_abi_signature: stored_assertion.constructor_abi_signature,
            };

            Ok(rpc_response(json_rpc, result))
        }
        None => {
            warn!(target: "json_rpc", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Assertion not found in database");
            Ok(rpc_error_with_request_id(
                json_rpc,
                404,
                "Assertion not found",
                &request_id,
            ))
        }
    }
}

fn rpc_response<T: Serialize>(request: &Value, result: T) -> String {
    json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": request["id"]
    })
    .to_string()
}

#[allow(dead_code)]
fn rpc_error(request: &Value, code: i128, message: &str) -> String {
    json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message
        },
        "id": request["id"]
    })
    .to_string()
}

pub fn rpc_error_with_request_id(
    request: &Value,
    code: i128,
    message: &str,
    request_id: &Uuid,
) -> String {
    json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message,
            "data": {
                "request_id": request_id.to_string()
            }
        },
        "id": request["id"]
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        db::listen_for_db,
        serve,
    };
    use alloy::primitives::hex;
    use sled::Config as DbConfig;
    use tempfile::TempDir;
    use tokio::{
        net::TcpListener,
        sync::mpsc,
    };
    use tokio_util::sync::CancellationToken;

    async fn setup_test_env() -> (TempDir, DbRequestSender, PrivateKeySigner, String) {
        // Create a temporary directory for the database
        let temp_dir = TempDir::new().unwrap();

        // Create channels for database communication
        let (db_sender, db_receiver) = mpsc::unbounded_channel();

        // Set up the database
        let db = DbConfig::new().path(&temp_dir).open().unwrap();

        // Create a random signer for testing
        let signer = PrivateKeySigner::random();

        // Set up test server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_url = format!("http://{addr}");

        // Start the server
        let db_sender_clone = db_sender.clone();
        let pk = hex::encode(signer.to_bytes());

        let docker = Arc::new(Docker::connect_with_local_defaults().unwrap());

        tokio::spawn(async move {
            serve(
                listener,
                db_sender_clone,
                docker,
                pk,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        });

        // Start the database listener
        tokio::spawn(async move {
            listen_for_db(db_receiver, db, CancellationToken::new())
                .await
                .unwrap();
        });

        (temp_dir, db_sender, signer, server_url)
    }

    #[tokio::test]
    #[cfg(feature = "debug_assertions")]
    async fn test_submit_assertion() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        // Bytecode for a simple contract
        let bytecode = "608060405234801561001057600080fd5b50610150806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c80632e64cec11461003b5780636057361d14610059575b600080fd5b610043610075565b60405161005091906100a1565b60405180910390f35b610073600480360381019061006e91906100ed565b61007e565b005b60008054905090565b8060008190555050565b6000819050919050565b61009b81610088565b82525050565b60006020820190506100b66000830184610092565b92915050565b600080fd5b6100ca81610088565b81146100d557600080fd5b50565b6000813590506100e7816100c1565b92915050565b600060208284031215610103576101026100bc565b5b6000610111848285016100d8565b9150509291505056fea2646970667358221220ec5ef0b1d98f01639556c6ea2467c504e5f0d7d7c6eb9b622bb6d8ac93d5fed464736f6c63430008110033";

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_assertion",
            "params": [
                bytecode
            ],
            "id": 1
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&server_url)
            .json(&request_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let response_json: Value = response.json().await.unwrap();
        dbg!(&response_json);
        assert_eq!(response_json["jsonrpc"], "2.0");
        assert_eq!(response_json["id"], 1);
        assert!(response_json["result"]["id"].is_string());
        assert!(response_json["result"]["prover_signature"].is_string());
    }

    #[tokio::test]
    async fn test_submit_solidity_assertion() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let source_code = r#"
            // SPDX-License-Identifier: MIT
            pragma solidity ^0.8.0;
            
            contract SimpleStorage {
                uint256 private value;
                
                function set(uint256 _value) public {
                    value = _value;
                }
                
                function get() public view returns (uint256) {
                    return value;
                }
            }
        "#;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_solidity_assertion",
            "params": [
                {
                    "solidity_source": source_code,
                    "compiler_version": "0.8.17",
                    "assertion_contract_name": "SimpleStorage",
                    "constructor_args": [],
                    "constructor_abi_signature": "constructor()"
                }
            ],
            "id": 1
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&server_url)
            .json(&request_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let response_json: Value = response.json().await.unwrap();
        assert_eq!(response_json["jsonrpc"], "2.0");
        assert_eq!(response_json["id"], 1);
        println!("{response_json:?}");
        let id = &response_json["result"]["id"];
        assert!(id.is_string());
        assert!(response_json["result"]["prover_signature"].is_string());

        let request_with_args = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_solidity_assertion",
            "params": [
                {
                    "solidity_source": source_code,
                    "compiler_version": "0.8.17",
                    "assertion_contract_name": "SimpleStorage",
                    "constructor_args": ["42"],
                    "constructor_abi_signature": "constructor(uint256)"
                }
            ],
            "id": 2
        });

        let response_with_args = client
            .post(&server_url)
            .json(&request_with_args)
            .send()
            .await
            .unwrap();

        assert_eq!(response_with_args.status(), 200);
        let response_json_with_args: Value = response_with_args.json().await.unwrap();
        assert_eq!(response_json_with_args["jsonrpc"], "2.0");
        assert_eq!(response_json_with_args["id"], 2);
        let id_with_args = &response_json_with_args["result"]["id"];
        assert!(id_with_args.is_string());
        assert!(response_json_with_args["result"]["prover_signature"].is_string());
        assert_ne!(id, id_with_args);
    }

    #[tokio::test]
    async fn test_get_assertion() {
        let (_temp_dir, db_sender, signer, server_url) = setup_test_env().await;

        // First submit an assertion
        let source_code = "contract Test { }";
        let stored_assertion = StoredAssertion {
            assertion_contract_name: "Test".to_string(),
            compiler_version: "0.8.17".to_string(),
            solidity_source: source_code.to_string(),
            bytecode: vec![1, 2, 3, 4],
            prover_signature: signer.sign_hash(&keccak256([1, 2, 3, 4])).await.unwrap(),
            constructor_abi_signature: "constructor()".to_string(),
            encoded_constructor_args: Bytes::new(),
        };

        let id = keccak256(&stored_assertion.bytecode);

        // Store directly in database
        let (tx, _) = oneshot::channel();
        let ser_assertion = bincode::serialize(&stored_assertion).unwrap();
        let _ = db_sender.send(DbRequest {
            request: DbOperation::Insert(id.to_vec(), ser_assertion),
            response: tx,
        });

        // Now try to fetch it
        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_get_assertion",
            "params": [format!("0x{}", hex::encode(id))],
            "id": 1
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&server_url)
            .json(&request_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let response_json: Value = response.json().await.unwrap();
        assert_eq!(response_json["jsonrpc"], "2.0");
        assert_eq!(response_json["id"], 1);

        let result = response_json["result"].as_object().unwrap();
        assert_eq!(result["solidity_source"].as_str().unwrap(), source_code);
        assert_eq!(
            hex::decode(
                result["bytecode"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("0x")
            )
            .unwrap(),
            vec![1, 2, 3, 4]
        );
    }

    #[tokio::test]
    async fn test_get_nonexistent_assertion() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_get_assertion",
            "params": ["0x1234567890123456789012345678901234567890123456789012345678901234"],
            "id": 1
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&server_url)
            .json(&request_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let response_json: Value = response.json().await.unwrap();
        assert_eq!(response_json["jsonrpc"], "2.0");
        assert_eq!(response_json["id"], 1);
        assert_eq!(response_json["error"]["code"], 404);
        assert_eq!(response_json["error"]["message"], "Assertion not found");
    }

    #[tokio::test]
    async fn test_invalid_method() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "invalid_method",
            "params": [],
            "id": 1
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&server_url)
            .json(&request_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let response_json: Value = response.json().await.unwrap();
        assert_eq!(response_json["jsonrpc"], "2.0");
        assert_eq!(response_json["id"], 1);
        assert_eq!(response_json["error"]["code"], -32601);
        assert_eq!(response_json["error"]["message"], "Method not found");
    }

    #[tokio::test]
    async fn test_request_size_limit() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        // Create a large payload that exceeds the 10MB limit
        let large_data = "x".repeat(11 * 1024 * 1024); // 11MB of data
        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_assertion",
            "params": [large_data],
            "id": 1
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&server_url)
            .header("Content-Type", "application/json")
            .body(request_body.to_string())
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let response_json: Value = response.json().await.unwrap();
        assert_eq!(response_json["jsonrpc"], "2.0");
        assert_eq!(response_json["error"]["code"], -32600);
        assert_eq!(response_json["error"]["message"], "Request body too large");
    }

    #[tokio::test]
    async fn test_invalid_json_schema() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        // Test 1: Missing required field (method)
        let request_body = json!({
            "jsonrpc": "2.0",
            "id": 1
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&server_url)
            .json(&request_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);

        let response_json: Value = response.json().await.unwrap();
        assert_eq!(response_json["jsonrpc"], "2.0");
        assert_eq!(response_json["id"], 1);
        assert_eq!(response_json["error"]["code"], -32600);
        assert_eq!(
            response_json["error"]["message"],
            "Invalid JSON-RPC request format"
        );

        // Test 2: Missing jsonrpc field
        let request_body2 = json!({
            "method": "da_submit_assertion",
            "params": [],
            "id": 2
        });

        let response2 = client
            .post(&server_url)
            .json(&request_body2)
            .send()
            .await
            .unwrap();

        assert_eq!(response2.status(), 200);

        let response_json2: Value = response2.json().await.unwrap();
        assert_eq!(response_json2["jsonrpc"], "2.0");
        assert_eq!(response_json2["id"], 2);
        assert_eq!(response_json2["error"]["code"], -32600);
        assert_eq!(
            response_json2["error"]["message"],
            "Invalid JSON-RPC request format"
        );

        // Test 3: Wrong jsonrpc version
        let request_body3 = json!({
            "jsonrpc": "1.0",
            "method": "da_submit_assertion",
            "params": [],
            "id": 3
        });

        let response3 = client
            .post(&server_url)
            .json(&request_body3)
            .send()
            .await
            .unwrap();

        assert_eq!(response3.status(), 200);

        let response_json3: Value = response3.json().await.unwrap();
        assert_eq!(response_json3["jsonrpc"], "2.0");
        assert_eq!(response_json3["id"], 3);
        assert_eq!(response_json3["error"]["code"], -32600);
        assert_eq!(
            response_json3["error"]["message"],
            "Invalid JSON-RPC request format"
        );

        // Test 4: Wrong params type (should be array)
        let request_body4 = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_assertion",
            "params": "not_an_array",
            "id": 4
        });

        let response4 = client
            .post(&server_url)
            .json(&request_body4)
            .send()
            .await
            .unwrap();

        assert_eq!(response4.status(), 200);

        let response_json4: Value = response4.json().await.unwrap();
        assert_eq!(response_json4["jsonrpc"], "2.0");
        assert_eq!(response_json4["id"], 4);
        assert_eq!(response_json4["error"]["code"], -32600);
        assert_eq!(
            response_json4["error"]["message"],
            "Invalid JSON-RPC request format"
        );

        // Test 5: Additional properties not allowed
        let request_body5 = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_assertion",
            "params": [],
            "id": 5,
            "extra": "field"
        });

        let response5 = client
            .post(&server_url)
            .json(&request_body5)
            .send()
            .await
            .unwrap();

        assert_eq!(response5.status(), 200);

        let response_json5: Value = response5.json().await.unwrap();
        assert_eq!(response_json5["jsonrpc"], "2.0");
        assert_eq!(response_json5["id"], 5);
        assert_eq!(response_json5["error"]["code"], -32600);
        assert_eq!(
            response_json5["error"]["message"],
            "Invalid JSON-RPC request format"
        );
    }

    #[tokio::test]
    async fn test_jsonschema_validation_behavior() {
        let schema = json!({
            "type": "object",
            "properties": {
                "jsonrpc": {
                    "type": "string",
                    "const": "2.0"
                },
                "method": {
                    "type": "string"
                },
                "params": {
                    "type": "array"
                },
                "id": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "number"},
                        {"type": "null"}
                    ]
                }
            },
            "required": ["jsonrpc", "method", "id"],
            "additionalProperties": false
        });

        // Test various inputs
        let valid_request = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_assertion",
            "params": [],
            "id": 1
        });

        let missing_method = json!({
            "jsonrpc": "2.0",
            "id": 1
        });

        let wrong_params_type = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_assertion",
            "params": "not_array",
            "id": 1
        });

        let empty_object = json!({});

        let extra_field = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_assertion",
            "params": [],
            "id": 1,
            "extra": "field"
        });

        // With proper JSON Schema, validation should work correctly
        println!(
            "Valid request: {}",
            jsonschema::is_valid(&schema, &valid_request)
        );
        println!(
            "Missing method: {}",
            jsonschema::is_valid(&schema, &missing_method)
        );
        println!(
            "Wrong params type: {}",
            jsonschema::is_valid(&schema, &wrong_params_type)
        );
        println!(
            "Empty object: {}",
            jsonschema::is_valid(&schema, &empty_object)
        );
        println!(
            "Extra field: {}",
            jsonschema::is_valid(&schema, &extra_field)
        );

        // Assertions for proper schema validation
        assert!(jsonschema::is_valid(&schema, &valid_request));
        assert!(!jsonschema::is_valid(&schema, &missing_method));
        assert!(!jsonschema::is_valid(&schema, &wrong_params_type));
        assert!(!jsonschema::is_valid(&schema, &empty_object));
        assert!(!jsonschema::is_valid(&schema, &extra_field));
    }
}
