use std::{
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

use crate::{
    api::{
        json_validation::{
            JsonRpcErrorCode,
            JsonRpcRequest,
            MAX_JSON_SIZE,
            sanitize_error_message,
            validate_da_submission_params,
            validate_hex_param,
        },
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
    info,
    trace,
    warn,
};

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

    // Read body with size limit
    let headers = req.headers().clone();
    let body = req.into_body().collect().await?.to_bytes();

    // Check payload size
    if body.len() > MAX_JSON_SIZE {
        warn!(target: "json_rpc", %request_id, %client_ip, size = body.len(), "Request payload too large");
        return Ok(json!({
            "jsonrpc": "2.0",
            "error": {
                "code": -32600,
                "message": "Request too large",
                "data": {
                    "request_id": request_id.to_string()
                }
            },
            "id": null
        })
        .to_string());
    }

    // Parse JSON
    let json_value: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            warn!(target: "json_rpc", %request_id, %client_ip, error = %e, "Failed to parse JSON");
            return Ok(json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": -32700,
                    "message": "Parse error",
                    "data": {
                        "request_id": request_id.to_string()
                    }
                },
                "id": null
            })
            .to_string());
        }
    };

    // Validate JSON-RPC structure
    let json_rpc = match JsonRpcRequest::validate(json_value) {
        Ok(req) => req,
        Err((code, msg)) => {
            warn!(target: "json_rpc", %request_id, %client_ip, error = msg, "Invalid JSON-RPC structure");
            return Ok(json!({
                "jsonrpc": "2.0",
                "error": {
                    "code": code as i32,
                    "message": sanitize_error_message(code, msg),
                    "data": {
                        "request_id": request_id.to_string()
                    }
                },
                "id": null
            })
            .to_string());
        }
    };

    let method = &json_rpc.method;
    let json_rpc_id = &json_rpc.id;
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
    let result = match method.as_str() {
        #[cfg(feature = "debug_assertions")]
        "da_submit_assertion" => {
            // Validate params structure first
            if let Some(params) = &json_rpc.params {
                if let Err(e) = validate_hex_param(params) {
                    warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = e, "Invalid params structure");
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InvalidParams,
                        sanitize_error_message(JsonRpcErrorCode::InvalidParams, e),
                        &request_id,
                    ));
                }
            }
            
            let code = match json_rpc.get_string_param(0) {
                Ok(code) => code,
                Err(e) => {
                    warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = e, "Invalid params: missing or invalid bytecode parameter");
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InvalidParams,
                        sanitize_error_message(JsonRpcErrorCode::InvalidParams, e),
                        &request_id,
                    ));
                }
            };

            // Validate hex inputs
            let bytecode = match alloy::hex::decode(code.trim_start_matches("0x")) {
                Ok(code) => code,
                _ => {
                    warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, code = code, "Failed to decode hex bytecode");
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InvalidParams,
                        sanitize_error_message(JsonRpcErrorCode::InvalidParams, ""),
                        &request_id,
                    ));
                }
            };

            debug!(target: "json_rpc", bytecode_len = bytecode.len(), "Submitting raw assertion bytecode");

            // Hash to get ID
            let id = keccak256(&bytecode);
            let signature = match signer.sign_hash(&id).await {
                Ok(sig) => sig,
                Err(err) => {
                    warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, "Failed to sign assertion");
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InternalError,
                        sanitize_error_message(JsonRpcErrorCode::InternalError, ""),
                        &request_id,
                    ));
                }
            };

            trace!(target: "json_rpc", ?id, ?signature, bytecode_hex = hex::encode(&bytecode), "Raw submitted bytecode");
            debug!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Processed raw assertion submission, proceeding to database storage");

            let stored_assertion = StoredAssertion::new(
                "NaN".to_string(),
                "NaN".to_string(),
                String::new(),
                bytecode,
                signature,
                "constructor()".to_string(),
                Bytes::new(),
            );

            let res = process_add_assertion(
                id,
                stored_assertion,
                db,
                &json_rpc,
                request_id,
                &client_ip,
                json_rpc_id,
            )
            .await;

            // Log success or failure based on response
            if let Ok(ref response) = res {
                if !response.contains("\"error\"") {
                    info!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Successfully processed raw assertion submission");
                } else {
                    warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Failed to process raw assertion submission");
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
        "da_submit_solidity_assertion" => {
            // Validate params structure first
            if let Some(params) = &json_rpc.params {
                if let Err(e) = validate_da_submission_params(params) {
                    warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = e, "Invalid params structure");
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InvalidParams,
                        sanitize_error_message(JsonRpcErrorCode::InvalidParams, e),
                        &request_id,
                    ));
                }
            }
            
            let da_submission: DaSubmission = match json_rpc.deserialize_param(0) {
                Ok(da_submission) => da_submission,
                Err(e) => {
                    warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = e, "Failed to parse DaSubmission payload");
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InvalidParams,
                        sanitize_error_message(JsonRpcErrorCode::InvalidParams, e),
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
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InternalError,
                        sanitize_error_message(JsonRpcErrorCode::InternalError, ""),
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
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InternalError,
                        sanitize_error_message(JsonRpcErrorCode::InternalError, ""),
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
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InternalError,
                        sanitize_error_message(JsonRpcErrorCode::InternalError, ""),
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
                json_rpc_id,
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
            // Validate params structure first
            if let Some(params) = &json_rpc.params {
                if let Err(e) = validate_hex_param(params) {
                    warn!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = e, "Invalid params structure");
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InvalidParams,
                        sanitize_error_message(JsonRpcErrorCode::InvalidParams, e),
                        &request_id,
                    ));
                }
            }
            
            let id = match json_rpc.get_string_param(0) {
                Ok(id) => id,
                Err(e) => {
                    warn!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = e, "Invalid params: missing id parameter");
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InvalidParams,
                        sanitize_error_message(JsonRpcErrorCode::InvalidParams, e),
                        &request_id,
                    ));
                }
            };

            // Validate hex input
            let id: B256 = match id.trim_start_matches("0x").parse() {
                Ok(id) => id,
                _ => {
                    warn!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, id = id, "Failed to decode hex ID");
                    return Ok(rpc_error_with_request_id_obj(
                        &json_rpc,
                        JsonRpcErrorCode::InvalidParams,
                        sanitize_error_message(JsonRpcErrorCode::InvalidParams, ""),
                        &request_id,
                    ));
                }
            };

            debug!(target: "json_rpc", ?id, "Getting assertion");

            let res =
                process_get_assertion(id, db, &json_rpc, request_id, &client_ip, &json_rpc_id)
                    .await;

            // Log success for get_assertion if not an error response
            if let Ok(ref response) = res {
                if !response.contains("\"error\"") {
                    info!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Successfully retrieved assertion");
                }
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
            Ok(rpc_error_with_request_id_obj(
                &json_rpc,
                JsonRpcErrorCode::MethodNotFound,
                sanitize_error_message(JsonRpcErrorCode::MethodNotFound, ""),
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

async fn process_add_assertion(
    id: B256,
    stored_assertion: StoredAssertion,
    db: &DbRequestSender,
    json_rpc: &JsonRpcRequest,
    request_id: Uuid,
    client_ip: &str,
    json_rpc_id: &Value,
) -> Result<String> {
    // Store in database
    let (tx, rx) = oneshot::channel();

    let ser_assertion = match bincode::serialize(&stored_assertion) {
        Ok(ser) => ser,
        Err(err) => {
            warn!(target: "json_rpc", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, "Failed to serialize assertion for database storage");
            return Ok(rpc_error_with_request_id_obj(
                json_rpc,
                JsonRpcErrorCode::InternalError,
                sanitize_error_message(JsonRpcErrorCode::InternalError, ""),
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
            debug!(target: "json_rpc", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, "Database operation failed for assertion storage");
            Ok(rpc_error_with_request_id_obj(
                json_rpc,
                JsonRpcErrorCode::InternalError,
                sanitize_error_message(JsonRpcErrorCode::InternalError, ""),
                &request_id,
            ))
        }
    }
}

async fn process_get_assertion(
    id: B256,
    db: &DbRequestSender,
    json_rpc: &JsonRpcRequest,
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
            Ok(rpc_error_with_request_id_obj(
                json_rpc,
                JsonRpcErrorCode::InvalidParams,
                "Assertion not found",
                &request_id,
            ))
        }
    }
}

fn rpc_response<T: Serialize>(request: &JsonRpcRequest, result: T) -> String {
    json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": request.id
    })
    .to_string()
}

#[allow(dead_code)]
fn rpc_error(request: &JsonRpcRequest, code: i128, message: &str) -> String {
    json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message
        },
        "id": request.id
    })
    .to_string()
}

fn rpc_error_with_request_id_obj(
    request: &JsonRpcRequest,
    code: JsonRpcErrorCode,
    message: &str,
    request_id: &Uuid,
) -> String {
    json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code as i32,
            "message": message,
            "data": {
                "request_id": request_id.to_string()
            }
        },
        "id": request.id
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
        assert_eq!(response_json["error"]["code"], -32602);
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

    // Security test cases
    #[tokio::test]
    async fn test_malformed_json_not_object() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        // Send array instead of object
        let request_body = json!(["invalid", "json", "structure"]);

        let client = reqwest::Client::new();
        let response = client
            .post(&server_url)
            .json(&request_body)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 200);
        let response_json: Value = response.json().await.unwrap();
        assert_eq!(response_json["error"]["code"], -32600);
        // Should get sanitized error message
        assert_eq!(
            response_json["error"]["message"].as_str().unwrap(),
            "Invalid request"
        );
    }

    #[tokio::test]
    async fn test_missing_method_field() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let request_body = json!({
            "jsonrpc": "2.0",
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
        assert_eq!(response_json["error"]["code"], -32600);
        // Should get sanitized error message
        assert_eq!(
            response_json["error"]["message"].as_str().unwrap(),
            "Invalid request"
        );
    }

    #[tokio::test]
    async fn test_empty_params_array_access() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_get_assertion",
            "params": [],  // Empty array - will cause panic when accessing params[0]
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
        assert_eq!(response_json["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn test_params_not_array() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_get_assertion",
            "params": {"0": "0x1234"},  // Object instead of array
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
        assert!(response_json.get("error").is_some());
    }

    #[tokio::test]
    async fn test_prototype_pollution_attempt() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let request_body = json!({
            "__proto__": {"isAdmin": true},
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

        // Should handle gracefully without prototype pollution
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_deeply_nested_json() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        // Create deeply nested structure
        let mut nested = json!({"value": "test"});
        for _ in 0..1000 {
            nested = json!({"nested": nested});
        }

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_solidity_assertion",
            "params": [nested],
            "id": 1
        });

        let client = reqwest::Client::new();
        let response = client
            .post(&server_url)
            .json(&request_body)
            .send()
            .await
            .unwrap();

        // Should reject or handle gracefully
        assert_eq!(response.status(), 200);
        let response_json: Value = response.json().await.unwrap();
        assert!(response_json.get("error").is_some());
    }

    #[tokio::test]
    async fn test_invalid_hex_encoding_injection() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_get_assertion",
            "params": ["0xZZZZinvalid_hex"],
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
        assert_eq!(response_json["error"]["code"], -32602);
        // Should not expose internal error details
        let error_msg = response_json["error"]["message"].as_str().unwrap();
        assert!(!error_msg.contains("Failed to decode hex"));
        assert_eq!(error_msg, "Invalid parameters");
    }

    #[tokio::test]
    async fn test_null_injection_in_params() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_get_assertion",
            "params": [null],
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
        assert_eq!(response_json["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn test_type_confusion_in_da_submission() {
        let (_temp_dir, _db_sender, _signer, server_url) = setup_test_env().await;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "da_submit_solidity_assertion",
            "params": [{
                "solidity_source": ["not", "a", "string"],  // Array instead of string
                "compiler_version": 123,  // Number instead of string
                "assertion_contract_name": null,
                "constructor_args": "not_an_array",
                "constructor_abi_signature": {}
            }],
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
        assert_eq!(response_json["error"]["code"], -32602);
    }
}
