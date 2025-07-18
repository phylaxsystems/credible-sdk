use crate::api::process_request::StoredAssertion;
use crate::api::process_request::process_add_assertion;
use crate::api::process_request::rpc_error_with_request_id;
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

/// Accepts, validates and stores a raw EVM bytecode assertion.
#[cfg(feature = "debug_assertions")]
pub async fn raw_bytecode_assertion(
    json_rpc: Value,
    db: &DbRequestSender,
    signer: &PrivateKeySigner,
    request_id: &Uuid,
    json_rpc_id: &Value,
    client_ip: &str,
) -> Result<String> {
    let code = match json_rpc["params"][0].as_str() {
        Some(code) => code,
        _ => {
            warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, "Invalid params: missing or invalid bytecode parameter");
            return Ok(rpc_error_with_request_id(
                &json_rpc,
                -32602,
                "Invalid params",
                &request_id,
            ));
        }
    };

    // Validate hex inputs
    let bytecode = match alloy::hex::decode(code.trim_start_matches("0x")) {
        Ok(code) => code,
        _ => {
            warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, code = code, "Failed to decode hex bytecode");
            return Ok(rpc_error_with_request_id(
                &json_rpc,
                500,
                "Failed to decode hex",
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
            return Ok(rpc_error_with_request_id(
                &json_rpc,
                -32604,
                "Internal Error: Failed to sign Assertion",
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
        *request_id,
        &client_ip,
        &json_rpc_id,
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

    res
}
