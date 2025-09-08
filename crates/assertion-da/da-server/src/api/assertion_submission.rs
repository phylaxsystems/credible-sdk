use std::sync::Arc;

use crate::{
    api::{
        process_request::{
            StoredAssertion,
            rpc_error_with_request_id,
            rpc_response,
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
        Signer,
        local::PrivateKeySigner,
    },
};
use anyhow::Result;

use bollard::Docker;
use serde_json::Value;
use tokio::sync::oneshot;
use uuid::Uuid;

use tracing::{
    debug,
    error,
    info,
    instrument,
    trace,
    warn,
};

/// Accepts, validates and stores a raw EVM bytecode assertion.
#[cfg(feature = "debug_assertions")]
#[instrument(
    skip_all,
    target = "assertion_submission::accept_bytecode_assertion",
    level = "debug",
    fields(request_id = %request_id, client_ip = %client_ip)
)]
pub async fn accept_bytecode_assertion(
    json_rpc: &Value,
    db: &DbRequestSender,
    signer: &PrivateKeySigner,
    request_id: &Uuid,
    json_rpc_id: &Value,
    client_ip: &str,
) -> Result<String> {
    let Some(code) = json_rpc["params"][0].as_str() else {
        warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, "Invalid params: missing or invalid bytecode parameter");
        return Ok(rpc_error_with_request_id(
            json_rpc,
            -32602,
            "Invalid params",
            request_id,
        ));
    };

    // Validate hex inputs
    let Ok(bytecode) = alloy::hex::decode(code.trim_start_matches("0x")) else {
        warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, code = code, "Failed to decode hex bytecode");
        return Ok(rpc_error_with_request_id(
            json_rpc,
            500,
            "Failed to decode hex",
            request_id,
        ));
    };

    debug!(target: "json_rpc", bytecode_len = bytecode.len(), "Submitting raw assertion bytecode");

    // Hash to get ID
    let id = keccak256(&bytecode);
    let signature = match signer.sign_hash(&id).await {
        Ok(sig) => sig,
        Err(err) => {
            warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, "Failed to sign assertion");
            return Ok(rpc_error_with_request_id(
                json_rpc,
                -32604,
                "Internal Error: Failed to sign Assertion",
                request_id,
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
        json_rpc,
        *request_id,
        client_ip,
        json_rpc_id,
    )
    .await;

    // Log success or failure based on response
    if let Ok(ref response) = res {
        if response.contains("\"error\"") {
            warn!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Failed to process raw assertion submission");
        } else {
            info!(target: "json_rpc", method = "da_submit_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Successfully processed raw assertion submission");
        }
    }

    res
}

/// Accept assertion written in solidity. Compiles the source code and verifies
/// it. Spins up a docker container as a part of the compilation job.
#[instrument(
    skip_all,
    target = "assertion_submission::accept_solidity_assertion",
    level = "debug",
    fields(request_id = %request_id, client_ip = %client_ip)
)]
pub async fn accept_solidity_assertion(
    json_rpc: &Value,
    db: &DbRequestSender,
    signer: &PrivateKeySigner,
    docker: Arc<Docker>,
    request_id: &Uuid,
    json_rpc_id: &Value,
    client_ip: &str,
) -> Result<String> {
    let da_submission: DaSubmission = match serde_json::from_value(json_rpc["params"][0].clone()) {
        Ok(da_submission) => da_submission,
        Err(err) => {
            warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, error = %err, "Failed to parse DaSubmission payload");
            return Ok(rpc_error_with_request_id(
                json_rpc,
                -32602,
                format!("Invalid params: Failed to parse payload {err:?}").as_str(),
                request_id,
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
                json_rpc,
                -32603,
                &format!("Solidity Compilation Error: {err}"),
                request_id,
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
                json_rpc,
                -32603,
                &format!("Constructor args ABI Encoding Error: {err}"),
                request_id,
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
                json_rpc,
                -32604,
                "Internal Error: Failed to sign Assertion",
                request_id,
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
        json_rpc,
        *request_id,
        client_ip,
        json_rpc_id,
    )
    .await;

    if let Ok(ref response) = res {
        if response.contains("\"error\"") {
            warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, contract_name = da_submission.assertion_contract_name, compiler_version = da_submission.compiler_version, "Failed to process Solidity assertion");
        } else {
            info!(target: "json_rpc", method = "da_submit_solidity_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, contract_name = da_submission.assertion_contract_name, compiler_version = da_submission.compiler_version, "Successfully compiled Solidity assertion");
        }
    }

    res
}

/// Retrieves assertion from the database.
#[instrument(
    skip_all,
    target = "assertion_submission::retreive_assertion",
    level = "debug",
    fields(request_id = %request_id, client_ip = %client_ip)
)]
pub async fn retreive_assertion(
    json_rpc: &Value,
    db: &DbRequestSender,
    request_id: &Uuid,
    json_rpc_id: &Value,
    client_ip: &str,
) -> Result<String> {
    let Some(id) = json_rpc["params"][0].as_str() else {
        warn!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, "Invalid params: missing id parameter");
        return Ok(rpc_error_with_request_id(
            json_rpc,
            -32602,
            "Invalid params: Didn't find id",
            request_id,
        ));
    };

    // Validate hex input
    let id: B256 = if let Ok(id) = id.trim_start_matches("0x").parse() {
        id
    } else {
        warn!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, id = id, "Failed to decode hex ID");
        return Ok(rpc_error_with_request_id(
            json_rpc,
            -32605,
            "Internal Error: Failed to decode hex of id",
            request_id,
        ));
    };

    debug!(target: "json_rpc", ?id, "Getting assertion");

    let res = process_get_assertion(id, db, json_rpc, *request_id, client_ip, json_rpc_id).await;

    // Log success for get_assertion if not an error response
    if let Ok(ref response) = res
        && !response.contains("\"error\"")
    {
        info!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, ?id, "Successfully retrieved assertion");
    }

    res
}

#[instrument(
    skip_all,
    target = "assertion_submission::process_add_assertion",
    level = "debug",
    fields(request_id = %request_id, client_ip = %client_ip, assertion_id = %id)
)]
async fn process_add_assertion(
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

#[instrument(
    skip_all,
    target = "assertion_submission::process_get_assertion",
    level = "debug",
    fields(request_id = %request_id, client_ip = %client_ip, assertion_id = %id)
)]
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
