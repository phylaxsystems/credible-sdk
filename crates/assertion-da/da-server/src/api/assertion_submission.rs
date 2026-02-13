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
        Signature,
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

pub struct SubmissionContext<'a> {
    pub json_rpc: &'a Value,
    pub db: &'a DbRequestSender,
    pub signer: &'a PrivateKeySigner,
    pub request_id: &'a Uuid,
    pub json_rpc_id: &'a Value,
    pub client_ip: &'a str,
}

impl SubmissionContext<'_> {
    fn parse_da_submission(&self) -> Result<DaSubmission, String> {
        serde_json::from_value(self.json_rpc["params"][0].clone()).map_err(|err| {
            warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %self.request_id, %self.client_ip, json_rpc_id = %self.json_rpc_id, error = ?err, "Failed to parse DaSubmission payload");
            rpc_error_with_request_id(
                self.json_rpc,
                -32602,
                format!("Invalid params: Failed to parse payload {err:?}").as_str(),
                self.request_id,
            )
        })
    }

    async fn compile_submission(
        &self,
        da_submission: &DaSubmission,
        docker: Arc<Docker>,
    ) -> Result<Vec<u8>, String> {
        compile_solidity(
            da_submission.assertion_contract_name.as_str(),
            da_submission.solidity_source.as_str(),
            da_submission.compiler_version.as_str(),
            docker,
        )
        .await
        .map_err(|err| {
            warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %self.request_id, %self.client_ip, json_rpc_id = %self.json_rpc_id, error = ?err, compiler_version = da_submission.compiler_version, contract_name = da_submission.assertion_contract_name, "Solidity compilation failed");
            rpc_error_with_request_id(
                self.json_rpc,
                -32603,
                &format!("Solidity Compilation Error: {err}"),
                self.request_id,
            )
        })
    }

    fn encode_submission_args(&self, da_submission: &DaSubmission) -> Result<Bytes, String> {
        encode_constructor_args(
            &da_submission.constructor_abi_signature,
            da_submission.constructor_args.clone(),
        )
        .map_err(|err| {
            warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %self.request_id, %self.client_ip, json_rpc_id = %self.json_rpc_id, error = ?err, constructor_abi = da_submission.constructor_abi_signature, "Constructor args ABI encoding failed");
            rpc_error_with_request_id(
                self.json_rpc,
                -32603,
                &format!("Constructor args ABI Encoding Error: {err}"),
                self.request_id,
            )
        })
    }

    async fn sign_assertion(&self, id: &B256) -> Result<Signature, String> {
        self.signer.sign_hash(id).await.map_err(|err| {
            warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %self.request_id, %self.client_ip, json_rpc_id = %self.json_rpc_id, error = ?err, "Failed to sign assertion");
            rpc_error_with_request_id(
                self.json_rpc,
                -32604,
                "Internal Error: Failed to sign Assertion",
                self.request_id,
            )
        })
    }
}

/// Accepts, validates and stores a raw EVM bytecode assertion.
#[cfg(feature = "debug_assertions")]
#[instrument(
    skip_all,
    target = "assertion_submission::accept_bytecode_assertion",
    level = "debug",
    fields(request_id = %ctx.request_id, client_ip = %ctx.client_ip)
)]
pub async fn accept_bytecode_assertion(ctx: &SubmissionContext<'_>) -> Result<String> {
    let Some(code) = ctx.json_rpc["params"][0].as_str() else {
        warn!(target: "json_rpc", method = "da_submit_assertion", %ctx.request_id, %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, "Invalid params: missing or invalid bytecode parameter");
        return Ok(rpc_error_with_request_id(
            ctx.json_rpc,
            -32602,
            "Invalid params",
            ctx.request_id,
        ));
    };

    let clean_code = match sanitize_single_hex_prefix(code).map_err(|err| {
        warn!(target: "json_rpc", method = "da_submit_assertion", %ctx.request_id, %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, code = code, error = ?err, "Invalid params: multiple 0x prefixes found in bytecode");
        rpc_error_with_request_id(
            ctx.json_rpc,
            400,
            "Failed to decode hex",
            ctx.request_id,
        )
    }) {
        Ok(code) => code,
        Err(err_response) => return Ok(err_response),
    };

    // Validate hex inputs
    let Ok(bytecode) = alloy::hex::decode(clean_code) else {
        warn!(target: "json_rpc", method = "da_submit_assertion", %ctx.request_id, %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, code = code, "Failed to decode hex bytecode");
        return Ok(rpc_error_with_request_id(
            ctx.json_rpc,
            500,
            "Failed to decode hex",
            ctx.request_id,
        ));
    };

    debug!(target: "json_rpc", bytecode_len = bytecode.len(), "Submitting raw assertion bytecode");

    // Hash to get ID
    let id = keccak256(&bytecode);
    let signature = match ctx.signer.sign_hash(&id).await {
        Ok(sig) => sig,
        Err(err) => {
            warn!(target: "json_rpc", method = "da_submit_assertion", %ctx.request_id, %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, error = ?err, "Failed to sign assertion");
            return Ok(rpc_error_with_request_id(
                ctx.json_rpc,
                -32604,
                "Internal Error: Failed to sign Assertion",
                ctx.request_id,
            ));
        }
    };

    trace!(target: "json_rpc", ?id, ?signature, bytecode_hex = hex::encode(&bytecode), "Raw submitted bytecode");
    debug!(target: "json_rpc", method = "da_submit_assertion", %ctx.request_id, %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, ?id, "Processed raw assertion submission, proceeding to database storage");

    let stored_assertion = StoredAssertion::new(
        "NaN".to_string(),
        "NaN".to_string(),
        String::new(),
        bytecode,
        signature,
        "constructor()".to_string(),
        Bytes::new(),
    );

    let res = process_add_assertion(id, stored_assertion, ctx).await;

    // Log success or failure based on response
    if let Ok(ref response) = res {
        if response.contains("\"error\"") {
            warn!(target: "json_rpc", method = "da_submit_assertion", %ctx.request_id, %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, ?id, "Failed to process raw assertion submission");
        } else {
            info!(target: "json_rpc", method = "da_submit_assertion", %ctx.request_id, %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, ?id, "Successfully processed raw assertion submission");
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
    fields(request_id = %ctx.request_id, client_ip = %ctx.client_ip)
)]
pub async fn accept_solidity_assertion(
    ctx: &SubmissionContext<'_>,
    docker: Arc<Docker>,
) -> Result<String> {
    let da_submission = match ctx.parse_da_submission() {
        Ok(da_submission) => da_submission,
        Err(err_response) => return Ok(err_response),
    };

    debug!(target: "json_rpc", compiler_version = da_submission.compiler_version, da_submission.solidity_source , "Compiling solidity source");

    let bytecode = match ctx.compile_submission(&da_submission, docker).await {
        Ok(bytecode) => bytecode,
        Err(err_response) => return Ok(err_response),
    };

    let encoded_constructor_args = match ctx.encode_submission_args(&da_submission) {
        Ok(encoded_args) => encoded_args,
        Err(err_response) => return Ok(err_response),
    };

    let mut deployment_data = bytecode.clone();
    deployment_data.extend_from_slice(&encoded_constructor_args);

    // Hash to get ID
    let id = keccak256(&deployment_data);
    let prover_signature = match ctx.sign_assertion(&id).await {
        Ok(sig) => sig,
        Err(err_response) => return Ok(err_response),
    };
    trace!(target: "json_rpc", ?id, ?prover_signature, bytecode_hex = ?deployment_data, "Compiled solidity source");

    let stored_assertion = StoredAssertion::new(
        da_submission.assertion_contract_name.clone(),
        da_submission.compiler_version.clone(),
        da_submission.solidity_source,
        bytecode,
        prover_signature,
        da_submission.constructor_abi_signature,
        encoded_constructor_args,
    );

    let res = process_add_assertion(id, stored_assertion, ctx).await;

    if let Ok(ref response) = res {
        if response.contains("\"error\"") {
            warn!(target: "json_rpc", method = "da_submit_solidity_assertion", %ctx.request_id, %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, ?id, contract_name = da_submission.assertion_contract_name, compiler_version = da_submission.compiler_version, "Failed to process Solidity assertion");
        } else {
            info!(target: "json_rpc", method = "da_submit_solidity_assertion", %ctx.request_id, %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, ?id, contract_name = da_submission.assertion_contract_name, compiler_version = da_submission.compiler_version, "Successfully compiled Solidity assertion");
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

    let clean_id = match sanitize_single_hex_prefix(id).map_err(|err| {
        warn!(target: "json_rpc", method = "da_get_assertion", %request_id, %client_ip, json_rpc_id = %json_rpc_id, id = id, error = ?err, "Invalid params: multiple 0x prefixes found in id");
        rpc_error_with_request_id(
            json_rpc,
            -32605,
            "Internal Error: Failed to decode hex of id",
            request_id,
        )
    }) {
        Ok(id) => id,
        Err(err_response) => return Ok(err_response),
    };

    // Validate hex input
    let id: B256 = if let Ok(id) = clean_id.parse() {
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
    fields(request_id = %ctx.request_id, client_ip = %ctx.client_ip, assertion_id = %id)
)]
async fn process_add_assertion(
    id: B256,
    stored_assertion: StoredAssertion,
    ctx: &SubmissionContext<'_>,
) -> Result<String> {
    // Store in database
    let (tx, rx) = oneshot::channel();

    let ser_assertion = match bincode::serialize(&stored_assertion) {
        Ok(ser) => ser,
        Err(err) => {
            error!(target: "json_rpc", request_id = %ctx.request_id, client_ip = %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, error = ?err, "Failed to serialize assertion for database storage");
            return Ok(rpc_error_with_request_id(
                ctx.json_rpc,
                -32603,
                "Failed to deserialize assertion internally.",
                ctx.request_id,
            ));
        }
    };

    let req = DbRequest {
        request: DbOperation::Insert(id.to_vec(), ser_assertion),
        response: tx,
    };

    ctx.db.send(req)?;

    let result = DaSubmissionResponse {
        id,
        prover_signature: stored_assertion.prover_signature.as_bytes().into(),
    };

    match rx.await {
        Ok(_) => {
            debug!(target: "json_rpc", request_id = %ctx.request_id, client_ip = %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, ?id, "Successfully stored assertion in database");
            Ok(rpc_response(ctx.json_rpc, result))
        }
        Err(err) => {
            error!(target: "json_rpc", request_id = %ctx.request_id, client_ip = %ctx.client_ip, json_rpc_id = %ctx.json_rpc_id, error = ?err, "Database operation failed for assertion storage");
            Ok(rpc_error_with_request_id(
                ctx.json_rpc,
                -32603,
                "Failed to write to database. Please try again later.",
                ctx.request_id,
            ))
        }
    }
}

fn sanitize_single_hex_prefix(input: &str) -> Result<&str> {
    if let Some(stripped) = input.strip_prefix("0x") {
        if stripped.starts_with("0x") {
            Err(anyhow::anyhow!("Multiple 0x prefixes found"))
        } else {
            Ok(stripped)
        }
    } else {
        Ok(input)
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

#[cfg(test)]
mod tests {
    use super::sanitize_single_hex_prefix;

    #[test]
    fn allows_single_prefix() {
        assert_eq!(sanitize_single_hex_prefix("0xabc123").unwrap(), "abc123");
    }

    #[test]
    fn rejects_multiple_prefixes() {
        assert!(sanitize_single_hex_prefix("0x0xabc123").is_err());
        assert!(sanitize_single_hex_prefix("0x0x0x").is_err());
    }

    #[test]
    fn allows_without_prefix() {
        assert_eq!(sanitize_single_hex_prefix("abc123").unwrap(), "abc123");
    }
}
