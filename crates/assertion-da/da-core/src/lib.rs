use alloy::primitives::{
    B256,
    Bytes,
};
use serde::{
    Deserialize,
    Serialize,
};

///The submission payload for the DA layer.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DaSubmission {
    pub solidity_source: String,
    pub compiler_version: String,
    pub assertion_contract_name: String,
    pub constructor_args: Vec<String>,
    pub constructor_abi_signature: String,
}

///The response from the DA layer when submitting an assertion
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DaSubmissionResponse {
    pub id: B256,
    pub prover_signature: Bytes,
}

///The response from the DA layer when fetching an assertion
#[derive(Debug, Deserialize, Serialize)]
pub struct DaFetchResponse {
    pub solidity_source: String,
    pub bytecode: Bytes,
    pub prover_signature: Bytes,
    pub encoded_constructor_args: Bytes,
    pub constructor_abi_signature: String,
}
