use assertion_executor::{
    ExecutorConfig,
    primitives::{
        Bytes,
        hex,
    },
};
use axum::{
    Json,
    Router,
    body::Bytes as RequestBytes,
    extract::State,
    routing::{
        get,
        post,
    },
};
use serde::Deserialize;
use serde_json::{
    Value,
    json,
};

use crate::verify_assertion;

const PARSE_ERROR_CODE: i64 = -32700;
const INVALID_REQUEST_CODE: i64 = -32600;
const METHOD_NOT_FOUND_CODE: i64 = -32601;
const INVALID_PARAMS_CODE: i64 = -32602;
const INTERNAL_ERROR_CODE: i64 = -32603;

#[derive(Clone)]
struct AppState {
    executor_config: ExecutorConfig,
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Value,
    id: Value,
}

#[derive(Debug, Deserialize)]
struct VerifyAssertionParams {
    bytecode: String,
}

pub fn build_router(executor_config: ExecutorConfig) -> Router {
    Router::new()
        .route("/", post(handle_rpc))
        .route("/health", get(health))
        .with_state(AppState { executor_config })
}

pub async fn health() -> &'static str {
    "ok"
}

async fn handle_rpc(State(state): State<AppState>, body: RequestBytes) -> Json<Value> {
    Json(process_json_rpc(&body, &state.executor_config))
}

#[must_use]
pub fn process_json_rpc(payload: &[u8], executor_config: &ExecutorConfig) -> Value {
    let json_value: Value = match serde_json::from_slice(payload) {
        Ok(value) => value,
        Err(_) => {
            return rpc_error(&Value::Null, PARSE_ERROR_CODE, "Parse error");
        }
    };

    if !json_value.is_object() {
        return rpc_error(
            &Value::Null,
            INVALID_REQUEST_CODE,
            "Invalid JSON-RPC request",
        );
    }

    let request: JsonRpcRequest = match serde_json::from_value(json_value.clone()) {
        Ok(request) => request,
        Err(_) => {
            return rpc_error(
                &Value::Null,
                INVALID_REQUEST_CODE,
                "Invalid JSON-RPC request",
            );
        }
    };

    if request.jsonrpc != "2.0" || !is_valid_jsonrpc_id(&request.id) {
        return rpc_error(
            &Value::Null,
            INVALID_REQUEST_CODE,
            "Invalid JSON-RPC request",
        );
    }

    match request.method.as_str() {
        "verify_assertion" => handle_verify_assertion(request, executor_config),
        _ => rpc_error(&request.id, METHOD_NOT_FOUND_CODE, "Method not found"),
    }
}

fn handle_verify_assertion(request: JsonRpcRequest, executor_config: &ExecutorConfig) -> Value {
    let params: VerifyAssertionParams = match serde_json::from_value(request.params) {
        Ok(params) => params,
        Err(_) => {
            return rpc_error(&request.id, INVALID_PARAMS_CODE, "Invalid params");
        }
    };

    let bytecode = match decode_bytecode(&params.bytecode) {
        Ok(bytecode) => bytecode,
        Err(err) => {
            return rpc_error(&request.id, INVALID_PARAMS_CODE, err);
        }
    };

    let result = verify_assertion(&bytecode, executor_config);
    match serde_json::to_value(result) {
        Ok(serialized) => rpc_result(&request.id, &serialized),
        Err(err) => {
            rpc_error(
                &request.id,
                INTERNAL_ERROR_CODE,
                format!("Internal error: {err}"),
            )
        }
    }
}

fn decode_bytecode(bytecode: &str) -> Result<Bytes, String> {
    let stripped = bytecode.strip_prefix("0x").unwrap_or(bytecode);
    hex::decode(stripped)
        .map(Into::into)
        .map_err(|err| format!("Invalid bytecode hex: {err}"))
}

fn is_valid_jsonrpc_id(id: &Value) -> bool {
    matches!(id, Value::String(_) | Value::Number(_) | Value::Null)
}

fn rpc_result(id: &Value, result: &Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "result": result,
        "id": id
    })
}

fn rpc_error(id: &Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "error": {
            "code": code,
            "message": message.into()
        },
        "id": id
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::path::Path;

    fn artifact_bytecode_hex() -> String {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let artifact_path = root.join(
            "../../testdata/mock-protocol/out/SimpleCounterAssertion.sol/SimpleCounterAssertion.json",
        );

        let file = std::fs::File::open(&artifact_path)
            .unwrap_or_else(|err| panic!("failed to open {}: {err}", artifact_path.display()));
        let json: Value = serde_json::from_reader(file)
            .unwrap_or_else(|err| panic!("failed to parse {}: {err}", artifact_path.display()));

        format!(
            "0x{}",
            json["bytecode"]["object"]
                .as_str()
                .expect("artifact bytecode should be a string")
        )
    }

    #[test]
    fn handles_verify_assertion_success() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "verify_assertion",
            "params": {
                "bytecode": artifact_bytecode_hex(),
            },
            "id": 1
        });

        let response = process_json_rpc(request.to_string().as_bytes(), &ExecutorConfig::default());
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["status"], "success");
        assert!(response["result"]["triggers"].is_object());
        assert!(
            response["result"]["triggers"]
                .as_object()
                .is_some_and(|triggers| !triggers.is_empty())
        );
        assert!(response.get("error").is_none());
    }

    #[test]
    fn handles_verify_assertion_deployment_failure() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "verify_assertion",
            "params": {
                "bytecode": "0x60006000fd",
            },
            "id": 1
        });

        let response = process_json_rpc(request.to_string().as_bytes(), &ExecutorConfig::default());
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["status"], "deployment_failure");
        assert!(response["result"].get("triggers").is_none());
        assert!(response["result"]["error"].is_string());
    }

    #[test]
    fn handles_verify_assertion_no_triggers() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "verify_assertion",
            "params": {
                "bytecode": "0x6001600c60003960016000f300",
            },
            "id": 1
        });

        let response = process_json_rpc(request.to_string().as_bytes(), &ExecutorConfig::default());
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["status"], "no_triggers");
        assert!(response["result"].get("triggers").is_none());
        assert!(response["result"].get("error").is_none());
    }

    #[test]
    fn rejects_invalid_params() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "verify_assertion",
            "params": ["not-an-object"],
            "id": 1
        });

        let response = process_json_rpc(request.to_string().as_bytes(), &ExecutorConfig::default());
        assert_eq!(response["error"]["code"], INVALID_PARAMS_CODE);
    }

    #[test]
    fn rejects_unknown_method() {
        let request = json!({
            "jsonrpc": "2.0",
            "method": "unknown_method",
            "params": {},
            "id": 1
        });

        let response = process_json_rpc(request.to_string().as_bytes(), &ExecutorConfig::default());
        assert_eq!(response["error"]["code"], METHOD_NOT_FOUND_CODE);
    }

    #[test]
    fn rejects_malformed_json() {
        let response = process_json_rpc(b"{invalid json", &ExecutorConfig::default());
        assert_eq!(response["error"]["code"], PARSE_ERROR_CODE);
    }

    #[tokio::test]
    async fn health_returns_ok() {
        assert_eq!(health().await, "ok");
    }
}
