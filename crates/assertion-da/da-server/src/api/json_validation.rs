use serde::{
    Deserialize,
    Serialize,
};
use serde_json::Value;
use std::fmt;

/// Maximum allowed JSON payload size (10MB)
pub const MAX_JSON_SIZE: usize = 10 * 1024 * 1024;

/// Maximum allowed nesting depth for JSON structures
pub const MAX_JSON_DEPTH: usize = 32;

/// Standard JSON-RPC error codes
#[derive(Debug, Clone, Copy)]
pub enum JsonRpcErrorCode {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
}

impl fmt::Display for JsonRpcErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as i32)
    }
}

/// Validated JSON-RPC request structure
#[derive(Debug, Deserialize, Serialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<Vec<Value>>,
    pub id: Value,
}

/// Strict validation for da_submit_solidity_assertion params
pub fn validate_da_submission_params(params: &[Value]) -> Result<(), &'static str> {
    if params.len() != 1 {
        return Err("Expected exactly one parameter");
    }
    
    let obj = params[0].as_object().ok_or("Parameter must be an object")?;
    
    // Check for exact fields - no more, no less
    let expected_fields = ["solidity_source", "compiler_version", "assertion_contract_name", 
                          "constructor_args", "constructor_abi_signature"];
    let mut seen_fields = std::collections::HashSet::new();
    
    for (key, value) in obj {
        if !expected_fields.contains(&key.as_str()) {
            return Err("Unexpected field in submission");
        }
        seen_fields.insert(key.as_str());
        
        // Validate field types
        match key.as_str() {
            "solidity_source" | "compiler_version" | "assertion_contract_name" | 
            "constructor_abi_signature" => {
                if !value.is_string() {
                    return Err("Field must be a string");
                }
            }
            "constructor_args" => {
                let arr = value.as_array().ok_or("constructor_args must be an array")?;
                // Ensure all elements are strings with no nesting
                for arg in arr {
                    if !arg.is_string() {
                        return Err("constructor_args must contain only strings");
                    }
                }
            }
            _ => {}
        }
    }
    
    // Ensure all required fields are present
    for field in expected_fields {
        if !seen_fields.contains(field) {
            return Err("Missing required field");
        }
    }
    
    Ok(())
}

/// Validate params for methods expecting a single hex string
pub fn validate_hex_param(params: &[Value]) -> Result<(), &'static str> {
    if params.len() != 1 {
        return Err("Expected exactly one parameter");
    }
    
    if !params[0].is_string() {
        return Err("Parameter must be a string");
    }
    
    Ok(())
}

/// Safe parameter access result
pub enum ParamAccess<'a> {
    Value(&'a Value),
    Missing,
    InvalidType,
}

impl JsonRpcRequest {
    /// Validates and parses a JSON-RPC request from raw JSON value
    pub fn validate(json: Value) -> Result<Self, (JsonRpcErrorCode, &'static str)> {
        // Check if it's an object
        let obj = json.as_object().ok_or((
            JsonRpcErrorCode::InvalidRequest,
            "Request must be a JSON object",
        ))?;

        // Validate JSON-RPC version
        let jsonrpc = obj.get("jsonrpc").and_then(|v| v.as_str()).ok_or((
            JsonRpcErrorCode::InvalidRequest,
            "Missing or invalid 'jsonrpc' field",
        ))?;

        if jsonrpc != "2.0" {
            return Err((
                JsonRpcErrorCode::InvalidRequest,
                "JSON-RPC version must be 2.0",
            ));
        }

        // Validate method
        let method = obj.get("method").and_then(|v| v.as_str()).ok_or((
            JsonRpcErrorCode::InvalidRequest,
            "Missing or invalid 'method' field",
        ))?;

        // Validate id (can be number, string, or null)
        let id = obj.get("id").cloned().unwrap_or(Value::Null);

        // Validate params if present (must be array)
        let params = if let Some(params_value) = obj.get("params") {
            match params_value {
                Value::Array(arr) => Some(arr.clone()),
                Value::Null => None,
                _ => {
                    return Err((
                        JsonRpcErrorCode::InvalidParams,
                        "Params must be an array or null",
                    ));
                }
            }
        } else {
            None
        };

        Ok(JsonRpcRequest {
            jsonrpc: jsonrpc.to_string(),
            method: method.to_string(),
            params,
            id,
        })
    }

    /// Safely access a parameter by index
    pub fn get_param(&self, index: usize) -> ParamAccess {
        match &self.params {
            Some(params) => {
                params
                    .get(index)
                    .map(ParamAccess::Value)
                    .unwrap_or(ParamAccess::Missing)
            }
            None => ParamAccess::Missing,
        }
    }

    /// Safely get a string parameter
    pub fn get_string_param(&self, index: usize) -> Result<&str, &'static str> {
        match self.get_param(index) {
            ParamAccess::Value(v) => v.as_str().ok_or("Parameter is not a string"),
            ParamAccess::Missing => Err("Missing parameter"),
            ParamAccess::InvalidType => Err("Invalid parameter type"),
        }
    }

    /// Safely deserialize a parameter into a specific type
    pub fn deserialize_param<T: for<'de> Deserialize<'de>>(
        &self,
        index: usize,
    ) -> Result<T, &'static str> {
        match self.get_param(index) {
            ParamAccess::Value(v) => {
                serde_json::from_value(v.clone()).map_err(|_| "Failed to deserialize parameter")
            }
            ParamAccess::Missing => Err("Missing parameter"),
            ParamAccess::InvalidType => Err("Invalid parameter type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_json_rpc_request() {
        let json = json!({
            "jsonrpc": "2.0",
            "method": "test_method",
            "params": ["param1", "param2"],
            "id": 1
        });

        let request = JsonRpcRequest::validate(json).unwrap();
        assert_eq!(request.jsonrpc, "2.0");
        assert_eq!(request.method, "test_method");
        assert_eq!(request.params.as_ref().unwrap().len(), 2);
        assert_eq!(request.id, 1);
    }

    #[test]
    fn test_validate_da_submission_params_valid() {
        let params = vec![json!({
            "solidity_source": "contract Test {}",
            "compiler_version": "0.8.17",
            "assertion_contract_name": "Test",
            "constructor_args": ["arg1", "arg2"],
            "constructor_abi_signature": "constructor(string,string)"
        })];
        
        assert!(validate_da_submission_params(&params).is_ok());
    }

    #[test]
    fn test_validate_da_submission_params_extra_field() {
        let params = vec![json!({
            "solidity_source": "contract Test {}",
            "compiler_version": "0.8.17",
            "assertion_contract_name": "Test",
            "constructor_args": [],
            "constructor_abi_signature": "constructor()",
            "extra_field": "not allowed"
        })];
        
        let result = validate_da_submission_params(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Unexpected field in submission");
    }

    #[test]
    fn test_validate_da_submission_params_nested_object() {
        let params = vec![json!({
            "solidity_source": "contract Test {}",
            "compiler_version": "0.8.17",
            "assertion_contract_name": "Test",
            "constructor_args": [{"nested": "not allowed"}],
            "constructor_abi_signature": "constructor()"
        })];
        
        let result = validate_da_submission_params(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "constructor_args must contain only strings");
    }

    #[test]
    fn test_validate_da_submission_params_missing_field() {
        let params = vec![json!({
            "solidity_source": "contract Test {}",
            "compiler_version": "0.8.17",
            "assertion_contract_name": "Test",
            "constructor_args": []
            // Missing constructor_abi_signature
        })];
        
        let result = validate_da_submission_params(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Missing required field");
    }

    #[test]
    fn test_validate_hex_param_valid() {
        let params = vec![json!("0xabcd1234")];
        assert!(validate_hex_param(&params).is_ok());
    }

    #[test]
    fn test_validate_hex_param_nested() {
        let params = vec![json!({"hex": "0xabcd1234"})];
        let result = validate_hex_param(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Parameter must be a string");
    }

    #[test]
    fn test_validate_hex_param_array() {
        let params = vec![json!(["0xabcd1234"])];
        let result = validate_hex_param(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Parameter must be a string");
    }

    #[test]
    fn test_validate_hex_param_multiple() {
        let params = vec![json!("0xabcd1234"), json!("0x5678")];
        let result = validate_hex_param(&params);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Expected exactly one parameter");
    }

}
