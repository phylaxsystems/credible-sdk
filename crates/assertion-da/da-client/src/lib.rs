use alloy::primitives::B256;
use http::header;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use url::Url;

pub use assertion_da_core::{DaFetchResponse, DaSubmission, DaSubmissionResponse};

/// A client for interacting with the DA layer
/// This client is responsible for fetching bytecode from the DA layer
///
/// ``` no_run
/// use assertion_da_client::DaClient;
/// use alloy::primitives::fixed_bytes;
///
/// #[tokio::main]
/// async fn main() {
///
///     let da_client = DaClient::new("http://localhost:3030").unwrap();
///     let assertion_id = fixed_bytes!("044852b2a670ade5407e78fb2863c51de9fcb96542a07186fe3aeda6bb8a116d");
///     let assertion = da_client.fetch_assertion(assertion_id).await.unwrap();
/// }         
#[derive(Debug)]
pub struct DaClient {
    client: Client,
    base_url: Url,
    request_id: std::sync::atomic::AtomicU64,
}

#[derive(Debug, thiserror::Error)]
pub enum DaClientError {
    #[error("HTTP client error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("URL parse error: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("JSON-RPC error code {code}: {message}")]
    JsonRpcError { code: i32, message: String },
    #[error("Invalid response: {0}")]
    InvalidResponse(String),
}

/// JSON-RPC request structure
#[derive(Debug, Serialize)]
struct JsonRpcRequest<T> {
    jsonrpc: String,
    method: String,
    params: T,
    id: u64,
}

/// JSON-RPC response structure for successful responses
#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    jsonrpc: String,
    result: Option<T>,
    error: Option<JsonRpcError>,
    id: u64,
}

/// JSON-RPC error structure
#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

impl DaClient {
    /// Create a new DA client
    pub fn new(da_url: &str) -> Result<Self, DaClientError> {
        let base_url = Url::parse(da_url)?;
        let client = Client::builder().use_rustls_tls().build()?;

        Ok(Self {
            client,
            base_url,
            request_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Create a new DA client with authentication
    pub fn new_with_auth(da_url: &str, auth: &str) -> Result<Self, DaClientError> {
        let base_url = Url::parse(da_url)?;
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            auth.parse().map_err(|_| {
                DaClientError::InvalidResponse("Invalid authorization header".to_string())
            })?,
        );

        let client = Client::builder()
            .use_rustls_tls()
            .default_headers(headers)
            .build()?;

        Ok(Self {
            client,
            base_url,
            request_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Get next request ID
    fn next_request_id(&self) -> u64 {
        self.request_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Make a JSON-RPC request
    async fn make_request<P, R>(&self, method: &str, params: P) -> Result<R, DaClientError>
    where
        P: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let request_id = self.next_request_id();
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
            id: request_id,
        };

        let response = self
            .client
            .post(self.base_url.clone())
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(DaClientError::InvalidResponse(format!(
                "HTTP error: {}",
                response.status()
            )));
        }

        let response_body: JsonRpcResponse<R> = response.json().await?;

        // Validate JSON-RPC 2.0 compliance
        if response_body.jsonrpc != "2.0" {
            return Err(DaClientError::InvalidResponse(format!(
                "Invalid JSON-RPC version: expected '2.0', got '{}'",
                response_body.jsonrpc
            )));
        }

        if response_body.id != request_id {
            return Err(DaClientError::InvalidResponse(format!(
                "Request/response ID mismatch: expected {}, got {}",
                request_id, response_body.id
            )));
        }

        if let Some(error) = response_body.error {
            return Err(DaClientError::JsonRpcError {
                code: error.code,
                message: error.message,
            });
        }

        response_body.result.ok_or_else(|| {
            DaClientError::InvalidResponse("Missing result in successful response".to_string())
        })
    }

    /// Fetch the bytecode and signature for the given assertion id from the DA layer
    pub async fn fetch_assertion(
        &self,
        assertion_id: B256,
    ) -> Result<DaFetchResponse, DaClientError> {
        let params = vec![assertion_id.to_string()];
        self.make_request("da_get_assertion", params).await
    }

    /// Submit the assertion bytecode to the DA layer
    pub async fn submit_assertion(
        &self,
        assertion_contract_name: String,
        solidity_source: String,
        compiler_version: String,
    ) -> Result<DaSubmissionResponse, DaClientError> {
        self.submit_assertion_with_args(
            assertion_contract_name,
            solidity_source,
            compiler_version,
            "constructor()".to_string(),
            vec![],
        )
        .await
    }

    /// Submit the assertion bytecode with constructor args to the DA layer
    pub async fn submit_assertion_with_args(
        &self,
        assertion_contract_name: String,
        solidity_source: String,
        compiler_version: String,
        constructor_abi_signature: String,
        constructor_args: Vec<String>,
    ) -> Result<DaSubmissionResponse, DaClientError> {
        let params = vec![DaSubmission {
            solidity_source,
            compiler_version,
            assertion_contract_name,
            constructor_abi_signature,
            constructor_args,
        }];

        self.make_request("da_submit_solidity_assertion", params)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::{
        primitives::{Bytes, hex, keccak256},
        signers::{Signer, local::PrivateKeySigner},
    };
    use assertion_da_server::api::{
        db::listen_for_db,
        process_request::StoredAssertion,
        serve,
        types::{DbOperation, DbRequest, DbRequestSender},
    };
    use bollard::Docker;
    use serde_json::json;
    use sled::Config as DbConfig;
    use tempfile::TempDir;
    use tokio::{
        net::TcpListener,
        sync::{mpsc, oneshot},
    };
    use tokio_util::sync::CancellationToken;

    use super::*;

    async fn setup_test_env() -> (TempDir, DbRequestSender, PrivateKeySigner, DaClient) {
        // Create a temporary directory for the database
        let temp_dir = TempDir::new().unwrap();
        let (db_sender, db_receiver) = mpsc::unbounded_channel();
        let db = DbConfig::new().path(&temp_dir).open().unwrap();
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
                docker.clone(),
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

        // Create the client
        let client = DaClient::new(&server_url).unwrap();

        (temp_dir, db_sender, signer, client)
    }

    #[tokio::test]
    async fn test_client_submit_solidity_assertion() {
        let (_temp_dir, _db_sender, _signer, client) = setup_test_env().await;

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

        let response = client
            .submit_assertion(
                "SimpleStorage".to_string(),
                source_code.to_string(),
                "0.8.17".to_string(),
            )
            .await
            .unwrap();

        assert!(!response.id.is_empty());
        assert!(!response.prover_signature.is_empty());
    }

    #[tokio::test]
    async fn test_client_get_assertion() {
        let (_temp_dir, db_sender, signer, client) = setup_test_env().await;

        // First submit an assertion directly to DB
        let source_code = "contract Test { }";
        let stored_assertion = StoredAssertion {
            solidity_source: source_code.to_string(),
            bytecode: vec![1, 2, 3, 4],
            prover_signature: signer.sign_hash(&keccak256([1, 2, 3, 4])).await.unwrap(),
            assertion_contract_name: "Test".to_string(),
            compiler_version: "0.8.17".to_string(),
            constructor_abi_signature: "constructor()".to_string(),
            encoded_constructor_args: Bytes::new(),
        };

        let id = keccak256(&stored_assertion.bytecode);

        // Store in database
        let (tx, _) = oneshot::channel();
        let ser_assertion = bincode::serialize(&stored_assertion).unwrap();
        let _ = db_sender.send(DbRequest {
            request: DbOperation::Insert(id.to_vec(), ser_assertion),
            response: tx,
        });

        // Now fetch using client
        let response = client.fetch_assertion(id).await.unwrap();

        assert_eq!(response.solidity_source, source_code);
        assert_eq!(response.bytecode.to_vec(), vec![1, 2, 3, 4]);
        assert_eq!(
            response.prover_signature.to_vec(),
            stored_assertion.prover_signature.as_bytes()
        );
    }

    #[tokio::test]
    async fn test_get_nonexistent_assertion() {
        let (_temp_dir, _db_sender, _signer, client) = setup_test_env().await;

        let nonexistent_id = B256::ZERO;
        let result = client.fetch_assertion(nonexistent_id).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Assertion not found")
        );
    }

    #[tokio::test]
    async fn test_client_with_auth() {
        // Test that the auth header is set correctly
        let auth_token = "Bearer test_token";

        // Use wiremock to verify the header is set correctly
        let mock_server = wiremock::MockServer::start().await;

        // Create a client with auth token pointing to our mock server
        let client = DaClient::new_with_auth(&mock_server.uri(), auth_token).unwrap();

        // Set up a mock to expect the Authorization header
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::header("Authorization", auth_token))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "id": "0x1234567890abcdef",
                    "prover_signature": "0xabcdef1234567890"
                }
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Make a request that will trigger the header to be sent
        let _ = client.fetch_assertion(B256::ZERO).await;

        // Verify that the mock was called with the expected header
        mock_server.verify().await;
    }

    #[tokio::test]
    async fn test_client_submit_solidity_assertion_with_args() {
        let (_temp_dir, _db_sender, _signer, client) = setup_test_env().await;

        let source_code = r#"
            // SPDX-License-Identifier: MIT
            pragma solidity ^0.8.0;
            
            contract SimpleStorage {
                uint256 private value;
                
                constructor(uint256 initialValue) {
                    value = initialValue;
                }
                
                function get() public view returns (uint256) {
                    return value;
                }
            }
        "#;

        let response = client
            .submit_assertion_with_args(
                "SimpleStorage".to_string(),
                source_code.to_string(),
                "0.8.17".to_string(),
                "constructor(uint256)".to_string(),
                vec!["42".to_string()],
            )
            .await
            .unwrap();

        assert!(!response.id.is_empty());
        assert!(!response.prover_signature.is_empty());
    }

    #[tokio::test]
    async fn test_invalid_solidity_submission() {
        let (_temp_dir, _db_sender, _signer, client) = setup_test_env().await;

        // Invalid Solidity code (missing semicolon)
        let invalid_source = "contract Test { uint256 x = 5 }";

        let result = client
            .submit_assertion(
                "Test".to_string(),
                invalid_source.to_string(),
                "0.8.17".to_string(),
            )
            .await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("Compilation Error")
                || error.to_string().contains("Solidity Error")
        );
    }

    #[tokio::test]
    async fn test_failed_authentication() {
        // Use wiremock to test failed authentication
        let mock_server = wiremock::MockServer::start().await;

        // Create a client with auth token
        let client = DaClient::new_with_auth(&mock_server.uri(), "Bearer invalid_token").unwrap();

        // Set up mock to return 401 Unauthorized
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(wiremock::ResponseTemplate::new(401))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Attempt to fetch an assertion
        let result = client.fetch_assertion(B256::ZERO).await;

        assert!(result.is_err());
        assert!(
            result.as_ref().unwrap_err().to_string().contains("401")
                || result.unwrap_err().to_string().contains("Unauthorized")
        );
    }

    #[tokio::test]
    async fn test_response_content_validation() {
        let (_temp_dir, db_sender, signer, client) = setup_test_env().await;

        // Create a test contract with specific bytecode
        let source_code = "contract Test { uint256 value; }";
        let bytecode = vec![0xDE, 0xAD, 0xBE, 0xEF]; // Example bytecode
        let id = keccak256(&bytecode);
        let signature = signer.sign_hash(&id).await.unwrap();

        // Create the stored assertion with known values
        let stored_assertion = StoredAssertion {
            solidity_source: source_code.to_string(),
            bytecode: bytecode.clone(),
            prover_signature: signature,
            assertion_contract_name: "Test".to_string(),
            compiler_version: "0.8.17".to_string(),
            constructor_abi_signature: "constructor()".to_string(),
            encoded_constructor_args: Bytes::new(),
        };

        // Insert directly into DB
        let (tx, _) = oneshot::channel();
        let _ = db_sender.send(DbRequest {
            request: DbOperation::Insert(
                id.to_vec(),
                bincode::serialize(&stored_assertion).unwrap(),
            ),
            response: tx,
        });

        // Fetch and verify response in detail
        let response = client.fetch_assertion(id).await.unwrap();

        // Detailed validation
        assert_eq!(response.solidity_source, source_code);
        assert_eq!(response.bytecode.to_vec(), bytecode);
        assert_eq!(
            hex::encode(&response.prover_signature),
            hex::encode(signature.as_bytes())
        );
        assert_eq!(response.constructor_abi_signature, "constructor()");
        assert!(response.encoded_constructor_args.is_empty());
    }

    #[tokio::test]
    async fn test_json_rpc_validation() {
        use serde_json::json;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // Test invalid JSON-RPC version
        {
            let mock_server = MockServer::start().await;
            let client = DaClient::new(&mock_server.uri()).unwrap();

            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "1.0", // Invalid version
                    "result": {
                        "solidity_source": "contract Test {}",
                        "bytecode": "0x608060405234801561001057600080fd5b50600080fd5b00",
                        "prover_signature": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef01",
                        "encoded_constructor_args": "0x",
                        "constructor_abi_signature": "constructor()"
                    },
                    "id": 1
                })))
                .expect(1)
                .mount(&mock_server)
                .await;

            let result = client.fetch_assertion(Default::default()).await;
            assert!(result.is_err());
            match result.unwrap_err() {
                DaClientError::InvalidResponse(msg) => {
                    assert!(msg.contains("Invalid JSON-RPC version"));
                }
                other => panic!("Expected InvalidResponse error, got: {other:?}"),
            }
        }

        // Test mismatched ID
        {
            let mock_server = MockServer::start().await;
            let client = DaClient::new(&mock_server.uri()).unwrap();

            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "jsonrpc": "2.0",
                    "result": {
                        "solidity_source": "contract Test {}",
                        "bytecode": "0x608060405234801561001057600080fd5b50600080fd5b00",
                        "prover_signature": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef01",
                        "encoded_constructor_args": "0x",
                        "constructor_abi_signature": "constructor()"
                    },
                    "id": 999 // Will not match the sent ID
                })))
                .expect(1)
                .mount(&mock_server)
                .await;

            let result = client.fetch_assertion(Default::default()).await;
            assert!(result.is_err());
            match result.unwrap_err() {
                DaClientError::InvalidResponse(msg) => {
                    assert!(msg.contains("Request/response ID mismatch"));
                }
                other => panic!("Expected InvalidResponse error, got: {other:?}"),
            }
        }
    }
}
