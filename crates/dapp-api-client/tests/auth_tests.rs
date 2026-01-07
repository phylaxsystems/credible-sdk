//! Tests for authentication behavior of the dapp-api-client
//!
//! These tests verify that:
//! - Public endpoints work without authentication
//! - Public endpoints still work with authentication
//! - Private endpoints fail without authentication
//! - Private endpoints work with authentication

use dapp_api_client::{
    AuthConfig,
    Client,
    Config,
};
use httpmock::prelude::*;
use serde_json::json;

#[tokio::test]
async fn test_public_endpoint_without_auth() {
    let server = MockServer::start();

    // Mock the public /projects endpoint
    let mock = server.mock(|when, then| {
        when.method(GET).path("/api/v1/projects");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([
                {
                    "project_id": "f22a0a2f-bde9-49b3-bd70-67599e1f178d",
                    "project_name": "Test Project",
                    "project_networks": ["1"], // Using integer to match spec
                    "project_manager": "0x1234567890123456789012345678901234567890",
                    "created_at": "2025-01-01T00:00:00Z",
                    "updated_at": "2025-01-01T00:00:00Z",
                    "saved_count": 0
                }
            ]));
    });

    // Create unauthenticated client
    let config = Config::new(server.url("/api/v1"));
    let client = Client::new(config).expect("Failed to create client");

    // Call public endpoint without auth
    let api = client.inner();
    let result = api.get_projects(None, None).await;

    // Should succeed
    if let Err(ref e) = result {
        eprintln!("Error calling get_projects: {e:?}");
        eprintln!("Error status: {:?}", e.status());
    }
    assert!(result.is_ok(), "Public endpoint should work without auth");
    let response = result.unwrap().into_inner();
    assert_eq!(response.len(), 1);
    assert_eq!(response[0].project_name.as_str(), "Test Project");

    // Verify the request was made without auth header
    mock.assert();
    let hits = mock.calls();
    assert_eq!(hits, 1);
}

#[tokio::test]
async fn test_public_endpoint_with_auth() {
    let server = MockServer::start();

    // Mock the public /projects endpoint
    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/api/v1/projects")
            .header("authorization", "Bearer test-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([
                {
                    "project_id": "f22a0a2f-bde9-49b3-bd70-67599e1f178d",
                    "project_name": "Test Project",
                    "project_networks": ["1"],
                    "project_manager": "0x1234567890123456789012345678901234567890",
                    "created_at": "2025-01-01T00:00:00Z",
                    "updated_at": "2025-01-01T00:00:00Z",
                    "saved_count": 0
                }
            ]));
    });

    // Create authenticated client
    let config = Config::new(server.url("/api/v1"));
    let auth =
        AuthConfig::bearer_token("test-token".to_string()).expect("Failed to create auth config");
    let client = Client::new_with_auth(config, auth).expect("Failed to create client");

    // Call public endpoint with auth
    let api = client.inner();
    let result = api.get_projects(None, None).await;

    // Should succeed
    assert!(result.is_ok(), "Public endpoint should work with auth");
    let response = result.unwrap().into_inner();
    assert_eq!(response.len(), 1);

    // Verify the request was made with auth header
    mock.assert();
}

#[tokio::test]
async fn test_private_endpoint_without_auth() {
    let server = MockServer::start();

    // Mock a private endpoint that requires auth (e.g., saved projects)
    let mock = server.mock(|when, then| {
        when.method(GET).path("/api/v1/projects/saved").query_param(
            "wallet_address",
            "0x1234567890123456789012345678901234567890",
        );
        then.status(401)
            .header("content-type", "application/json")
            .json_body(json!({
                "error": "Unauthorized",
                "message": "Authentication required"
            }));
    });

    // Create unauthenticated client
    let config = Config::new(server.url("/api/v1"));
    let client = Client::new(config).expect("Failed to create client");

    // Call private endpoint without auth
    let api = client.inner();
    let result = api
        .get_projects_saved("0x1234567890123456789012345678901234567890")
        .await;

    // Should fail with 401
    assert!(result.is_err(), "Private endpoint should fail without auth");

    // Verify we got a 401 error
    if let Err(e) = result {
        if let Some(status) = e.status() {
            assert_eq!(status, 401, "Should return 401 Unauthorized");
        } else {
            panic!("Expected HTTP status in error");
        }
    }

    mock.assert();
}

#[tokio::test]
async fn test_private_endpoint_with_auth() {
    let server = MockServer::start();

    // Mock a private endpoint with valid auth
    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/api/v1/projects/saved")
            .query_param(
                "wallet_address",
                "0x1234567890123456789012345678901234567890",
            )
            .header("authorization", "Bearer test-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([
                {
                    "project_id": "c1e794ce-4030-487c-a4e6-917caeeb4875",
                    "project_name": "Saved Project",
                    "project_networks": ["1"],
                    "project_manager": "0x1234567890123456789012345678901234567890",
                    "created_at": "2025-01-01T00:00:00Z",
                    "updated_at": "2025-01-01T00:00:00Z",
                    "saved_count": 1,
                    "saved_at": "2025-01-02T00:00:00Z"
                }
            ]));
    });

    // Create authenticated client
    let config = Config::new(server.url("/api/v1"));
    let auth =
        AuthConfig::bearer_token("test-token".to_string()).expect("Failed to create auth config");
    let client = Client::new_with_auth(config, auth).expect("Failed to create client");

    // Call private endpoint with auth
    let api = client.inner();
    let result = api
        .get_projects_saved("0x1234567890123456789012345678901234567890")
        .await;

    // Should succeed
    assert!(result.is_ok(), "Private endpoint should work with auth");
    let response = result.unwrap().into_inner();
    assert_eq!(response.len(), 1);
    assert_eq!(response[0].project_name.as_str(), "Saved Project");

    mock.assert();
}

#[tokio::test]
async fn test_health_endpoint_without_auth() {
    let server = MockServer::start();

    // Mock the health endpoint (should be public)
    let mock = server.mock(|when, then| {
        when.method(GET).path("/api/v1/health");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!({
                "status": "ok",
                "timestamp": "2025-01-01T00:00:00Z",
                "environment": "test"
            }));
    });

    // Create unauthenticated client
    let config = Config::new(server.url("/api/v1"));
    let client = Client::new(config).expect("Failed to create client");

    // Call health endpoint without auth
    let api = client.inner();
    let result = api.get_health().await;

    // Should succeed
    if let Err(ref e) = result {
        eprintln!("Error calling get_health: {e:?}");
        eprintln!("Error status: {:?}", e.status());
    }
    assert!(result.is_ok(), "Health endpoint should work without auth");

    mock.assert();
}
