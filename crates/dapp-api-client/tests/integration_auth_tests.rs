//! Integration tests for authentication behavior using mock API
//!
//! These tests use httpmock to simulate API responses and test client behavior.
//! They test the same scenarios as the real API but without external dependencies.

use dapp_api_client::{
    AuthConfig,
    Client,
    Config,
};
use httpmock::prelude::*;
use httpmock::{
    Mock,
    Then,
    When,
};
use serde_json::json;

/// Helper function to create a mock with cleaner syntax
fn setup_mock<F>(server: &MockServer, configure: F) -> Mock<'_>
where
    F: FnOnce(When, Then),
{
    server.mock(configure)
}

#[tokio::test]
async fn test_public_endpoint_without_auth_real_api() {
    let server = MockServer::start();

    // Mock the health endpoint (should be public)
    let mock = setup_mock(&server, |when, then| {
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

    // Call public endpoint without auth
    let api = client.inner();
    let result = api.get_health().await;

    // Health endpoint should work without auth
    assert!(
        result.is_ok(),
        "Health endpoint should work without auth against real API"
    );

    mock.assert();
}

#[tokio::test]
async fn test_private_endpoint_without_auth_real_api() {
    let server = MockServer::start();

    // Mock the private endpoint returning empty array for unauthenticated requests
    // (mimics real API behavior)
    let mock = setup_mock(&server, |when, then| {
        when.method(GET).path("/api/v1/projects/saved").query_param(
            "wallet_address",
            "0x70997970c51812dc3a010c7d01b50e0d17dc79c8",
        );
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([])); // Empty array for unauthenticated requests
    });

    // Create unauthenticated client
    let config = Config::new(server.url("/api/v1"));
    let client = Client::new(config).expect("Failed to create client");

    // Try to call a private endpoint without auth
    let api = client.inner();
    let result = api
        .get_projects_saved("0x70997970c51812dc3a010c7d01b50e0d17dc79c8")
        .await;

    // The API returns an empty array for unauthenticated requests
    // rather than a 401/403 error - this is valid API design
    assert!(
        result.is_ok(),
        "GET /projects/saved should succeed without auth"
    );

    let response = result.unwrap().into_inner();
    assert_eq!(
        response.len(),
        0,
        "Should return empty array for unauthenticated request"
    );

    mock.assert();
}

#[tokio::test]
async fn test_private_endpoint_with_auth_real_api() {
    let server = MockServer::start();

    // Mock the private endpoint with valid auth returning data
    let mock = setup_mock(&server, |when, then| {
        when.method(GET)
            .path("/api/v1/projects/saved")
            .query_param(
                "wallet_address",
                "0x70997970c51812dc3a010c7d01b50e0d17dc79c8",
            )
            .header("authorization", "Bearer test-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!([
                {
                    "project_id": "c1e794ce-4030-487c-a4e6-917caeeb4875",
                    "project_name": "Saved Project",
                    "project_networks": ["1"],
                    "project_manager": "0x70997970c51812dc3a010c7d01b50e0d17dc79c8",
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
        .get_projects_saved("0x70997970c51812dc3a010c7d01b50e0d17dc79c8")
        .await;

    // Should succeed with valid auth
    assert!(
        result.is_ok(),
        "Private endpoint should work with valid auth"
    );

    let response = result.unwrap().into_inner();
    assert_eq!(response.len(), 1);
    assert_eq!(response[0].project_name.as_str(), "Saved Project");

    mock.assert();
}

#[tokio::test]
async fn test_public_endpoint_with_auth_real_api() {
    let server = MockServer::start();

    // Mock the health endpoint accepting auth (but not requiring it)
    let mock = setup_mock(&server, |when, then| {
        when.method(GET)
            .path("/api/v1/health")
            .header("authorization", "Bearer dummy-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(json!({
                "status": "ok",
                "timestamp": "2025-01-01T00:00:00Z",
                "environment": "test"
            }));
    });

    // Create authenticated client
    let config = Config::new(server.url("/api/v1"));
    let auth =
        AuthConfig::bearer_token("dummy-token".to_string()).expect("Failed to create auth config");
    let client = Client::new_with_auth(config, auth).expect("Failed to create client");

    // Call public endpoint with auth (even invalid auth should work for public endpoints)
    let api = client.inner();
    let result = api.get_health().await;

    // Should succeed even with invalid auth for public endpoints
    assert!(
        result.is_ok(),
        "Public endpoint should work even with invalid auth token"
    );

    mock.assert();
}
