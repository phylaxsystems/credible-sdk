//! Integration tests for authentication behavior against real API
//!
//! Run these tests with a local development server running:
//! DAPP_ENV=development cargo test --test integration_auth_tests -- --nocapture

use dapp_api_client::{
    AuthConfig,
    Client,
    Config,
    Environment,
};

#[tokio::test]
async fn test_public_endpoint_without_auth_real_api() {
    // Use development environment
    let config = Config::from_environment(Environment::Development);
    let client = Client::new(config).expect("Failed to create client");

    // Call public endpoint without auth
    let api = client.inner();
    let result = api.get_health().await;

    // Health endpoint should work without auth
    assert!(
        result.is_ok(),
        "Health endpoint should work without auth against real API"
    );
}

#[tokio::test]
async fn test_private_endpoint_without_auth_real_api() {
    // Use development environment
    let config = Config::from_environment(Environment::Development);
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
}

#[tokio::test]
#[ignore = "Requires valid auth token in DAPP_API_TOKEN env var"]
async fn test_private_endpoint_with_auth_real_api() {
    // This test requires a valid token in DAPP_API_TOKEN
    let token = std::env::var("DAPP_API_TOKEN").expect("DAPP_API_TOKEN must be set for this test");

    let config = Config::from_environment(Environment::Development);
    let auth = AuthConfig::bearer_token(token).expect("Failed to create auth config");
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
}

#[tokio::test]
async fn test_public_endpoint_with_auth_real_api() {
    // Create a dummy auth token
    let config = Config::from_environment(Environment::Development);
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
}
