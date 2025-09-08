//! Authentication utilities for the dapp API client

use crate::error::{
    Error,
    Result,
};
use serde::{
    Deserialize,
    Serialize,
};

/// Authentication configuration for bearer token handling
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthConfig {
    /// The bearer token
    token: String,
}

impl AuthConfig {
    /// Create a new authentication configuration
    pub fn new(token: String) -> Result<Self> {
        let config = Self { token };
        config.validate()?;
        Ok(config)
    }

    /// Create a new authentication configuration with a bearer token
    ///
    /// This is a convenience method that's equivalent to `AuthConfig::new(token)`
    pub fn bearer_token(token: String) -> Result<Self> {
        Self::new(token)
    }

    /// Get the bearer token
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Validate the authentication configuration
    fn validate(&self) -> Result<()> {
        if self.token.trim().is_empty() {
            return Err(Error::AuthError("Bearer token cannot be empty".to_string()));
        }
        Ok(())
    }

    /// Format the token as an Authorization header value
    pub fn as_header_value(&self) -> String {
        format!("Bearer {}", self.token)
    }
}

/// Authentication-related functionality
pub struct Auth;

impl Auth {
    /// Add bearer token to request headers
    ///
    /// Formats the token according to RFC 6750 as "Bearer ***"
    pub fn add_bearer_token(headers: &mut reqwest::header::HeaderMap, token: &str) -> Result<()> {
        use reqwest::header::{
            AUTHORIZATION,
            HeaderValue,
        };

        // Validate token is not empty
        if token.trim().is_empty() {
            return Err(Error::AuthError(
                "Cannot add empty bearer token".to_string(),
            ));
        }

        // Format as Bearer token
        let header_value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
            Error::AuthError("Invalid token format: contains invalid characters".to_string())
        })?;

        headers.insert(AUTHORIZATION, header_value);
        Ok(())
    }

    /// Add bearer token from `AuthConfig` to request headers
    pub fn add_auth_config(
        headers: &mut reqwest::header::HeaderMap,
        config: &AuthConfig,
    ) -> Result<()> {
        Self::add_bearer_token(headers, config.token())
    }

    /// Create a `HeaderValue` from a bearer token
    ///
    /// This is useful when you need the `HeaderValue` directly
    pub fn create_bearer_header(token: &str) -> Result<reqwest::header::HeaderValue> {
        use reqwest::header::HeaderValue;

        if token.trim().is_empty() {
            return Err(Error::AuthError(
                "Cannot create header from empty token".to_string(),
            ));
        }

        HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| Error::AuthError("Token contains invalid header characters".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_config_new() {
        // Valid token
        let config = AuthConfig::new("test-token".to_string()).unwrap();
        assert_eq!(config.token(), "test-token");

        // Empty token should fail
        assert!(AuthConfig::new(String::new()).is_err());
        assert!(AuthConfig::new("   ".to_string()).is_err());
    }

    #[test]
    fn test_auth_config_header_value() {
        let config = AuthConfig::new("my-token".to_string()).unwrap();
        assert_eq!(config.as_header_value(), "Bearer my-token");
    }

    #[test]
    fn test_auth_config_serde() {
        let config = AuthConfig::new("test-token".to_string()).unwrap();

        // Serialize
        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(json, r#"{"token":"test-token"}"#);

        // Deserialize
        let deserialized: AuthConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.token(), "test-token");
    }

    #[test]
    fn test_add_bearer_token() {
        use reqwest::header::{
            AUTHORIZATION,
            HeaderMap,
        };

        let mut headers = HeaderMap::new();
        Auth::add_bearer_token(&mut headers, "test-token").unwrap();

        assert_eq!(
            headers.get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "Bearer test-token"
        );

        // Test with empty token
        let mut headers2 = HeaderMap::new();
        assert!(Auth::add_bearer_token(&mut headers2, "").is_err());
        assert!(Auth::add_bearer_token(&mut headers2, "  ").is_err());
    }

    #[test]
    fn test_add_auth_config() {
        use reqwest::header::{
            AUTHORIZATION,
            HeaderMap,
        };

        let config = AuthConfig::new("config-token".to_string()).unwrap();
        let mut headers = HeaderMap::new();
        Auth::add_auth_config(&mut headers, &config).unwrap();

        assert_eq!(
            headers.get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "Bearer config-token"
        );
    }

    #[test]
    fn test_create_bearer_header() {
        let header = Auth::create_bearer_header("test-token").unwrap();
        assert_eq!(header.to_str().unwrap(), "Bearer test-token");

        // Test with empty token
        assert!(Auth::create_bearer_header("").is_err());
        assert!(Auth::create_bearer_header("  ").is_err());
    }

    #[test]
    fn test_special_characters_in_token() {
        use reqwest::header::HeaderMap;

        // Test with various valid token formats
        let mut headers = HeaderMap::new();

        // Base64 encoded token
        Auth::add_bearer_token(&mut headers, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9").unwrap();

        // Token with hyphens and underscores
        Auth::add_bearer_token(&mut headers, "test-token_123").unwrap();

        // Token with dots (common in JWTs)
        Auth::add_bearer_token(&mut headers, "header.payload.signature").unwrap();
    }

    #[test]
    fn test_auth_config_edge_cases() {
        // Very long token
        let long_token = "a".repeat(10000);
        let config = AuthConfig::new(long_token.clone()).unwrap();
        assert_eq!(config.token(), &long_token);

        // Token with only whitespace at the end
        assert!(AuthConfig::new("token   ".to_string()).is_ok());
        assert!(AuthConfig::new("   token".to_string()).is_ok());

        // Token with newlines should fail with header creation
        let token_with_newline = "token\nwith\nnewline";
        let config_result = AuthConfig::new(token_with_newline.to_string());
        assert!(config_result.is_ok()); // AuthConfig creation succeeds

        // But adding to headers should fail
        let mut headers = reqwest::header::HeaderMap::new();
        assert!(Auth::add_bearer_token(&mut headers, token_with_newline).is_err());
    }

    #[test]
    fn test_auth_config_clone_and_debug() {
        let config = AuthConfig::bearer_token("secret-token".to_string()).unwrap();

        // Test Clone
        let cloned = config.clone();
        assert_eq!(config.token(), cloned.token());

        // Test Debug
        let debug_str = format!("{config:?}");
        assert!(debug_str.contains("AuthConfig"));
        // Note: The default Debug derive does expose the token
        // In a production system, you might want a custom Debug impl
    }

    #[test]
    fn test_auth_config_partial_eq() {
        let config1 = AuthConfig::bearer_token("token123".to_string()).unwrap();
        let config2 = AuthConfig::bearer_token("token123".to_string()).unwrap();
        let config3 = AuthConfig::bearer_token("different-token".to_string()).unwrap();

        assert_eq!(config1, config2);
        assert_ne!(config1, config3);
    }

    #[test]
    fn test_invalid_header_characters() {
        use reqwest::header::HeaderMap;

        let mut headers = HeaderMap::new();

        // Control characters should fail
        assert!(Auth::add_bearer_token(&mut headers, "token\x00").is_err());
        assert!(Auth::add_bearer_token(&mut headers, "token\x1F").is_err());

        // Carriage return should fail
        assert!(Auth::add_bearer_token(&mut headers, "token\rwith\rcarriage").is_err());

        // Note: Some non-ASCII characters might be allowed in modern HTTP implementations
        // The actual validation depends on the underlying HTTP library
    }

    #[test]
    fn test_auth_config_serialization_edge_cases() {
        // Test with escaped characters
        let config = AuthConfig::bearer_token(r#"token"with"quotes"#.to_string()).unwrap();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains(r#"\"with\""#));

        // Deserialize back
        let deserialized: AuthConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.token(), r#"token"with"quotes"#);

        // Test invalid JSON
        assert!(serde_json::from_str::<AuthConfig>("{}").is_err()); // Missing token field
        assert!(serde_json::from_str::<AuthConfig>(r#"{"token":null}"#).is_err());
        assert!(serde_json::from_str::<AuthConfig>(r#"{"token":123}"#).is_err());
        assert!(serde_json::from_str::<AuthConfig>("[]").is_err());
    }

    #[test]
    fn test_header_value_overwrite() {
        use reqwest::header::{
            AUTHORIZATION,
            HeaderMap,
        };

        let mut headers = HeaderMap::new();

        // Add first token
        Auth::add_bearer_token(&mut headers, "first-token").unwrap();
        assert_eq!(
            headers.get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "Bearer first-token"
        );

        // Add second token - should overwrite
        Auth::add_bearer_token(&mut headers, "second-token").unwrap();
        assert_eq!(
            headers.get(AUTHORIZATION).unwrap().to_str().unwrap(),
            "Bearer second-token"
        );

        // Verify only one authorization header exists
        assert_eq!(headers.get_all(AUTHORIZATION).iter().count(), 1);
    }

    #[test]
    fn test_auth_config_thread_safety() {
        use std::{
            sync::Arc,
            thread,
        };

        let config = Arc::new(AuthConfig::bearer_token("shared-token".to_string()).unwrap());
        let mut handles = vec![];

        for _ in 0..10 {
            let config_clone = Arc::clone(&config);
            let handle = thread::spawn(move || {
                // Access the token from multiple threads
                assert_eq!(config_clone.token(), "shared-token");
                assert_eq!(config_clone.as_header_value(), "Bearer shared-token");
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_create_bearer_header_edge_cases() {
        // Valid edge cases
        assert!(Auth::create_bearer_header("a").is_ok()); // Single character
        assert!(Auth::create_bearer_header(&"x".repeat(1000)).is_ok()); // Long token

        // Tokens that look like they have Bearer prefix already
        let header = Auth::create_bearer_header("Bearer token").unwrap();
        assert_eq!(header.to_str().unwrap(), "Bearer Bearer token"); // Double Bearer

        // Token with leading/trailing spaces (trimmed in validation)
        assert!(Auth::create_bearer_header(" token ").is_ok());
    }

    #[test]
    fn test_auth_error_messages() {
        // Test specific error messages
        match AuthConfig::new(String::new()) {
            Err(Error::AuthError(msg)) => {
                assert!(msg.contains("empty"));
            }
            _ => panic!("Expected AuthError"),
        }

        match Auth::add_bearer_token(&mut reqwest::header::HeaderMap::new(), "\n") {
            Err(Error::AuthError(msg)) => {
                assert!(msg.contains("empty") || msg.contains("whitespace"));
            }
            _ => panic!("Expected AuthError"),
        }

        match Auth::create_bearer_header("token\x00") {
            Err(Error::AuthError(msg)) => {
                assert!(msg.contains("invalid") && msg.contains("characters"));
            }
            _ => panic!("Expected AuthError about invalid characters"),
        }
    }

    #[test]
    fn test_auth_config_from_various_sources() {
        // Test bearer_token helper
        let config1 = AuthConfig::bearer_token("token1".to_string()).unwrap();
        assert_eq!(config1.token(), "token1");

        // Test new with trimming
        let config2 = AuthConfig::new("  token2  ".to_string()).unwrap();
        assert_eq!(config2.token(), "  token2  "); // Preserves spaces

        // Test that the bearer_token helper doesn't double-trim
        let config3 = AuthConfig::bearer_token("  token3  ".to_string()).unwrap();
        assert_eq!(config3.token(), "  token3  ");
    }

    #[test]
    fn test_auth_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<AuthConfig>();
        assert_sync::<AuthConfig>();
        assert_send::<Auth>();
        assert_sync::<Auth>();
    }
}
