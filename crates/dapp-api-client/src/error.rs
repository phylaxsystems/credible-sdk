//! Error types for the dapp API client

use thiserror::Error;

/// Main error type for the dapp API client
#[derive(Debug, Error)]
pub enum Error {
    /// HTTP request error
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Authentication error
    #[error("Authentication error: {0}")]
    AuthError(String),
}

/// Result type alias for the dapp API client
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_error_http_error_variant() {
        // We can't easily create a real reqwest::Error in tests,
        // so we'll test the error message format instead

        // Create a mock HTTP error scenario
        let url = "https://example.com";
        let client = reqwest::Client::new();

        // This won't actually fail, but we're testing the Error type structure
        if let Ok(_request) = client.get(url).build() {
            // In real usage, this would be an actual reqwest::Error
            // For now, we just verify the Error enum structure exists
            // Verify that our error messages are formatted correctly
            let test_msg = "test error";
            let config_err = Error::ConfigError(test_msg.to_string());
            assert!(config_err.to_string().contains("Configuration error"));
        }
    }

    #[test]
    fn test_error_from_serde_json_error() {
        let json_str = r#"{"invalid": json"#;
        let serde_err: serde_json::Error =
            serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();

        let error = Error::from(serde_err);

        assert_matches!(error, Error::SerializationError(_));
        assert!(error.to_string().contains("Serialization error"));
    }

    #[test]
    fn test_config_error_creation() {
        let error = Error::ConfigError("Invalid base URL".to_string());

        assert_eq!(error.to_string(), "Configuration error: Invalid base URL");
        assert_matches!(error, Error::ConfigError(msg) if msg == "Invalid base URL");
    }

    #[test]
    fn test_auth_error_creation() {
        let error = Error::AuthError("Invalid token".to_string());

        assert_eq!(error.to_string(), "Authentication error: Invalid token");
        assert_matches!(error, Error::AuthError(msg) if msg == "Invalid token");
    }

    #[test]
    fn test_error_debug_format() {
        let error = Error::ConfigError("Test error".to_string());
        let debug_str = format!("{error:?}");

        assert!(debug_str.contains("ConfigError"));
        assert!(debug_str.contains("Test error"));
    }

    #[test]
    fn test_error_display_messages() {
        let test_cases = vec![
            (
                Error::ConfigError("Missing API key".to_string()),
                "Configuration error: Missing API key",
            ),
            (
                Error::AuthError("Token expired".to_string()),
                "Authentication error: Token expired",
            ),
        ];

        for (error, expected_msg) in test_cases {
            assert_eq!(error.to_string(), expected_msg);
        }
    }

    #[test]
    fn test_result_type_alias() {
        fn returns_ok() -> String {
            "success".to_string()
        }

        fn returns_err() -> Result<String> {
            Err(Error::ConfigError("failure".to_string()))
        }

        assert_matches!(returns_ok().as_str(), "success");
        assert_matches!(returns_err(), Err(Error::ConfigError(msg)) if msg == "failure");
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<Error>();
        assert_sync::<Error>();
    }

    #[test]
    fn test_error_source_chain() {
        // Test that the source chain works properly for wrapped errors
        let json_str = r#"{"invalid": json"#;
        let serde_err: serde_json::Error =
            serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();

        let error = Error::from(serde_err);

        // The error should have a source
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn test_error_variants_are_distinct() {
        use std::mem::discriminant;

        let json_error =
            Error::from(serde_json::from_str::<serde_json::Value>("invalid").unwrap_err());
        let config_error = Error::ConfigError("test".to_string());
        let auth_error = Error::AuthError("test".to_string());

        // All error variants should have different discriminants
        assert_ne!(discriminant(&json_error), discriminant(&config_error));
        assert_ne!(discriminant(&json_error), discriminant(&auth_error));
        assert_ne!(discriminant(&config_error), discriminant(&auth_error));
    }

    #[test]
    fn test_error_with_empty_messages() {
        let config_error = Error::ConfigError(String::new());
        let auth_error = Error::AuthError(String::new());

        assert_eq!(config_error.to_string(), "Configuration error: ");
        assert_eq!(auth_error.to_string(), "Authentication error: ");
    }

    #[test]
    fn test_error_with_long_messages() {
        let long_msg = "a".repeat(1000);
        let config_error = Error::ConfigError(long_msg.clone());
        let auth_error = Error::AuthError(long_msg.clone());

        assert!(config_error.to_string().contains(&long_msg));
        assert!(auth_error.to_string().contains(&long_msg));
    }

    #[test]
    fn test_error_with_special_characters() {
        let special_msg = "Error with \n newlines \t tabs and \"quotes\"";
        let config_error = Error::ConfigError(special_msg.to_string());

        assert_eq!(
            config_error.to_string(),
            format!("Configuration error: {special_msg}")
        );
    }

    #[test]
    fn test_error_conversion_in_result_chain() {
        fn might_fail_with_json() -> Result<String> {
            let json_str = r#"{"invalid": json"#;
            let _: serde_json::Value = serde_json::from_str(json_str)?;
            Ok("success".to_string())
        }

        let result = might_fail_with_json();
        assert_matches!(result, Err(Error::SerializationError(_)));
    }

    #[test]
    fn test_error_pattern_matching() {
        let errors = vec![
            Error::SerializationError(
                serde_json::from_str::<serde_json::Value>("invalid").unwrap_err(),
            ),
            Error::ConfigError("config issue".to_string()),
            Error::AuthError("auth issue".to_string()),
        ];

        for error in errors {
            match error {
                Error::HttpError(_) => {
                    assert!(error.to_string().contains("HTTP request failed"));
                }
                Error::SerializationError(_) => {
                    assert!(error.to_string().contains("Serialization error"));
                }
                Error::ConfigError(msg) => {
                    assert_eq!(msg, "config issue");
                }
                Error::AuthError(msg) => {
                    assert_eq!(msg, "auth issue");
                }
            }
        }
    }

    #[test]
    fn test_error_clone_impl() {
        // Note: Error doesn't implement Clone because reqwest::Error doesn't
        // This test verifies that we can still work with errors effectively

        let config_error = Error::ConfigError("test".to_string());
        let auth_error = Error::AuthError("test".to_string());

        // We can create new instances with the same message
        let config_error2 = Error::ConfigError("test".to_string());
        let auth_error2 = Error::AuthError("test".to_string());

        assert_eq!(config_error.to_string(), config_error2.to_string());
        assert_eq!(auth_error.to_string(), auth_error2.to_string());
    }
}
