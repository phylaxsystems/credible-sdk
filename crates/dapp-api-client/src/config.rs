//! Configuration for the dapp API client

use crate::error::{
    Error,
    Result,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::fmt;
use std::str::FromStr;

/// Environment for the dapp API client
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Environment {
    /// Development environment (localhost)
    Development,
    /// Production environment (dapp.phylax.systems)
    Production,
}

impl Default for Environment {
    /// Returns Production as the default environment
    fn default() -> Self {
        Environment::Production
    }
}

impl FromStr for Environment {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "development" | "dev" => Ok(Environment::Development),
            "production" | "prod" => Ok(Environment::Production),
            _ => {
                Err(Error::ConfigError(format!(
                    "Invalid environment '{s}'. Valid values are: development, dev, production, prod"
                )))
            }
        }
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Environment::Development => write!(f, "Development"),
            Environment::Production => write!(f, "Production"),
        }
    }
}

impl Environment {
    /// Get the base URL for this environment
    pub fn base_url(&self) -> &'static str {
        match self {
            Environment::Development => "http://localhost:3000/api/v1",
            Environment::Production => "https://dapp.phylax.systems/api/v1",
        }
    }

    /// Load environment from environment variable
    ///
    /// Checks the `DAPP_ENV` environment variable and returns the corresponding Environment.
    /// Valid values: "development", "dev", "production", "prod" (case-insensitive)
    /// Returns None if the variable is not set or contains an invalid value.
    pub fn from_env() -> Option<Self> {
        std::env::var("DAPP_ENV").ok().and_then(|val| {
            match val.to_lowercase().as_str() {
                "development" | "dev" => Some(Environment::Development),
                "production" | "prod" => Some(Environment::Production),
                _ => None,
            }
        })
    }

    /// Load environment from environment variable with a default fallback
    ///
    /// Same as `from_env()` but returns the provided default if the environment
    /// variable is not set or invalid.
    pub fn from_env_or(default: Self) -> Self {
        Self::from_env().unwrap_or(default)
    }
}

/// Configuration for the dapp API client
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Config {
    /// Base URL for the API
    pub base_url: String,
    /// Bearer token for authentication
    pub bearer_token: Option<String>,
}

impl Config {
    /// Create a new configuration
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            bearer_token: None,
        }
    }

    /// Create a new configuration from an Environment
    pub fn from_environment(env: Environment) -> Self {
        Self {
            base_url: env.base_url().to_string(),
            bearer_token: None,
        }
    }

    /// Create a new configuration from environment variables
    ///
    /// Uses DAPP_ENV to determine the environment, defaulting to Production
    pub fn from_env() -> Self {
        let env = Environment::from_env_or(Environment::default());
        Self::from_environment(env)
    }

    /// Set the bearer token
    pub fn with_bearer_token(mut self, token: String) -> Self {
        self.bearer_token = Some(token);
        self
    }

    /// Validate the configuration
    ///
    /// Ensures the base URL is valid and properly formatted
    pub fn validate(&self) -> Result<()> {
        // Basic URL validation
        if self.base_url.is_empty() {
            return Err(Error::ConfigError("Base URL cannot be empty".to_string()));
        }

        // Check URL format
        if !self.base_url.starts_with("http://") && !self.base_url.starts_with("https://") {
            return Err(Error::ConfigError(
                "Base URL must start with http:// or https://".to_string(),
            ));
        }

        // Validate bearer token if present
        if let Some(token) = &self.bearer_token {
            if token.trim().is_empty() {
                return Err(Error::ConfigError(
                    "Bearer token cannot be empty or whitespace".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Create and validate a configuration
    ///
    /// Same as `new()` but validates the configuration before returning
    pub fn new_validated(base_url: String) -> Result<Self> {
        let config = Self::new(base_url);
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_environment_urls() {
        assert_eq!(
            Environment::Development.base_url(),
            "http://localhost:3000/api/v1"
        );
        assert_eq!(
            Environment::Production.base_url(),
            "https://dapp.phylax.systems/api/v1"
        );
    }

    #[test]
    fn test_environment_equality() {
        assert_eq!(Environment::Development, Environment::Development);
        assert_eq!(Environment::Production, Environment::Production);
        assert_ne!(Environment::Development, Environment::Production);
    }

    #[test]
    fn test_environment_copy_clone() {
        let env = Environment::Development;
        let env_copy = env; // Tests Copy trait
        let env_clone = env; // Tests Clone trait (via Copy)

        assert_eq!(env, env_copy);
        assert_eq!(env, env_clone);
    }

    #[test]
    fn test_environment_serde() {
        // Test serialization
        let env = Environment::Production;
        let json = serde_json::to_string(&env).unwrap();
        assert_eq!(json, "\"Production\"");

        // Test deserialization
        let env_from_json: Environment = serde_json::from_str(&json).unwrap();
        assert_eq!(env_from_json, Environment::Production);
    }

    #[test]
    fn test_environment_from_env() {
        unsafe {
            // Test with valid development values
            std::env::set_var("DAPP_ENV", "development");
            assert_eq!(Environment::from_env(), Some(Environment::Development));

            std::env::set_var("DAPP_ENV", "dev");
            assert_eq!(Environment::from_env(), Some(Environment::Development));

            std::env::set_var("DAPP_ENV", "DEVELOPMENT");
            assert_eq!(Environment::from_env(), Some(Environment::Development));

            // Test with valid production values
            std::env::set_var("DAPP_ENV", "production");
            assert_eq!(Environment::from_env(), Some(Environment::Production));

            std::env::set_var("DAPP_ENV", "prod");
            assert_eq!(Environment::from_env(), Some(Environment::Production));

            std::env::set_var("DAPP_ENV", "PRODUCTION");
            assert_eq!(Environment::from_env(), Some(Environment::Production));

            // Test with invalid value
            std::env::set_var("DAPP_ENV", "invalid");
            assert_eq!(Environment::from_env(), None);

            // Test with missing env var
            std::env::remove_var("DAPP_ENV");
            assert_eq!(Environment::from_env(), None);
        }
    }

    #[test]
    fn test_environment_from_env_or() {
        unsafe {
            // Test with valid value
            std::env::set_var("DAPP_ENV", "dev");
            assert_eq!(
                Environment::from_env_or(Environment::Production),
                Environment::Development
            );

            // Test with invalid value - should use default
            std::env::set_var("DAPP_ENV", "invalid");
            assert_eq!(
                Environment::from_env_or(Environment::Production),
                Environment::Production
            );

            // Test with missing env var - should use default
            std::env::remove_var("DAPP_ENV");
            assert_eq!(
                Environment::from_env_or(Environment::Development),
                Environment::Development
            );
        }
    }

    #[test]
    fn test_environment_default() {
        assert_eq!(Environment::default(), Environment::Production);
    }

    #[test]
    fn test_config_from_environment() {
        let dev_config = Config::from_environment(Environment::Development);
        assert_eq!(dev_config.base_url, "http://localhost:3000/api/v1");
        assert_eq!(dev_config.bearer_token, None);

        let prod_config = Config::from_environment(Environment::Production);
        assert_eq!(prod_config.base_url, "https://dapp.phylax.systems/api/v1");
        assert_eq!(prod_config.bearer_token, None);
    }

    #[test]
    fn test_config_from_env() {
        unsafe {
            // Test with production env
            std::env::set_var("DAPP_ENV", "production");
            let config = Config::from_env();
            assert_eq!(config.base_url, "https://dapp.phylax.systems/api/v1");

            // Test with development env
            std::env::set_var("DAPP_ENV", "dev");
            let config = Config::from_env();
            assert_eq!(config.base_url, "http://localhost:3000/api/v1");

            // Test with missing env var (should default to production)
            std::env::remove_var("DAPP_ENV");
            let config = Config::from_env();
            assert_eq!(config.base_url, "https://dapp.phylax.systems/api/v1");
        }
    }

    #[test]
    fn test_environment_from_str() {
        // Valid values
        assert_eq!(
            Environment::from_str("development").unwrap(),
            Environment::Development
        );
        assert_eq!(
            Environment::from_str("dev").unwrap(),
            Environment::Development
        );
        assert_eq!(
            Environment::from_str("DEVELOPMENT").unwrap(),
            Environment::Development
        );
        assert_eq!(
            Environment::from_str("production").unwrap(),
            Environment::Production
        );
        assert_eq!(
            Environment::from_str("prod").unwrap(),
            Environment::Production
        );
        assert_eq!(
            Environment::from_str("PRODUCTION").unwrap(),
            Environment::Production
        );

        // Invalid values
        assert!(Environment::from_str("invalid").is_err());
        assert!(Environment::from_str("testing").is_err());
        assert!(Environment::from_str("").is_err());
    }

    #[test]
    fn test_config_validation() {
        // Valid configurations
        let valid_config = Config::new("https://api.example.com".to_string());
        assert!(valid_config.validate().is_ok());

        let valid_config_http = Config::new("http://localhost:3000".to_string());
        assert!(valid_config_http.validate().is_ok());

        // Invalid configurations
        let empty_url = Config::new("".to_string());
        assert!(empty_url.validate().is_err());

        let invalid_url = Config::new("not-a-url".to_string());
        assert!(invalid_url.validate().is_err());

        // Invalid bearer token
        let mut config_with_empty_token = Config::new("https://api.example.com".to_string());
        config_with_empty_token.bearer_token = Some("   ".to_string());
        assert!(config_with_empty_token.validate().is_err());
    }

    #[test]
    fn test_config_new_validated() {
        // Valid URL
        assert!(Config::new_validated("https://api.example.com".to_string()).is_ok());

        // Invalid URL
        assert!(Config::new_validated("".to_string()).is_err());
        assert!(Config::new_validated("invalid-url".to_string()).is_err());
    }

    #[test]
    fn test_config_with_bearer_token() {
        let config = Config::new("https://api.example.com".to_string())
            .with_bearer_token("my-token".to_string());

        assert_eq!(config.bearer_token, Some("my-token".to_string()));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_edge_cases() {
        // URLs with unusual but valid formats
        assert!(
            Config::new("http://127.0.0.1:8080".to_string())
                .validate()
                .is_ok()
        );
        assert!(
            Config::new("https://api.example.com:443".to_string())
                .validate()
                .is_ok()
        );
        assert!(
            Config::new("http://[::1]:8080".to_string())
                .validate()
                .is_ok()
        ); // IPv6
        assert!(
            Config::new("https://sub.domain.example.com".to_string())
                .validate()
                .is_ok()
        );

        // URLs with paths and query params
        assert!(
            Config::new("https://api.example.com/v1/endpoint".to_string())
                .validate()
                .is_ok()
        );
        assert!(
            Config::new("https://api.example.com?param=value".to_string())
                .validate()
                .is_ok()
        );
        assert!(
            Config::new("https://api.example.com/path?param=value#fragment".to_string())
                .validate()
                .is_ok()
        );

        // Bearer token edge cases
        let mut config = Config::new("https://api.example.com".to_string());
        config.bearer_token = Some("".to_string());
        assert!(config.validate().is_err());

        config.bearer_token = Some("\t\n ".to_string());
        assert!(config.validate().is_err());

        // Very long token should be valid
        config.bearer_token = Some("a".repeat(1000));
        assert!(config.validate().is_ok());

        // Token with special characters
        config.bearer_token = Some("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_environment_from_str_edge_cases() {
        // Whitespace handling
        assert!(Environment::from_str("  ").is_err());
        assert!(Environment::from_str("\t").is_err());
        assert!(Environment::from_str("\n").is_err());

        // Case variations with whitespace
        assert_eq!(
            Environment::from_str(" development ").unwrap(),
            Environment::Development
        );
        assert_eq!(
            Environment::from_str("\tPRODUCTION\n").unwrap(),
            Environment::Production
        );

        // Unicode edge cases
        assert!(Environment::from_str("développement").is_err());
        assert!(Environment::from_str("プロダクション").is_err());

        // Near-matches
        assert!(Environment::from_str("devs").is_err());
        assert!(Environment::from_str("product").is_err());
        assert!(Environment::from_str("develop").is_err());
    }

    #[test]
    fn test_environment_serialization_edge_cases() {
        // Test that serialization round-trips correctly
        let envs = vec![Environment::Development, Environment::Production];

        for env in envs {
            let serialized = serde_json::to_string(&env).unwrap();
            let deserialized: Environment = serde_json::from_str(&serialized).unwrap();
            assert_eq!(env, deserialized);
        }

        // Test invalid JSON deserialization
        assert!(serde_json::from_str::<Environment>("\"Invalid\"").is_err());
        assert!(serde_json::from_str::<Environment>("123").is_err());
        assert!(serde_json::from_str::<Environment>("null").is_err());
        assert!(serde_json::from_str::<Environment>("{}").is_err());
    }

    #[test]
    fn test_config_clone_and_debug() {
        let config = Config {
            base_url: "https://api.example.com".to_string(),
            bearer_token: Some("secret-token".to_string()),
        };

        let cloned = config.clone();
        assert_eq!(config.base_url, cloned.base_url);
        assert_eq!(config.bearer_token, cloned.bearer_token);

        // Test Debug implementation
        let debug_str = format!("{config:?}");
        assert!(debug_str.contains("Config"));
        assert!(debug_str.contains("base_url"));
        assert!(debug_str.contains("bearer_token"));
    }

    #[test]
    fn test_config_partial_eq() {
        let config1 = Config::new("https://api.example.com".to_string());
        let config2 = Config::new("https://api.example.com".to_string());
        let config3 = Config::new("https://different.com".to_string());

        assert_eq!(config1, config2);
        assert_ne!(config1, config3);

        // Test with bearer tokens
        let config_with_token1 = Config::new("https://api.example.com".to_string())
            .with_bearer_token("token123".to_string());
        let config_with_token2 = Config::new("https://api.example.com".to_string())
            .with_bearer_token("token123".to_string());
        let config_with_different_token = Config::new("https://api.example.com".to_string())
            .with_bearer_token("different-token".to_string());

        assert_eq!(config_with_token1, config_with_token2);
        assert_ne!(config_with_token1, config_with_different_token);
        assert_ne!(config1, config_with_token1);
    }

    #[test]
    fn test_environment_from_env_thread_safety() {
        use std::sync::Arc;
        use std::sync::Mutex;
        use std::thread;

        let results = Arc::new(Mutex::new(Vec::new()));
        let mut handles = vec![];

        for i in 0..10 {
            let results_clone = Arc::clone(&results);
            let handle = thread::spawn(move || unsafe {
                if i % 2 == 0 {
                    std::env::set_var("DAPP_ENV", "development");
                } else {
                    std::env::set_var("DAPP_ENV", "production");
                }

                let env = Environment::from_env();
                results_clone.lock().unwrap().push(env);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let results = results.lock().unwrap();
        assert_eq!(results.len(), 10);
        // All results should be Some(Environment)
        assert!(results.iter().all(|e| e.is_some()));
    }

    #[test]
    fn test_config_validation_url_edge_cases() {
        // File URLs should fail
        assert!(
            Config::new("file:///path/to/file".to_string())
                .validate()
                .is_err()
        );

        // FTP URLs should fail
        assert!(
            Config::new("ftp://example.com".to_string())
                .validate()
                .is_err()
        );

        // URLs with authentication should be valid
        assert!(
            Config::new("https://user:pass@example.com".to_string())
                .validate()
                .is_ok()
        );

        // URLs with non-standard ports
        assert!(
            Config::new("http://example.com:65535".to_string())
                .validate()
                .is_ok()
        );
        assert!(
            Config::new("http://example.com:1".to_string())
                .validate()
                .is_ok()
        );

        // URLs with encoded characters
        assert!(
            Config::new("https://example.com/path%20with%20spaces".to_string())
                .validate()
                .is_ok()
        );
        assert!(
            Config::new("https://example.com/über".to_string())
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn test_environment_display_and_string_conversion() {
        // Test Display trait (via to_string())
        assert_eq!(Environment::Development.to_string(), "Development");
        assert_eq!(Environment::Production.to_string(), "Production");

        // Test that Display output can be parsed back
        let dev_str = Environment::Development.to_string();
        assert_eq!(
            Environment::from_str(&dev_str).unwrap(),
            Environment::Development
        );

        let prod_str = Environment::Production.to_string();
        assert_eq!(
            Environment::from_str(&prod_str).unwrap(),
            Environment::Production
        );
    }

    #[test]
    fn test_config_new_with_trimmed_url() {
        // URLs with leading/trailing whitespace
        let config = Config::new("  https://api.example.com  ".to_string());
        assert_eq!(config.base_url, "  https://api.example.com  "); // Should preserve whitespace

        // Validation should handle this appropriately
        assert!(config.validate().is_err()); // Will fail because of leading spaces
    }
}
