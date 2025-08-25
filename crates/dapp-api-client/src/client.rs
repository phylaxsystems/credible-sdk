//! Main client for interacting with the dapp API

use crate::{
    Auth,
    AuthConfig,
    Config,
    Error,
    Result,
    generated::GeneratedClient,
};

/// Main client for dapp API operations
pub struct Client {
    config: Config,
    inner: GeneratedClient,
    auth_config: Option<AuthConfig>,
}

impl Client {
    /// Create a new client instance without authentication
    pub fn new(config: Config) -> Result<Self> {
        let base_url = &config.base_url;
        let http_client = reqwest::Client::new();

        let inner = GeneratedClient::new_with_client(base_url, http_client);

        Ok(Self {
            config,
            inner,
            auth_config: None,
        })
    }

    /// Create a new client instance with authentication
    pub fn new_with_auth(config: Config, auth_config: AuthConfig) -> Result<Self> {
        let base_url = &config.base_url;

        // Create HTTP client with default auth header
        let mut headers = reqwest::header::HeaderMap::new();
        Auth::add_auth_config(&mut headers, &auth_config)?;

        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|e| Error::ConfigError(format!("Failed to build HTTP client: {e}")))?;

        let inner = GeneratedClient::new_with_client(base_url, http_client);

        Ok(Self {
            config,
            inner,
            auth_config: Some(auth_config),
        })
    }

    /// Get the base URL being used
    pub fn base_url(&self) -> &str {
        &self.config.base_url
    }

    /// Get a reference to the inner generated client
    ///
    /// This provides access to all the auto-generated API methods
    pub fn inner(&self) -> &GeneratedClient {
        &self.inner
    }

    /// Get a mutable reference to the inner generated client
    pub fn inner_mut(&mut self) -> &mut GeneratedClient {
        &mut self.inner
    }

    /// Get the current configuration
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the authentication configuration if set
    pub fn auth_config(&self) -> Option<&AuthConfig> {
        self.auth_config.as_ref()
    }

    /// Update the authentication configuration
    ///
    /// This creates a new HTTP client with the updated auth headers
    pub fn set_auth(&mut self, auth_config: AuthConfig) -> Result<()> {
        let base_url = &self.config.base_url;

        // Create new HTTP client with updated auth header
        let mut headers = reqwest::header::HeaderMap::new();
        Auth::add_auth_config(&mut headers, &auth_config)?;

        let http_client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|e| Error::ConfigError(format!("Failed to build HTTP client: {e}")))?;

        self.inner = GeneratedClient::new_with_client(base_url, http_client);
        self.auth_config = Some(auth_config);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rstest::*;

    fn test_config() -> Config {
        Config::new("https://example.com")
    }

    fn test_auth_config() -> AuthConfig {
        AuthConfig::bearer_token("test-token-123".to_string()).unwrap()
    }

    #[test]
    fn test_client_new_without_auth() {
        let config = test_config();
        let base_url = config.base_url.clone();

        let client = Client::new(config).expect("Failed to create client");

        assert_eq!(client.base_url(), &base_url);
        assert!(client.auth_config().is_none());
    }

    #[test]
    fn test_client_new_with_auth() {
        let config = test_config();
        let auth_config = test_auth_config();
        let base_url = config.base_url.clone();

        let client = Client::new_with_auth(config, auth_config.clone())
            .expect("Failed to create client with auth");

        assert_eq!(client.base_url(), &base_url);
        assert_eq!(client.auth_config(), Some(&auth_config));
    }

    #[test]
    fn test_client_new_with_invalid_base_url() {
        let config = Config::new("");
        let result = Client::new(config);

        // Should still succeed as the generated client doesn't validate URLs on creation
        assert!(result.is_ok());
    }

    #[test]
    fn test_client_accessors() {
        let config = test_config();
        let original_config = config.clone();
        let client = Client::new(config).expect("Failed to create client");

        // Test base_url accessor
        assert_eq!(client.base_url(), &original_config.base_url);

        // Test config accessor
        assert_eq!(client.config(), &original_config);

        // Test auth_config accessor when no auth
        assert!(client.auth_config().is_none());
    }

    #[test]
    fn test_client_with_auth_accessors() {
        let config = test_config();
        let auth_config = test_auth_config();
        let client =
            Client::new_with_auth(config, auth_config.clone()).expect("Failed to create client");

        // Test auth_config accessor when auth is set
        assert_eq!(client.auth_config(), Some(&auth_config));
    }

    #[test]
    fn test_client_inner_references() {
        let config = test_config();
        let client = Client::new(config).expect("Failed to create client");

        // Test that we can get immutable reference to inner client
        let _inner = client.inner();

        // Test that we can get mutable reference to inner client
        let mut client = client;
        let _inner_mut = client.inner_mut();
    }

    #[test]
    fn test_set_auth_updates_auth_config() {
        let config = test_config();
        let mut client = Client::new(config).expect("Failed to create client");

        // Initially no auth
        assert!(client.auth_config().is_none());

        // Set auth
        let auth_config = test_auth_config();
        client
            .set_auth(auth_config.clone())
            .expect("Failed to set auth");

        // Verify auth was set
        assert_eq!(client.auth_config(), Some(&auth_config));
    }

    #[test]
    fn test_set_auth_can_update_existing_auth() {
        let config = test_config();
        let initial_auth = AuthConfig::bearer_token("initial-token".to_string()).unwrap();
        let mut client =
            Client::new_with_auth(config, initial_auth.clone()).expect("Failed to create client");

        // Verify initial auth
        assert_eq!(client.auth_config(), Some(&initial_auth));

        // Update auth
        let new_auth = AuthConfig::bearer_token("new-token".to_string()).unwrap();
        client
            .set_auth(new_auth.clone())
            .expect("Failed to update auth");

        // Verify auth was updated
        assert_eq!(client.auth_config(), Some(&new_auth));
    }

    #[test]
    fn test_client_with_special_characters_in_url() {
        let config = Config::new("https://example.com/path%20with%20spaces");
        let client = Client::new(config).expect("Failed to create client");

        assert_eq!(
            client.base_url(),
            "https://example.com/path%20with%20spaces"
        );
    }

    #[test]
    fn test_client_with_port_in_url() {
        let config = Config::new("https://example.com:8080");
        let client = Client::new(config).expect("Failed to create client");

        assert_eq!(client.base_url(), "https://example.com:8080");
    }

    #[test]
    fn test_client_with_path_in_url() {
        let config = Config::new("https://example.com/api/v1");
        let client = Client::new(config).expect("Failed to create client");

        assert_eq!(client.base_url(), "https://example.com/api/v1");
    }

    #[test]
    fn test_client_with_query_params_in_url() {
        let config = Config::new("https://example.com?key=value");
        let client = Client::new(config).expect("Failed to create client");

        assert_eq!(client.base_url(), "https://example.com?key=value");
    }

    #[test]
    fn test_multiple_auth_updates() {
        let config = test_config();
        let mut client = Client::new(config).expect("Failed to create client");

        // Set auth multiple times
        for i in 0..5 {
            let auth = AuthConfig::bearer_token(format!("token-{i}")).unwrap();
            client.set_auth(auth.clone()).expect("Failed to set auth");
            assert_eq!(client.auth_config(), Some(&auth));
        }
    }

    #[test]
    fn test_client_with_localhost_url() {
        let config = Config::new("http://localhost:3000");
        let client = Client::new(config).expect("Failed to create client");

        assert_eq!(client.base_url(), "http://localhost:3000");
    }

    #[test]
    fn test_client_with_ip_address_url() {
        let config = Config::new("http://192.168.1.1:8080");
        let client = Client::new(config).expect("Failed to create client");

        assert_eq!(client.base_url(), "http://192.168.1.1:8080");
    }

    #[rstest]
    #[case("https://example.com")]
    #[case("https://staging.example.com")]
    #[case("http://localhost:3000")]
    #[case("https://api.example.com/v2")]
    fn test_client_creation_with_various_configs(#[case] base_url: &str) {
        let config = Config::new(base_url);
        let client = Client::new(config.clone()).expect("Failed to create client");

        assert_eq!(client.base_url(), base_url);
        assert_eq!(client.config(), &config);
        assert!(client.auth_config().is_none());
    }

    #[rstest]
    #[case("token123")]
    #[case("Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")]
    #[case("sk-1234567890abcdef")]
    #[case("xyz.abc.123-456_789")]
    fn test_client_with_various_auth_tokens(#[case] token: &str) {
        let config = test_config();
        let auth_config = AuthConfig::bearer_token(token.to_string()).unwrap();
        let client = Client::new_with_auth(config, auth_config.clone())
            .expect("Failed to create client with auth");

        assert_eq!(client.auth_config(), Some(&auth_config));
    }

    #[test]
    fn test_client_thread_safety() {
        use std::{
            sync::Arc,
            thread,
        };

        let config = test_config();
        let client = Arc::new(Client::new(config).expect("Failed to create client"));

        let handles: Vec<_> = (0..5)
            .map(|_| {
                let client_clone = Arc::clone(&client);
                thread::spawn(move || {
                    // Just verify we can access the client from multiple threads
                    let _ = client_clone.base_url();
                    let _ = client_clone.config();
                    let _ = client_clone.auth_config();
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    }
}
