use crate::{
    DEFAULT_PLATFORM_URL,
    config::{
        CliConfig,
        UserAuth,
    },
    error::AuthError,
};
use alloy_primitives::Address;
use color_eyre::Result;
use colored::Colorize;
use dapp_api_client::generated::client::{
    Client as GeneratedClient,
    types::{
        GetCliAuthCodeResponse,
        GetCliAuthStatusResponse,
    },
};
use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use tokio::time::{
    Duration,
    sleep,
};

/// Interval between authentication status checks
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Maximum number of retry attempts (5 minutes worth of 2-second intervals)
const MAX_RETRIES: u32 = 150;

/// Authentication commands for the PCL CLI
#[derive(clap::Parser)]
#[command(about = "Authenticate the CLI with your Credible Layer Platform account")]
pub struct AuthCommand {
    #[command(subcommand)]
    pub command: AuthSubcommands,

    #[arg(
        short = 'u',
        long = "auth-url",
        env = "PCL_AUTH_URL",
        default_value = DEFAULT_PLATFORM_URL,
        help = "Base URL for authentication service"
    )]
    pub auth_url: url::Url,
}

/// Available authentication subcommands
#[derive(clap::Subcommand)]
#[command(about = "Authentication operations")]
pub enum AuthSubcommands {
    /// Login to PCL
    #[command(
        long_about = "Initiates the login process. Opens a browser window for authentication.",
        after_help = "Example: pcl auth login"
    )]
    Login,

    /// Logout from PCL
    #[command(
        long_about = "Removes stored authentication credentials.",
        after_help = "Example: pcl auth logout"
    )]
    Logout,

    /// Check current authentication status
    #[command(
        long_about = "Displays whether you're currently logged in and shows the connected identity if authenticated.",
        after_help = "Example: pcl auth status"
    )]
    Status,
}

impl AuthCommand {
    /// Execute the authentication command
    pub async fn run(&self, config: &mut CliConfig) -> Result<(), AuthError> {
        match &self.command {
            AuthSubcommands::Login => self.login(config).await,
            AuthSubcommands::Logout => {
                Self::logout(config);
                Ok(())
            }
            AuthSubcommands::Status => {
                Self::status(config);
                Ok(())
            }
        }
    }

    /// Initiate the login process and wait for user authentication
    async fn login(&self, config: &mut CliConfig) -> Result<(), AuthError> {
        if let Some(auth) = &config.auth {
            println!(
                "{} Already logged in as: {}",
                "ℹ️".blue(),
                auth.display_name()
            );
            println!(
                "Please use {} first to login with a different account",
                "pcl auth logout".yellow()
            );
            return Ok(());
        }

        let client = self.api_client();
        let auth_response = Self::request_auth_code(&client).await?;
        self.display_login_instructions(&auth_response);
        self.wait_for_verification(config, &client, &auth_response)
            .await
    }

    // Helper to create a new API client with the base URL set
    fn api_client(&self) -> GeneratedClient {
        let mut base = self.auth_url.clone();
        base.set_path("/api/v1");
        GeneratedClient::new(base.as_str())
    }

    /// Request an authentication code from the server
    async fn request_auth_code(
        client: &GeneratedClient,
    ) -> Result<GetCliAuthCodeResponse, AuthError> {
        client
            .get_cli_auth_code()
            .await
            .map(dapp_api_client::generated::client::ResponseValue::into_inner)
            .map_err(|e| AuthError::AuthRequestFailed(e.to_string()))
    }

    /// Display login URL and code to the user, attempting to open the browser automatically
    fn display_login_instructions(&self, auth_response: &GetCliAuthCodeResponse) {
        let base = self.auth_url.as_str().trim_end_matches('/');
        let url = format!("{base}/device?session_id={}", auth_response.session_id);

        if open::that(&url).is_ok() {
            println!(
                "\n{} Opening browser for authentication...\n\n🔗 {}\n📝 {}\n",
                "🌐".green(),
                url.white(),
                format!("Code: {}", *auth_response.code).green().bold()
            );
        } else {
            println!(
                "\nTo authenticate, please visit:\n\n🔗 {}\n📝 {}\n",
                url.white(),
                format!("Code: {}", *auth_response.code).green().bold()
            );
        }
    }

    /// Wait for the user to complete the authentication process
    async fn wait_for_verification(
        &self,
        config: &mut CliConfig,
        client: &GeneratedClient,
        auth_response: &GetCliAuthCodeResponse,
    ) -> Result<(), AuthError> {
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template("{spinner} {msg}")
                .map_err(|e| {
                    AuthError::InvalidAuthData(format!("Failed to set spinner style: {e}"))
                })?,
        );
        spinner.enable_steady_tick(Duration::from_millis(80));
        spinner.set_message("Waiting for authentication...");

        for _ in 0..MAX_RETRIES {
            // Stop polling once the session has expired
            if chrono::Utc::now() >= auth_response.expires_at {
                spinner.finish_with_message("❌ Session expired");
                return Err(AuthError::SessionExpired);
            }

            let status = match Self::check_auth_status(client, auth_response).await {
                Ok(s) => s,
                // Transient errors — keep polling
                Err(AuthError::ServerError(_) | AuthError::StatusRequestFailed(_)) => {
                    spinner.tick();
                    sleep(POLL_INTERVAL).await;
                    continue;
                }
                // Terminal errors — stop immediately
                Err(e) => {
                    spinner.finish_with_message(format!("❌ {e}"));
                    return Err(e);
                }
            };

            if status.verified {
                let token = status.token.ok_or_else(|| {
                    AuthError::InvalidAuthData("Verified but missing access token".to_string())
                })?;
                let refresh_token = status.refresh_token.ok_or_else(|| {
                    AuthError::InvalidAuthData("Verified but missing refresh token".to_string())
                })?;
                let user_id = status.user_id.ok_or_else(|| {
                    AuthError::InvalidAuthData("Verified but missing user_id".to_string())
                })?;
                let wallet_address = status
                    .address
                    .and_then(|a| a.to_string().parse::<Address>().ok());

                spinner.finish_with_message("✅ Authentication successful!");
                config.auth = Some(UserAuth {
                    access_token: token,
                    refresh_token,
                    expires_at: auth_response.expires_at,
                    user_id: Some(user_id),
                    wallet_address,
                    email: status.email,
                });
                Self::display_success_message(config)?;
                return Ok(());
            }

            spinner.tick();
            sleep(POLL_INTERVAL).await;
        }

        spinner.finish_with_message("❌ Authentication timed out");
        Err(AuthError::Timeout(MAX_RETRIES))
    }

    /// Check authentication status using the generated client.
    async fn check_auth_status(
        client: &GeneratedClient,
        auth_response: &GetCliAuthCodeResponse,
    ) -> Result<GetCliAuthStatusResponse, AuthError> {
        client
            .get_cli_auth_status(&auth_response.device_secret, &auth_response.session_id)
            .await
            .map(dapp_api_client::generated::client::ResponseValue::into_inner)
            .map_err(AuthError::from)
    }

    /// Display success message after authentication
    fn display_success_message(config: &CliConfig) -> Result<(), AuthError> {
        let auth = config
            .auth
            .as_ref()
            .ok_or_else(|| AuthError::InvalidAuthData("Missing auth after update".to_string()))?;
        println!(
            "{}\n🔗 {}\n",
            "Authentication successful! 🎉".green().bold(),
            format!("Connected as: {}", auth.display_name()).white()
        );
        Ok(())
    }

    /// Remove authentication data from configuration
    fn logout(config: &mut CliConfig) {
        config.auth = None;
        println!("{} Logged out successfully", "👋".green());
    }

    /// Display current authentication status
    fn status(config: &CliConfig) {
        let (icon, message) = if let Some(auth) = &config.auth {
            (
                "✅".green(),
                format!("Logged in as: {}", auth.display_name().green().bold()),
            )
        } else {
            ("❌".red(), "Not logged in".to_string())
        };
        println!("{icon} {message}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{
        TimeZone,
        Utc,
    };
    use clap::Parser;
    use mockito::Server;
    use uuid::Uuid;

    fn create_test_config() -> CliConfig {
        CliConfig {
            auth: Some(UserAuth {
                access_token: "test_token".to_string(),
                refresh_token: "test_refresh".to_string(),
                expires_at: Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap(),
                user_id: Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
                wallet_address: Some(
                    "0x1234567890123456789012345678901234567890"
                        .parse()
                        .unwrap(),
                ),
                email: None,
            }),
        }
    }

    fn test_auth_response_json() -> &'static str {
        r#"{"code":"123456","sessionId":"550e8400-e29b-41d4-a716-446655440000","deviceSecret":"test_secret","expiresAt":"2024-12-31T00:00:00Z"}"#
    }

    #[test]
    fn test_display_login_instructions() {
        let cmd = AuthCommand {
            command: AuthSubcommands::Login,
            auth_url: "https://app.phylax.systems".parse().unwrap(),
        };
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();
        cmd.display_login_instructions(&auth_response);
    }

    #[test]
    fn test_display_success_message() {
        let config = create_test_config();
        AuthCommand::display_success_message(&config).unwrap();
    }

    #[tokio::test]
    async fn test_request_auth_code() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/code")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(test_auth_response_json())
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();

        let client = cmd.api_client();
        let result = AuthCommand::request_auth_code(&client).await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(*response.code, "123456");
        mock.assert();
    }

    #[tokio::test]
    async fn test_check_auth_status_verified() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("session_id".into(), "550e8400-e29b-41d4-a716-446655440000".into()),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"verified":true,"user_id":"550e8400-e29b-41d4-a716-446655440000","address":"0x1234567890123456789012345678901234567890","token":"test_token","refresh_token":"test_refresh"}"#)
            .expect(1)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();

        let result = AuthCommand::check_auth_status(&client, &auth_response).await;
        assert!(result.is_ok());
        let status = result.unwrap();
        assert!(status.verified);
        assert_eq!(status.token.as_deref(), Some("test_token"));
        assert_eq!(status.refresh_token.as_deref(), Some("test_refresh"));
        assert_eq!(
            &*status.address.unwrap(),
            "0x1234567890123456789012345678901234567890"
        );
        mock.assert();
    }

    #[tokio::test]
    async fn test_check_auth_status_not_verified() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"verified":false}"#)
            .expect(1)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();

        let result = AuthCommand::check_auth_status(&client, &auth_response).await;
        assert!(result.is_ok());
        let status = result.unwrap();
        assert!(!status.verified);
        assert!(status.token.is_none());
        mock.assert();
    }

    #[tokio::test]
    async fn test_check_auth_status_verified_without_address() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("session_id".into(), "550e8400-e29b-41d4-a716-446655440000".into()),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .expect(1)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"verified":true,"user_id":"550e8400-e29b-41d4-a716-446655440000","token":"test_token","refresh_token":"test_refresh"}"#)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();

        let result = AuthCommand::check_auth_status(&client, &auth_response).await;
        assert!(result.is_ok());
        let status = result.unwrap();
        assert!(status.verified);
        assert_eq!(status.token.as_deref(), Some("test_token"));
        assert!(status.address.is_none());
        mock.assert();
    }

    #[test]
    fn test_logout() {
        let mut config = create_test_config();
        AuthCommand::logout(&mut config);
        assert!(config.auth.is_none());
    }

    #[test]
    fn test_status() {
        let config = create_test_config();
        AuthCommand::status(&config);
    }

    #[test]
    fn test_status_when_logged_out() {
        let config = CliConfig::default();
        AuthCommand::status(&config);
    }

    #[tokio::test]
    async fn test_login_when_already_authenticated() {
        let mut config = create_test_config();
        let cmd = AuthCommand::try_parse_from(vec![
            "auth",
            "--auth-url",
            "https://app.phylax.systems",
            "login",
        ])
        .unwrap();

        let result = cmd.login(&mut config).await;
        assert!(result.is_ok());
        assert_eq!(
            config.auth.as_ref().unwrap().wallet_address,
            Some(
                "0x1234567890123456789012345678901234567890"
                    .parse::<Address>()
                    .unwrap()
            )
        );
    }

    #[tokio::test]
    async fn test_check_auth_status_verified_missing_optional_fields() {
        let mut server = Server::new_async().await;

        // verified:true but missing optional token/address fields
        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"verified":true}"#)
            .expect(1)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();

        let result = AuthCommand::check_auth_status(&client, &auth_response).await;
        assert!(result.is_ok());
        let status = result.unwrap();
        assert!(status.verified);
        assert!(status.token.is_none());
        assert!(status.refresh_token.is_none());
        mock.assert();
    }

    #[tokio::test]
    async fn test_wait_for_verification_stops_when_session_expired() {
        let server = Server::new_async().await;

        // No mocks — the server should never be called because the session
        // is already expired before the first poll.

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let mut config = CliConfig::default();

        // Build an auth response with expiresAt in the past
        let expired_response: GetCliAuthCodeResponse = serde_json::from_str(
            r#"{"code":"999999","sessionId":"550e8400-e29b-41d4-a716-446655440000","deviceSecret":"test_secret","expiresAt":"2020-01-01T00:00:00Z"}"#,
        )
        .unwrap();

        let result = cmd
            .wait_for_verification(&mut config, &client, &expired_response)
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, AuthError::SessionExpired),
            "Expected SessionExpired, got {err:?}"
        );
        // Config should remain unauthenticated
        assert!(config.auth.is_none());
    }

    #[tokio::test]
    async fn test_check_auth_status_session_expired_returns_typed_error() {
        let mut server = Server::new_async().await;

        // Server returns 400 with SESSION_EXPIRED error code
        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"Session has expired","code":"SESSION_EXPIRED"}"#)
            .expect(1)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();

        let result = AuthCommand::check_auth_status(&client, &auth_response).await;
        assert!(
            matches!(result, Err(AuthError::SessionExpired)),
            "Expected SessionExpired, got {result:?}"
        );
        mock.assert();
    }

    #[tokio::test]
    async fn test_check_auth_status_session_not_found_returns_typed_error() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"Session not found","code":"SESSION_NOT_FOUND"}"#)
            .expect(1)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();

        let result = AuthCommand::check_auth_status(&client, &auth_response).await;
        assert!(
            matches!(result, Err(AuthError::SessionNotFound)),
            "Expected SessionNotFound, got {result:?}"
        );
        mock.assert();
    }

    #[tokio::test]
    async fn test_check_auth_status_user_not_found_returns_typed_error() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"User not found. Request a new code and try again.","code":"USER_NOT_FOUND"}"#)
            .expect(1)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();

        let result = AuthCommand::check_auth_status(&client, &auth_response).await;
        assert!(
            matches!(result, Err(AuthError::UserNotFound)),
            "Expected UserNotFound, got {result:?}"
        );
        mock.assert();
    }

    #[tokio::test]
    async fn test_check_auth_status_server_error_returns_typed_error() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(500)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"Internal server error","code":"INTERNAL_ERROR"}"#)
            .expect(1)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();

        let result = AuthCommand::check_auth_status(&client, &auth_response).await;
        assert!(
            matches!(result, Err(AuthError::ServerError(_))),
            "Expected ServerError, got {result:?}"
        );
        mock.assert();
    }

    #[tokio::test]
    async fn test_polling_stops_on_session_expired() {
        let mut server = Server::new_async().await;

        // First poll: pending. Second poll: session expired.
        let pending_mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"verified":false}"#)
            .expect(1)
            .create();

        let expired_mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(400)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"Session has expired","code":"SESSION_EXPIRED"}"#)
            .expect(1)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let mut config = CliConfig::default();

        // Use a far-future expiresAt so the client-side check doesn't trigger
        let auth_response: GetCliAuthCodeResponse = serde_json::from_str(
            r#"{"code":"123456","sessionId":"550e8400-e29b-41d4-a716-446655440000","deviceSecret":"test_secret","expiresAt":"2099-12-31T00:00:00Z"}"#,
        )
        .unwrap();

        let result = cmd
            .wait_for_verification(&mut config, &client, &auth_response)
            .await;

        assert!(
            matches!(result, Err(AuthError::SessionExpired)),
            "Expected SessionExpired, got {result:?}"
        );
        assert!(config.auth.is_none());
        pending_mock.assert();
        expired_mock.assert();
    }

    #[tokio::test]
    async fn test_polling_retries_on_server_error() {
        let mut server = Server::new_async().await;

        // First poll: 500 (transient). Second poll: success.
        let error_mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(500)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"Internal server error","code":"INTERNAL_ERROR"}"#)
            .expect(1)
            .create();

        let success_mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"verified":true,"user_id":"550e8400-e29b-41d4-a716-446655440000","token":"test_token","refresh_token":"test_refresh","address":"0x1234567890123456789012345678901234567890"}"#)
            .expect(1)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let mut config = CliConfig::default();

        let auth_response: GetCliAuthCodeResponse = serde_json::from_str(
            r#"{"code":"123456","sessionId":"550e8400-e29b-41d4-a716-446655440000","deviceSecret":"test_secret","expiresAt":"2099-12-31T00:00:00Z"}"#,
        )
        .unwrap();

        let result = cmd
            .wait_for_verification(&mut config, &client, &auth_response)
            .await;

        assert!(
            result.is_ok(),
            "Expected success after retry, got {result:?}"
        );
        assert!(config.auth.is_some());
        error_mock.assert();
        success_mock.assert();
    }

    #[tokio::test]
    async fn test_check_auth_status_invalid_json() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded(
                    "session_id".into(),
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r"not valid json")
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();
        let client = cmd.api_client();
        let auth_response: GetCliAuthCodeResponse =
            serde_json::from_str(test_auth_response_json()).unwrap();

        let result = AuthCommand::check_auth_status(&client, &auth_response).await;
        assert!(result.is_err());
        mock.assert();
    }
}
