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
    ClientInfo,
    types::{
        GetCliAuthCodeResponse,
        GetCliAuthStatusResponse,
        GetCliAuthStatusResponseVariant1Address,
        GetCliAuthStatusResponseVariant1Username,
    },
};
use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use serde::Deserialize;
use tokio::time::{
    Duration,
    sleep,
};

/// Interval between authentication status checks
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Maximum number of retry attempts (5 minutes worth of 2-second intervals)
const MAX_RETRIES: u32 = 150;
/// Timeout for individual auth status requests
const AUTH_STATUS_TIMEOUT: Duration = Duration::from_secs(60);

/// Flat deserialization target for the auth status endpoint.
///
/// The generated `GetCliAuthStatusResponse` untagged enum always deserializes as
/// Variant0 because typify ignores `enum:[true/false]` constraints. We
/// deserialize here then convert via `From<AuthStatusRaw>`.

// TODO(typify): Revisit with a newer typify version — this workaround can be
// removed once typify handles untagged enums with boolean discriminants.
// Ref: https://github.com/oxidecomputer/typify/issues/498
#[derive(Deserialize)]
struct AuthStatusRaw {
    verified: bool,
    user_id: Option<uuid::Uuid>,
    address: Option<GetCliAuthStatusResponseVariant1Address>,
    email: Option<String>,
    username: Option<GetCliAuthStatusResponseVariant1Username>,
    token: Option<String>,
    refresh_token: Option<String>,
}

impl From<AuthStatusRaw> for GetCliAuthStatusResponse {
    fn from(raw: AuthStatusRaw) -> Self {
        match (raw.verified, raw.token, raw.refresh_token, raw.user_id) {
            // All required Variant1 fields present
            (true, Some(token), Some(refresh_token), Some(user_id)) => {
                Self::Variant1 {
                    verified: true,
                    token,
                    refresh_token,
                    user_id,
                    address: raw.address,
                    email: raw.email,
                    username: raw.username,
                }
            }
            // Otherwise it's a not-verified response
            _ => {
                Self::Variant0 {
                    verified: raw.verified,
                }
            }
        }
    }
}

/// Server error response body (used for non-200 status codes).
#[derive(Deserialize)]
struct ApiErrorBody {
    error: String,
}

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
            let status = Self::check_auth_status(client, auth_response).await?;

            if let GetCliAuthStatusResponse::Variant1 {
                token,
                refresh_token,
                user_id,
                address,
                email,
                ..
            } = status
            {
                spinner.finish_with_message("✅ Authentication successful!");
                let wallet_address = address.and_then(|a| a.to_string().parse::<Address>().ok());
                config.auth = Some(UserAuth {
                    access_token: token,
                    refresh_token,
                    expires_at: auth_response.expires_at,
                    user_id: Some(user_id),
                    wallet_address,
                    email,
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

    /// Fetch auth status via raw request, bypassing the generated client.
    ///
    /// NOTE: The generated untagged enum always matches Variant0 (serde ignores extra
    /// fields), and the server treats verified sessions as single-use — so a
    /// fallback re-fetch would get 400. Single raw request + `AuthStatusRaw`
    /// avoids both issues.
    async fn check_auth_status(
        client: &GeneratedClient,
        auth_response: &GetCliAuthCodeResponse,
    ) -> Result<GetCliAuthStatusResponse, AuthError> {
        let url = format!("{}/cli/auth/status", client.baseurl());
        let response = client
            .client()
            .get(&url)
            .timeout(AUTH_STATUS_TIMEOUT)
            .query(&[
                ("session_id", auth_response.session_id.to_string()),
                ("device_secret", auth_response.device_secret.clone()),
            ])
            .send()
            .await
            .map_err(|e| AuthError::StatusRequestFailed(e.to_string()))?;

        if !response.status().is_success() {
            let msg = response
                .json::<ApiErrorBody>()
                .await
                .map_or_else(|_| "Unknown error".to_string(), |b| b.error);
            return Err(AuthError::InvalidSession(msg));
        }

        let raw: AuthStatusRaw = response
            .json()
            .await
            .map_err(|e| AuthError::StatusRequestFailed(e.to_string()))?;

        Ok(raw.into())
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
        match result.unwrap() {
            GetCliAuthStatusResponse::Variant1 {
                token,
                refresh_token,
                address,
                ..
            } => {
                assert_eq!(token, "test_token");
                assert_eq!(refresh_token, "test_refresh");
                assert_eq!(
                    &*address.unwrap(),
                    "0x1234567890123456789012345678901234567890"
                );
            }
            other @ GetCliAuthStatusResponse::Variant0 { .. } => {
                panic!("Expected Variant1, got {other:?}")
            }
        }
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
        assert!(matches!(
            result.unwrap(),
            GetCliAuthStatusResponse::Variant0 { verified: false }
        ));
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
        match result.unwrap() {
            GetCliAuthStatusResponse::Variant1 { token, address, .. } => {
                assert_eq!(token, "test_token");
                assert!(address.is_none());
            }
            other @ GetCliAuthStatusResponse::Variant0 { .. } => {
                panic!("Expected Variant1, got {other:?}")
            }
        }
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
    async fn test_check_auth_status_verified_missing_required_fields_falls_back() {
        let mut server = Server::new_async().await;

        // verified:true but missing required Variant1 fields → AuthStatusRaw → Variant0
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
        assert!(matches!(
            result.unwrap(),
            GetCliAuthStatusResponse::Variant0 { verified: true }
        ));
        mock.assert();
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
