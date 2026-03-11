use crate::{
    DEFAULT_DAPP_URL,
    config::{
        CliConfig,
        UserAuth,
    },
    error::AuthError,
};
use alloy_primitives::Address;
use chrono::{
    DateTime,
    Utc,
};
use color_eyre::Result;
use colored::Colorize;
use indicatif::{
    ProgressBar,
    ProgressStyle,
};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::{
    Duration,
    sleep,
};
use uuid::Uuid;

/// Interval between authentication status checks
const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Maximum number of retry attempts (5 minutes worth of 2-second intervals)
const MAX_RETRIES: u32 = 150;

/// Response from the initial authentication request
#[derive(Deserialize)]
struct AuthResponse {
    code: String,
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(rename = "deviceSecret")]
    device_secret: String,
    #[serde(rename = "expiresAt")]
    expires_at: DateTime<Utc>,
}

/// Response from the authentication status check
#[derive(Deserialize, Debug)]
struct AuthStatusResponse {
    verified: bool,
    user_id: Option<Uuid>,
    address: Option<String>,
    email: Option<String>,
    token: Option<String>,
    refresh_token: Option<String>,
}

/// Authentication commands for the PCL CLI
#[derive(clap::Parser)]
#[command(about = "Authenticate the CLI with your Credible Layer dApp account")]
pub struct AuthCommand {
    #[command(subcommand)]
    pub command: AuthSubcommands,

    #[arg(
        short = 'u',
        long = "auth-url",
        env = "PCL_AUTH_URL",
        default_value = DEFAULT_DAPP_URL,
        help = "Base URL for authentication service"
    )]
    pub auth_url: String,
}

/// Available authentication subcommands
#[derive(clap::Subcommand)]
#[command(about = "Authentication operations")]
pub enum AuthSubcommands {
    /// Login to PCL using your wallet
    #[command(
        long_about = "Initiates the login process. Opens a browser window for wallet authentication.",
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
        long_about = "Displays whether you're currently logged in and shows the connected wallet address if authenticated.",
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

        let auth_response = self.request_auth_code().await?;
        self.display_login_instructions(&auth_response);
        self.wait_for_verification(config, &auth_response).await
    }

    /// Request an authentication code from the server
    async fn request_auth_code(&self) -> Result<AuthResponse, AuthError> {
        let client = Client::new();
        let url = format!("{}/api/v1/cli/auth/code", self.auth_url);
        client
            .get(url)
            .send()
            .await
            .map_err(AuthError::AuthRequestFailed)?
            .json()
            .await
            .map_err(AuthError::AuthRequestInvalidResponse)
    }

    /// Display login URL and code to the user, attempting to open the browser automatically
    fn display_login_instructions(&self, auth_response: &AuthResponse) {
        let url = format!(
            "{}/device?session_id={}",
            self.auth_url, auth_response.session_id
        );

        // Try to open the URL in the default browser
        if open::that(&url).is_ok() {
            println!(
                "\n{} Opening browser for authentication...\n\n🔗 {}\n📝 {}\n",
                "🌐".green(),
                url.white(),
                format!("Code: {}", auth_response.code).green().bold()
            );
        } else {
            println!(
                "\nTo authenticate, please visit:\n\n🔗 {}\n📝 {}\n",
                url.white(),
                format!("Code: {}", auth_response.code).green().bold()
            );
        }
    }

    async fn wait_for_verification(
        &self,
        config: &mut CliConfig,
        auth_response: &AuthResponse,
    ) -> Result<(), AuthError> {
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template("{spinner} {msg}")
                .expect("Failed to set spinner style"),
        );
        spinner.enable_steady_tick(Duration::from_millis(80));
        spinner.set_message("Waiting for authentication...");

        let client = Client::new();

        for _ in 0..MAX_RETRIES {
            let status = self.check_auth_status(&client, auth_response).await?;

            if status.verified {
                spinner.finish_with_message("✅ Authentication successful!");
                Self::update_config(config, status, auth_response)?;
                Self::display_success_message(config);
                return Ok(());
            }

            spinner.tick();
            sleep(POLL_INTERVAL).await;
        }

        spinner.finish_with_message("❌ Authentication timed out");
        Err(AuthError::Timeout(MAX_RETRIES))
    }

    /// Check the current authentication status
    async fn check_auth_status(
        &self,
        client: &Client,
        auth_response: &AuthResponse,
    ) -> Result<AuthStatusResponse, AuthError> {
        let url = format!("{}/api/v1/cli/auth/status", self.auth_url);
        client
            .get(url)
            .query(&[
                ("session_id", &auth_response.session_id),
                ("device_secret", &auth_response.device_secret),
            ])
            .send()
            .await
            .map_err(AuthError::StatusRequestFailed)?
            .json()
            .await
            .map_err(AuthError::StatusRequestInvalidResponse)
    }

    /// Update the configuration with authentication data
    fn update_config(
        config: &mut CliConfig,
        status: AuthStatusResponse,
        auth_response: &AuthResponse,
    ) -> Result<(), AuthError> {
        let user_address = status
            .address
            .and_then(|a| a.parse::<Address>().ok())
            .unwrap_or(Address::ZERO);

        config.auth = Some(UserAuth {
            access_token: status
                .token
                .ok_or(AuthError::InvalidAuthData("Missing token".to_string()))?,
            refresh_token: status.refresh_token.ok_or(AuthError::InvalidAuthData(
                "Missing refresh token".to_string(),
            ))?,
            user_address,
            expires_at: auth_response.expires_at,
            user_id: status.user_id,
            email: status.email,
        });
        Ok(())
    }

    /// Display success message after authentication
    fn display_success_message(config: &CliConfig) {
        let auth = config.auth.as_ref().unwrap();
        println!(
            "{}\n🔗 {}\n",
            "Authentication successful! 🎉".green().bold(),
            format!("Connected as: {}", auth.display_name()).white()
        );
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
    use chrono::TimeZone;
    use clap::Parser;
    use mockito::Server;

    fn create_test_config() -> CliConfig {
        CliConfig {
            auth: Some(UserAuth {
                access_token: "test_token".to_string(),
                refresh_token: "test_refresh".to_string(),
                user_address: "0x1234567890123456789012345678901234567890"
                    .parse()
                    .unwrap(),
                expires_at: Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap(),
                user_id: Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
                email: None,
            }),
            ..Default::default()
        }
    }

    fn create_test_auth_response() -> AuthResponse {
        AuthResponse {
            code: "123456".to_string(),
            session_id: "test_session".to_string(),
            device_secret: "test_secret".to_string(),
            expires_at: Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap(),
        }
    }

    fn create_test_status_response() -> AuthStatusResponse {
        AuthStatusResponse {
            verified: true,
            user_id: Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
            address: Some("0x1234567890123456789012345678901234567890".to_string()),
            email: None,
            token: Some("test_token".to_string()),
            refresh_token: Some("test_refresh".to_string()),
        }
    }

    #[test]
    fn test_display_login_instructions() {
        let cmd = AuthCommand {
            command: AuthSubcommands::Login,
            auth_url: "https://dapp.phylax.systems".to_string(),
        };
        let auth_response = create_test_auth_response();

        // Can't easily test stdout, but we can verify it doesn't panic
        cmd.display_login_instructions(&auth_response);
    }

    #[test]
    fn test_update_config() {
        let mut config = CliConfig::default();
        let auth_response = create_test_auth_response();
        let status = create_test_status_response();

        let result = AuthCommand::update_config(&mut config, status, &auth_response);

        if let Err(e) = &result {
            println!("Error: {e:?}");
        }
        assert!(result.is_ok());
        assert!(config.auth.is_some());
        let auth = config.auth.as_ref().unwrap();
        assert_eq!(
            auth.user_address,
            "0x1234567890123456789012345678901234567890"
                .parse::<Address>()
                .unwrap()
        );
        assert_eq!(auth.access_token, "test_token");
        assert_eq!(auth.refresh_token, "test_refresh");
        assert_eq!(
            auth.user_id,
            Some(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap())
        );
        assert_eq!(
            auth.expires_at,
            Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn test_display_success_message() {
        let config = create_test_config();

        // Can't easily test stdout, but we can verify it doesn't panic
        AuthCommand::display_success_message(&config);
    }

    #[tokio::test]
    async fn test_request_auth_code() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/code")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"code":"123456","sessionId":"test_session","deviceSecret":"test_secret","expiresAt":"2024-12-31"}"#)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();

        let result = cmd.request_auth_code().await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.code, "123456");
        assert_eq!(response.session_id, "test_session");
        mock.assert();
    }

    #[tokio::test]
    async fn test_check_auth_status() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/api/v1/cli/auth/status")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("session_id".into(), "test_session".into()),
                mockito::Matcher::UrlEncoded("device_secret".into(), "test_secret".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"verified":true,"user_id":"550e8400-e29b-41d4-a716-446655440000","address":"0xtest","token":"test_token","refresh_token":"test_refresh"}"#)
            .create();

        let cmd = AuthCommand::try_parse_from(vec!["auth", "--auth-url", &server.url(), "login"])
            .unwrap();

        let client = Client::new();
        let auth_response = create_test_auth_response();

        let result = cmd.check_auth_status(&client, &auth_response).await;

        assert!(result.is_ok());
        let status = result.unwrap();
        assert!(status.verified);
        assert_eq!(status.address.unwrap(), "0xtest");
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

    #[test]
    fn test_update_config_with_invalid_address_falls_back_to_zero() {
        let mut config = CliConfig::default();
        let auth_response = create_test_auth_response();
        let mut status = create_test_status_response();
        status.address = Some("invalid_address".to_string());

        let result = AuthCommand::update_config(&mut config, status, &auth_response);
        assert!(result.is_ok());
        assert_eq!(config.auth.as_ref().unwrap().user_address, Address::ZERO);
    }

    #[test]
    fn test_update_config_with_missing_token() {
        let mut config = CliConfig::default();
        let auth_response = create_test_auth_response();
        let mut status = create_test_status_response();
        status.token = None;

        let result = AuthCommand::update_config(&mut config, status, &auth_response);
        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::InvalidAuthData(_))));
    }

    #[test]
    fn test_update_config_with_missing_refresh_token() {
        let mut config = CliConfig::default();
        let auth_response = create_test_auth_response();
        let mut status = create_test_status_response();
        status.refresh_token = None;

        let result = AuthCommand::update_config(&mut config, status, &auth_response);
        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::InvalidAuthData(_))));
    }

    #[test]
    fn test_update_config_with_missing_address_uses_zero() {
        let mut config = CliConfig::default();
        let auth_response = create_test_auth_response();
        let mut status = create_test_status_response();
        status.address = None;

        let result = AuthCommand::update_config(&mut config, status, &auth_response);
        assert!(result.is_ok());
        assert_eq!(config.auth.as_ref().unwrap().user_address, Address::ZERO);
    }

    #[tokio::test]
    async fn test_login_when_already_authenticated() {
        let mut config = create_test_config();
        let cmd = AuthCommand::try_parse_from(vec![
            "auth",
            "--auth-url",
            "https://dapp.phylax.systems",
            "login",
        ])
        .unwrap();

        let result = cmd.login(&mut config).await;
        assert!(result.is_ok());
        assert_eq!(
            config.auth.as_ref().unwrap().user_address,
            "0x1234567890123456789012345678901234567890"
                .parse::<Address>()
                .unwrap()
        );
    }
}
