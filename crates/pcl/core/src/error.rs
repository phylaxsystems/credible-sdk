use crate::credible_config::CredibleConfigError;
use dapp_api_client::generated::client::{
    Error as ApiError,
    types::GetCliAuthStatusResponse,
};
use pcl_phoundry::error::PhoundryError;
use serde::Deserialize;
use thiserror::Error;

/// Errors that can occur during declarative apply.
#[derive(Error, Debug)]
pub enum ApplyError {
    #[error("Run `pcl auth login` first")]
    NoAuthToken,

    #[error("{message}: {source}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to parse credible.toml: {0}")]
    Toml(#[source] toml::de::Error),

    #[error("Invalid credible.toml: {0}")]
    InvalidConfig(String),

    #[error("Project selection failed: {0}")]
    ProjectSelectionFailed(#[source] inquire::InquireError),

    #[error("No projects found for the authenticated user")]
    NoProjectsFound,

    #[error("Build failed: {0}")]
    BuildFailed(#[source] Box<PhoundryError>),

    #[error("API request to {endpoint} failed{}: {body}", status.map_or(String::new(), |s| format!(" with status {s}")))]
    Api {
        endpoint: String,
        status: Option<u16>,
        body: String,
    },

    #[error("{0}")]
    VerificationFailed(String),

    #[error("Apply cancelled")]
    ApplyCancelled,

    #[error("JSON mode with pending changes requires `--yes`")]
    JsonConfirmationRequiresYes,

    #[error("Failed to encode JSON output: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<CredibleConfigError> for ApplyError {
    fn from(e: CredibleConfigError) -> Self {
        Self::InvalidConfig(e.to_string())
    }
}

/// Errors that can occur during assertion verification.
#[derive(Error, Debug)]
pub enum VerifyError {
    #[error(transparent)]
    Config(#[from] CredibleConfigError),

    #[error("{message}: {source}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Build failed: {0}")]
    BuildFailed(#[source] Box<PhoundryError>),

    #[error("Failed to encode constructor arguments: {0}")]
    AbiEncode(String),

    #[error("Failed to encode JSON output: {0}")]
    Json(#[from] serde_json::Error),
}

/// Errors that can occur during configuration operations
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Error when reading the config file from ~/.config/pcl/config.toml fails
    #[error("Failed to read config file: {0}")]
    ReadError(std::io::Error),

    /// Error when writing to the config file at ~/.config/pcl/config.toml fails
    #[error("Failed to write config file: {0}")]
    WriteError(std::io::Error),

    /// Error when deserializing the config file fails
    #[error("Failed to parse config file: {0}")]
    ParseError(#[source] toml::de::Error),

    /// Error when serializing the config file fails
    #[error("Failed to serialize config file: {0}")]
    SerializeError(#[source] toml::ser::Error),

    /// Error when attempting an operation that requires authentication
    /// but no authentication token is present in the config
    #[error("No Authentication Token Found")]
    NotAuthenticated,
}

/// Errors that can occur during authentication operations
#[derive(Error, Debug)]
pub enum AuthError {
    /// Error when the auth code request fails
    #[error(
        "Authentication request failed. Please check your connection and try again.\nError: {0}"
    )]
    AuthRequestFailed(String),

    /// Error when the auth status check fails due to network/transport issues
    #[error(
        "Authentication status request failed. Please check your connection and try again.\nError: {0}"
    )]
    StatusRequestFailed(String),

    /// Error when the auth session is no longer valid
    #[error("Invalid session: {0}. Please run `pcl auth login` again.")]
    InvalidSession(String),

    /// Error when the session has expired server-side
    #[error("Session expired. Please run `pcl auth login` to start a new session.")]
    SessionExpired,

    /// Error when the session is not found (bad `session_id` or `device_secret`)
    #[error("Session not found. Please run `pcl auth login` to start a new session.")]
    SessionNotFound,

    /// Error when the user is not found in the platform
    #[error("User not found. Please ensure your account exists on the Credible Layer Platform.")]
    UserNotFound,

    /// Error when the server encounters an internal error
    #[error("Server error. Please try again later.\nDetails: {0}")]
    ServerError(String),

    /// Error when authentication times out
    #[error(
        "Authentication timed out after {0} attempts. Please try again and approve the wallet connection promptly."
    )]
    Timeout(u32),

    /// Error when authentication verification fails
    #[error("Authentication failed: {0}")]
    InvalidAuthData(String),

    /// Error when config operations fail during auth
    #[error("Config error: {0}")]
    ConfigError(#[source] ConfigError),
}

/// API error response body with structured error code.
#[derive(Deserialize)]
struct ApiErrorBody {
    error: String,
    code: Option<DappErrorCode>,
}

/// Structured error codes returned by the dapp CLI auth status endpoint.
///
/// Only codes that drive distinct polling behavior are listed here.
#[derive(Deserialize, Debug, PartialEq, Eq)]
enum DappErrorCode {
    #[serde(rename = "SESSION_EXPIRED")]
    SessionExpired,
    #[serde(rename = "SESSION_NOT_FOUND")]
    SessionNotFound,
    #[serde(rename = "USER_NOT_FOUND")]
    UserNotFound,
    #[serde(rename = "INTERNAL_ERROR")]
    InternalError,
    /// Catch-all for codes that don't need distinct handling.
    #[serde(other)]
    Other,
}

impl From<ApiError<GetCliAuthStatusResponse>> for AuthError {
    /// Convert a progenitor API error into a typed `AuthError`.
    ///
    /// The generated client returns `InvalidResponsePayload` for 400/500
    /// responses because the error body `{ error, code }` doesn't match the
    /// success type. We extract the raw bytes, parse the error code, and map
    /// to the appropriate variant.
    fn from(err: ApiError<GetCliAuthStatusResponse>) -> Self {
        // InvalidResponsePayload carries the raw bytes — parse them.
        if let ApiError::InvalidResponsePayload(bytes, _) = &err
            && let Ok(body) = serde_json::from_slice::<ApiErrorBody>(bytes)
        {
            return match body.code {
                Some(DappErrorCode::SessionExpired) => Self::SessionExpired,
                Some(DappErrorCode::SessionNotFound) => Self::SessionNotFound,
                Some(DappErrorCode::UserNotFound) => Self::UserNotFound,
                Some(DappErrorCode::InternalError) => Self::ServerError(body.error),
                Some(DappErrorCode::Other) | None => Self::InvalidSession(body.error),
            };
        }

        // ErrorResponse means progenitor managed to deserialize — shouldn't
        // happen for our error shapes, but handle gracefully.
        if let ApiError::ErrorResponse(ref rv) = err {
            if let Some(status) = err.status()
                && status.is_server_error()
            {
                return Self::ServerError(format!("HTTP {status}"));
            }
            return Self::InvalidSession(format!("{rv:?}"));
        }

        // Network / transport failures
        Self::StatusRequestFailed(err.to_string())
    }
}
