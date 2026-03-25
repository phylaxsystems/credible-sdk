use pcl_phoundry::error::PhoundryError;
use reqwest::Error as ReqwestError;
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

    #[error("Request to {endpoint} failed: {source}")]
    Network {
        endpoint: String,
        #[source]
        source: ReqwestError,
    },

    #[error("API request to {endpoint} failed with status {status}: {body}")]
    Api {
        endpoint: String,
        status: u16,
        body: String,
    },

    #[error("Apply cancelled")]
    ApplyCancelled,

    #[error("JSON mode with pending changes requires `--yes`")]
    JsonConfirmationRequiresYes,

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
    /// Error when HTTP request to the auth service fails
    #[error(
        "Authentication request failed. Please check your connection and try again.\nError: {0}"
    )]
    AuthRequestFailed(#[source] reqwest::Error),

    /// Error when HTTP request to the auth service fails
    #[error(
        "Invalid authentication response. Please check your connection and try again.\nError: {0}"
    )]
    AuthRequestInvalidResponse(#[source] reqwest::Error),

    /// Error when HTTP request to the auth service fails
    #[error(
        "Authentication status request failed. Please check your connection and try again.\nError: {0}"
    )]
    StatusRequestFailed(#[source] reqwest::Error),

    /// Error when HTTP request to the auth service fails
    #[error(
        "Invalid authentication status response. Please check your connection and try again.\nError: {0}"
    )]
    StatusRequestInvalidResponse(#[source] reqwest::Error),

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
