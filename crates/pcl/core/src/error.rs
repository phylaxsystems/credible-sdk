use assertion_da_client::DaClientError;
use pcl_phoundry::error::PhoundryError;
use reqwest::Error as ReqwestError;
use thiserror::Error;

/// Errors that can occur during assertion submission to the Data Availability (DA) layer
#[derive(Error, Debug)]
pub enum DaSubmitError {
    /// Error when HTTP request to the DA layer fails
    #[error("Da Submission Error: {0}")]
    DaClientError(#[from] DaClientError),
    /// Error during the build process of the assertion
    #[error("There was an error with the solidity file")]
    PhoundryError(#[from] PhoundryError),
    /// Failed to parse bytecode as hex
    #[error("Failed to parse bytecode as hex")]
    ParseError,
    /// From Hex Error
    #[error("From Hex Error: {0}")]
    FromHexError(#[from] alloy_primitives::hex::FromHexError),
    /// HTTP Error with status code
    #[error("HTTP Error: {0}")]
    HttpError(u16),
    /// Invalid Constructor Args
    #[error("Invalid Constructor Args Count: Constructor Signature expects: {0}, Constructor Args submitted: {1};
        Pass args by calling the command in the following format: `pcl store <assertion_contract> <arg0> <arg1>`")]
    InvalidConstructorArgs(usize, usize),
}

impl From<Box<DaSubmitError>> for DaSubmitError {
    fn from(error: Box<DaSubmitError>) -> Self {
        *error
    }
}

/// Errors that can occur during assertion submission to the Credible Layer dApp
#[derive(Error, Debug)]
pub enum DappSubmitError {
    /// Error when no authentication token is found in the config
    #[error("No auth token found, please run `pcl auth` first")]
    NoAuthToken,

    /// Error during project selection process
    #[error("Project selection failed: {0}")]
    ProjectSelectionFailed(#[from] inquire::InquireError),

    /// Error when no projects are found for the authenticated user
    #[error("No projects found for the authenticated user.\nPlease run `pcl project new` or head to https://dapp.phylax.systems to create one.")]
    NoProjectsFound,

    /// Error when connection to the dApp API fails
    #[error("Failed to connect to the dApp API")]
    ApiConnectionError(#[from] ReqwestError),

    /// Error when the submission is rejected by the dApp
    #[error("Submission failed: {0}")]
    SubmissionFailed(String),

    /// Error when could not find stored assertion
    #[error("Could not find stored assertion {0} in the config.\nPlease run `pcl store` first.")]
    CouldNotFindStoredAssertion(String),

    /// Error when no stored assertions are found
    #[error("No stored assertions found.\nPlease run `pcl store` first to store some assertions.")]
    NoStoredAssertions,
}

/// Errors that can occur during configuration operations
#[derive(Error, Debug)]
pub enum ConfigError {
    /// Error when reading the config file from ~/.pcl/config.toml fails
    #[error("Failed to read config file: {0}")]
    ReadError(std::io::Error),

    /// Error when writing to the config file at ~/.pcl/config.toml fails
    #[error("Failed to write config file: {0}")]
    WriteError(std::io::Error),

    /// Error when deserializing the config file fails
    #[error("Failed to parse config file: {0}")]
    ParseError(#[from] toml::de::Error),

    /// Error when serializing the config file fails
    #[error("Failed to serialize config file: {0}")]
    SerializeError(#[from] toml::ser::Error),

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
    RequestFailed(#[from] reqwest::Error),

    /// Error when authentication times out
    #[error("Authentication timed out after {0} attempts. Please try again and approve the wallet connection promptly.")]
    Timeout(u32),

    /// Error when authentication verification fails
    #[error("Authentication failed: {0}")]
    InvalidAuthData(String),

    /// Error when config operations fail during auth
    #[error("Config error: {0}")]
    ConfigError(#[from] ConfigError),

    /// Error when an invalid Ethereum address is received
    #[error(
        "Invalid Ethereum address received. Please ensure you're connecting with a valid wallet."
    )]
    InvalidAddress,

    /// Error when an invalid timestamp format is received
    #[error("Invalid timestamp received from server. Please try again.")]
    InvalidTimestamp,
}
