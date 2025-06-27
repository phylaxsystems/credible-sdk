use color_eyre::{
    Report,
    eyre,
};
use foundry_compilers::{
    error::SolcError,
    flatten::FlattenerError,
};
use std::{
    fmt::Debug,
    path::PathBuf,
};
use thiserror::Error;

use foundry_config::error::ExtractConfigError;

#[derive(Error, Debug)]
pub enum PhoundryError {
    #[error("forge is not installed or not available in PATH")]
    ForgeNotInstalled,
    #[error("forge command failed")]
    ForgeCommandFailed(#[from] color_eyre::Report),
    #[error("invalid forge output: {0}")]
    InvalidForgeOutput(&'static str),
    #[error("invalid forge command: {0}")]
    InvalidForgeCommand(String),
    #[error("Phoundry profile {0} was not found in config {1}")]
    InvalidFoundryProfile(String, PathBuf),
    #[error("Phoundry failed to extract the config: {0}")]
    FoundryConfigError(#[from] foundry_config::error::ExtractConfigError),
    #[error("Contract {0} was not found in the build output")]
    ContractNotFound(String),
    #[error("Invalid path: {0:?}")]
    InvalidPath(PathBuf),
    #[error("Directory not found: {0:?}")]
    DirectoryNotFound(PathBuf),
    #[error("Solc error: {0}")]
    SolcError(#[from] SolcError),
    #[error("Failed to canonicalize path: {0:?}")]
    CanonicalizePathError(#[from] std::io::Error),
    #[error("Flattener error: {0}")]
    FlattenerError(#[from] FlattenerError),
    #[error("No source files found in specified build paths.")]
    NoSourceFilesFound,
    #[error("Compilation failed:\n{0}")]
    CompilationError(eyre::Report),
}

impl From<ExtractConfigError> for Box<PhoundryError> {
    fn from(error: ExtractConfigError) -> Self {
        Box::new(PhoundryError::FoundryConfigError(error))
    }
}

impl From<std::io::Error> for Box<PhoundryError> {
    fn from(error: std::io::Error) -> Self {
        Box::new(PhoundryError::from(error))
    }
}

impl From<Report> for Box<PhoundryError> {
    fn from(error: Report) -> Self {
        Box::new(PhoundryError::ForgeCommandFailed(error))
    }
}
