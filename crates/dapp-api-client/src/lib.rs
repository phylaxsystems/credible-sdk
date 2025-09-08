//! API client for interacting with dapp services
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]

pub mod auth;
pub mod client;
pub mod config;
pub mod error;
pub mod generated;

pub use auth::{
    Auth,
    AuthConfig,
};
pub use client::Client;
pub use config::{
    Config,
    Environment,
};
pub use error::{
    Error,
    Result,
};
