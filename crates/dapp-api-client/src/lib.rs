//! API client for interacting with dapp services

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
