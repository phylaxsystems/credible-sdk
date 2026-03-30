#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::unreadable_literal)]

pub mod apply;
pub mod auth;
pub mod config;
pub mod credible_config;
pub mod error;
pub mod verify;

/// Default platform url. URL suffixes added on demand.
pub const DEFAULT_PLATFORM_URL: &str = "https://app.phylax.systems";
