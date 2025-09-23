#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::unreadable_literal)]

pub mod assertion_da;
pub mod assertion_submission;
pub mod auth;
pub mod config;
pub mod error;

/// Default dapp url. URL suffixes added on demand.
pub const DEFAULT_DAPP_URL: &str = "https://dapp.internal.phylax.systems";

/// Default da url. URL suffixes added on demand.
pub const DEFAULT_DA_URL: &str = "https://da.linea-internal.phylax.systems";

/// Build a URL by appending a path segment to the default dapp URL.
pub fn default_dapp_url_with(path: &str) -> String {
    let base = DEFAULT_DAPP_URL.trim_end_matches('/');
    let trimmed_path = path.trim_start_matches('/');

    if trimmed_path.is_empty() {
        base.to_string()
    } else {
        format!("{base}/{trimmed_path}")
    }
}

// TODO(0xgregthedev): Add project module once tested, polished, added to cli, and ready for release.
//pub mod project;
