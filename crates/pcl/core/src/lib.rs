#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::unreadable_literal)]

pub mod assertion_da;
pub mod assertion_submission;
pub mod auth;
pub mod config;
pub mod error;

/// Macro that defines the default DA URL - can be used in concat! macros
#[macro_export]
macro_rules! default_dapp_url {
    () => {
        "https://dapp.internal.phylax.systems"
    };
}

/// Macro that defines the default DA URL - can be used in concat! macros
#[macro_export]
macro_rules! default_da_url {
    () => {
        "https://da.linea-internal.phylax.systems"
    };
}

// TODO(0xgregthedev): Add project module once tested, polished, added to cli, and ready for release.
//pub mod project;
