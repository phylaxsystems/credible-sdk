#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]

#[macro_use]
extern crate credible_utils;

pub mod cli;
pub mod embedded;
pub mod error;
pub mod genesis;
pub mod geth_version;
#[cfg(test)]
mod integration_tests;
pub mod metrics;
pub mod service;
pub mod state;
pub mod system_calls;
pub mod worker;

pub use service::connect_provider;
