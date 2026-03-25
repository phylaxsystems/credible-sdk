#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]

#[macro_use]
extern crate credible_utils;

pub mod genesis;
pub mod geth_version;
#[cfg(test)]
mod integration_tests;
pub mod metrics;
pub mod service;
pub mod state;
pub mod system_calls;
pub mod worker;

pub use service::{
    DEFAULT_TRACE_TIMEOUT,
    StateWorkerCommand,
    StateWorkerConfig,
    StateWorkerMode,
    StateWorkerServiceError,
    connect_provider,
    load_genesis_state,
    run_state_worker_once,
    validate_geth_version,
};
pub use worker::{
    StateWorker,
    StateWorkerError,
    UNINITIALIZED_COMMITTED_HEIGHT,
};
