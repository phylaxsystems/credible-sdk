//! State worker library — exposes internals for use by the embedded sidecar integration.
//!
//! This crate is primarily a binary (`state-worker`), but exposes a library target so the
//! sidecar can construct a `StateWorker` in-process without duplicating the tracing logic.
//!
//! Only the modules required by the sidecar embedding are re-exported here.
#[macro_use]
extern crate credible_utils;

pub mod genesis;
pub mod metrics;
pub mod state;
pub mod system_calls;
pub mod worker;
