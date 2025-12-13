//! Credible RPC proxy.
//!
//! This crate exposes two integration surfaces:
//!
//! - [`RpcProxy`], a ready-to-run binary server that listens for JSON-RPC
//!   traffic and applies the heuristics described in `HEURISTICS.md`.
//! - Library helpers (`fingerprint`, [`ProxyConfig`], [`RpcProxyBuilder`]) that
//!   other sequencer integrations can embed directly without running the
//!   standalone proxy or gRPC transport.
//!
//! The implementation builds on top of [`ajj`](https://github.com/init4tech/ajj),
//! the JSON-RPC router we use to share logic between HTTP clients and any
//! in-process integrations.

pub mod backpressure;
pub mod config;
pub mod error;
pub mod fingerprint;
pub mod server;
pub mod sidecar;

pub use config::ProxyConfig;
pub use error::{
    ProxyError,
    Result,
};
pub use server::{
    RpcProxy,
    RpcProxyBuilder,
};
