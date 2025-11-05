//! Contains helpers to monitor and instrument the cache and sources.
//!
//! Holds code responsible for:
//! - Validating cache correctness by comparing engine traces against the
//!   canonical client (`cache` module, enabled with the `cache_validation` feature).
//! - Tracking synchronization health for every configured state source and
//!   surfacing Prometheus metrics consumed by the sidecar (`sources` module).
//!
//! These helpers run alongside the engine so transports and cache sources can
//! quickly detect divergence or stale data and surface actionable telemetry.

#[cfg(feature = "cache_validation")]
pub mod cache;
pub mod sources;
