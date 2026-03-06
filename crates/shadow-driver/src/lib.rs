//! Public library surface for shared shadow-driver utilities.

/// MDBX persistence primitives and access helpers for shadow-driver payloads.
pub mod mdbx_store;

/// Helpers for converting Alloy transactions and numeric values into sidecar protobuf shapes.
pub mod tx_env;
