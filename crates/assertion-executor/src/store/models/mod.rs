//! Persistent data models for the assertion store.
//!
//! WARNING: Structs in this module are serialized to sled via bincode.
//! Any field change requires a new migration in `store::migration`.

mod assertion_state;
pub use assertion_state::AssertionState;
