//! Common types, configuration, and utilities shared between reader and writer.

pub mod error;
pub(crate) mod tables;
pub mod types;

// Re-export commonly used items from error module
pub use error::{
    StateError,
    StateResult,
};

// Re-export commonly used items from types module
pub use types::{
    BlockMetadata,
    CircularBufferConfig,
    GlobalMetadata,
    NamespacedAccountKey,
    NamespacedBytecodeKey,
    NamespacedStorageKey,
    StorageValue,
};

// Re-export from root crate for convenience
pub use crate::{
    AccountInfo,
    AccountState,
    AddressHash,
    BlockStateUpdate,
};
