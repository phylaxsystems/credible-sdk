pub mod api;
mod config;
mod encode_args;
mod server;

pub use config::Config;
pub use server::DaServer;

/// Leaf fanout for sled.
pub const LEAF_FANOUT: usize = 1024;
