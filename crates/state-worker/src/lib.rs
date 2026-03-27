#![recursion_limit = "1024"]
#![doc = include_str!("../README.md")]
// These items were already `pub` in their modules; re-exporting them from the
// library crate triggers pedantic lints that are out of scope for this
// structural extraction.  Suppress until a dedicated doc/lint cleanup pass.
#![allow(clippy::missing_errors_doc, clippy::must_use_candidate)]

#[macro_use]
extern crate credible_utils;

mod genesis;
pub mod metrics;
mod state;
mod system_calls;
mod worker;

pub use genesis::{
    GenesisState,
    parse_from_str as parse_genesis_from_str,
};
pub use state::{
    BlockStateUpdateBuilder,
    TraceProvider,
    create_trace_provider,
    geth_provider::GethTraceProvider,
};
pub use system_calls::{
    SystemCallConfig,
    SystemCalls,
};
pub use worker::StateWorker;
