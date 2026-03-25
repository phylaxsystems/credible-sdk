#![doc = include_str!("../README.md")]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::similar_names)]
#![allow(clippy::struct_field_names)]
#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]
#![allow(unused)]
#[macro_use]
extern crate credible_utils;
extern crate core;

pub mod args;
pub mod cache;
pub mod config;
pub mod da_reachability;
pub mod engine;
pub mod event_sequencing;
pub mod execution_ids;
pub mod graphql_event_source;
pub mod health;
pub mod indexer;
pub mod metrics;
pub mod transaction_observer;
pub mod transactions_state;
pub mod transport;
pub mod state_worker_thread;
pub mod utils;

pub use credible_utils::critical;

pub use cache::{
    Sources,
    sources::{
        Source,
        SourceError,
    },
};
pub use engine::{
    CoreEngine,
    CoreEngineConfig,
    EngineInspectorProvider,
    RecordedInspectorResult,
    RecordingInspectorProvider,
};
pub use transactions_state::TransactionsState;
