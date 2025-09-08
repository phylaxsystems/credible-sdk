//! # The credible layer sidecar
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::similar_names)]

mod args;
mod cache;
mod config;
pub mod engine;
mod indexer;
pub(crate) mod transactions_state;
pub mod transport;
mod utils;
mod metrics;

use crate::{
    config::{
        init_assertion_store,
        init_executor_config,
        init_indexer_config,
    },
    engine::CoreEngine,
    transport::Transport,
};
use assertion_executor::{
    AssertionExecutor,
    db::overlay::OverlayDb,
};
use crossbeam::channel::unbounded;
use std::sync::Arc;

use clap::Parser;
use rust_tracing::trace;

use crate::{
    cache::{
        Cache,
        sources::sequencer::Sequencer,
    },
    transactions_state::TransactionsState,
    transport::http::{
        HttpTransport,
        config::HttpTransportConfig,
    },
};
use args::SidecarArgs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    trace();

    let args = SidecarArgs::parse();

    let (tx_sender, tx_receiver) = unbounded();

    let sequencer = Arc::new(Sequencer::try_new(&args.chain.rpc_url).await?);
    let cache = Arc::new(Cache::new(vec![sequencer]));
    let state: OverlayDb<Cache> = OverlayDb::new(
        Some(cache.clone()),
        args.credible
            .overlay_cache_capacity_bytes
            .unwrap_or(1024 * 1024 * 1024) as u64,
    );

    let executor_config = init_executor_config(&args);
    let assertion_store = init_assertion_store(&args)?;
    let assertion_executor =
        AssertionExecutor::new(executor_config.clone(), assertion_store.clone());
    let indexer_cfg = init_indexer_config(&args, assertion_store, executor_config).await?;

    let engine_state_results = TransactionsState::new();
    let transport = HttpTransport::new(
        HttpTransportConfig::try_from(args.transport.clone())?,
        tx_sender,
        engine_state_results.clone(),
    )?;

    let mut engine = CoreEngine::new(
        state,
        cache,
        tx_receiver,
        assertion_executor,
        engine_state_results.clone(),
        args.credible.transaction_results_max_capacity,
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down...");
        }
        result = engine.run() => {
            if let Err(e) = result {
                tracing::error!("Engine exited with error: {}", e);
            }
            tracing::info!("Engine run completed, shutting down...");
        }
        result = transport.run() => {
            if let Err(e) = result {
                tracing::error!("Transport exited with error: {}", e);
            }
            tracing::info!("Engine run completed, shutting down...");
        }
        result = indexer::run_indexer(indexer_cfg) => {
            if let Err(e) = result {
                tracing::error!("Indexer exited with error: {}", e);
            }
            tracing::info!("Indexer exited, shutting down...");
        }
    }

    tracing::info!("Sidecar shutdown complete.");
    Ok(())
}
