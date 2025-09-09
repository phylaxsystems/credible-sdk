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
mod metrics;
pub(crate) mod transactions_state;
pub mod transport;
mod utils;

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
    utils::ErrorRecoverability,
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

    loop {
        let indexer_cfg =
            init_indexer_config(&args, assertion_store.clone(), &executor_config).await?;

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received Ctrl+C, shutting down...");
                break;
            }
            result = engine.run() => {
                let Err(e) = result else {
                    continue;
                };

                if ErrorRecoverability::from(&e).is_recoverable() {
                    tracing::error!(error = %e, "Engine exited");
                } else {
                    critical!(error = %e, "Engine exited");
                }
            }
            result = transport.run() => {
                let Err(e) = result else {
                    continue;
                };

                if ErrorRecoverability::from(&e).is_recoverable() {
                    tracing::error!(error = %e, "Transport exited");
                } else {
                    critical!(error = %e, "Transport exited");
                }
            }
            result = indexer::run_indexer(indexer_cfg) => {
                let Err(e) = result else {
                    continue;
                };

                if ErrorRecoverability::from(&e).is_recoverable() {
                    tracing::error!(error = %e, "Indexer exited");
                } else {
                    critical!(error = %e, "Indexer exited");
                }
            }
        }

        tracing::warn!("Sidecar restarted.");
    }

    tracing::info!("Sidecar shutdown complete.");
    Ok(())
}
