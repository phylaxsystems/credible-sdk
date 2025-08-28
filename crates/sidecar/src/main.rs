//! # The credible layer sidecar

mod args;
mod config;
#[allow(dead_code)] // TODO: rm when engine fully impld and connected to transport
pub mod engine;
mod indexer;
mod json_rpc_db;
mod rpc;
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

use clap::Parser;
use rust_tracing::trace;

use crate::{
    json_rpc_db::JsonRpcDb,
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
    let json_rpc_db = JsonRpcDb::try_new(&args.rollup.rpc_url).await?;
    let state: OverlayDb<JsonRpcDb> = OverlayDb::new(
        Some(json_rpc_db),
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
    let mock_transport = HttpTransport::new(
        HttpTransportConfig::try_from(args.transport.clone())?,
        tx_sender,
        engine_state_results.clone(),
    )?;

    let mut engine = CoreEngine::new(
        state,
        tx_receiver,
        assertion_executor,
        engine_state_results.clone(),
        args.credible.transaction_results_max_capacity,
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down...");
        }
        _ = rpc::start_rpc_server(&args) => {
            tracing::info!("rpc server exited, shutting down...");
        }
        _ = engine.run() => {
            tracing::info!("Engine run completed, shutting down...");
        }
        _ = mock_transport.run() => {
            tracing::info!("Engine run completed, shutting down...");
        }
        _ = indexer::run_indexer(indexer_cfg) => {
            tracing::info!("Indexer exited, shutting down...");
        }
    }

    tracing::info!("Sidecar shutdown complete.");
    Ok(())
}
