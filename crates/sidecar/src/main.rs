//! # The credible layer sidecar

mod args;
mod config;
#[allow(dead_code)] // TODO: rm when engine fully impld and connected to transport
pub mod engine;
mod indexer;
mod rpc;
pub mod transport;
mod utils;

use crate::{
    config::{
        init_assertion_store,
        init_executor_config,
        init_indexer_config,
    },
    engine::CoreEngine,
    transport::{
        Transport,
        mock::MockTransport,
    },
};
use assertion_executor::{
    AssertionExecutor,
    db::overlay::OverlayDb,
};
use crossbeam::channel::unbounded;
use std::convert::Infallible;

use revm::database::{
    CacheDB,
    EmptyDBTyped,
};

use clap::Parser;
use rust_tracing::trace;

use crate::engine::StateResults;
use args::SidecarArgs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    trace();

    let args = SidecarArgs::parse();

    let (tx_sender, tx_receiver) = unbounded();
    let (get_tx_result_sender, get_tx_result_receiver) = unbounded();
    let state: OverlayDb<CacheDB<EmptyDBTyped<Infallible>>> = OverlayDb::new(
        None,
        args.credible
            .overlay_cache_capacity_bytes
            .unwrap_or(1024 * 1024 * 1024) as u64,
    );

    let executor_config = init_executor_config(&args);
    let assertion_store = init_assertion_store(&args)?;
    let assertion_executor =
        AssertionExecutor::new(executor_config.clone(), assertion_store.clone());
    let indexer_cfg = init_indexer_config(&args, assertion_store, executor_config).await?;

    let (_, mock_receiver) = unbounded();
    let mock_transport =
        MockTransport::with_receiver(tx_sender, mock_receiver, get_tx_result_sender);

    let engine_state_results = StateResults::new();
    let mut engine = CoreEngine::new(
        state,
        tx_receiver,
        assertion_executor,
        engine_state_results.clone(),
    );
    let mut engine_state = engine::result_handler::ResultHandler::new(get_tx_result_receiver, engine_state_results);

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
        _ = engine_state.run() => {
            tracing::info!("Engine state run completed, shutting down...");
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
