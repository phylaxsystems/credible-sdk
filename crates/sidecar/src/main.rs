mod args;
#[allow(dead_code)] // TODO: rm when engine fully impld and connected to transport
pub mod engine;
mod rpc;
pub mod transport;
mod utils;

use crate::{
    engine::CoreEngine,
    transport::{
        Transport,
        mock::MockTransport,
    },
};
use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::overlay::OverlayDb,
    store::AssertionStore,
};
use crossbeam::channel::unbounded;
use std::convert::Infallible;

use revm::database::{
    CacheDB,
    EmptyDBTyped,
};

use clap::Parser;
use rust_tracing::trace;

use args::SidecarArgs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    trace();

    let args = SidecarArgs::parse();

    let (tx_sender, tx_receiver) = unbounded();
    let state: OverlayDb<CacheDB<EmptyDBTyped<Infallible>>> =
        OverlayDb::new(None, 128 * 1024 * 1024);
    let assertion_executor = AssertionExecutor::new(
        ExecutorConfig::default(),
        AssertionStore::new_ephemeral().unwrap(),
    );

    let (_, mock_receiver) = unbounded();
    let mock_transport = MockTransport::new(tx_sender, mock_receiver);

    let mut engine = CoreEngine::new(state, tx_receiver, assertion_executor);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
        _ = rpc::start_rpc_server(&args) => {
            println!("rpc server exited, shutting down...");
        }
        _ = engine.run() => {
            println!("Engine run completed, shutting down...");
        }
        _ = mock_transport.run() => {
            println!("Engine run completed, shutting down...");
        }
    }

    println!("Sidecar shutdown complete.");
    Ok(())
}
