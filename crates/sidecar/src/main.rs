use clap::Parser;
use rust_tracing::trace;

mod args;
mod rpc;
use args::SidecarArgs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    trace();

    let args = SidecarArgs::parse();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
        _ = rpc::start_rpc_server(&args) => {
            println!("rpc server exited, shutting down...");
        }
    }

    println!("Sidecar running. Press Ctrl+C to stop.");

    println!("Sidecar shutdown complete.");
    Ok(())
}
