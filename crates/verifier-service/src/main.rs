use std::net::SocketAddr;

use assertion_executor::ExecutorConfig;
use clap::Parser;
use verifier_service::build_router;

#[derive(Debug, Parser)]
struct Args {
    /// Address the verifier service HTTP server binds to.
    #[arg(
        long,
        env = "VERIFIER_SERVICE_BIND_ADDR",
        default_value = "127.0.0.1:8200"
    )]
    bind_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _guard = rust_tracing::trace();
    let args = Args::parse();

    let listener = tokio::net::TcpListener::bind(args.bind_addr).await?;
    tracing::info!(bind_addr = %args.bind_addr, "Starting verifier service");

    axum::serve(listener, build_router(ExecutorConfig::default()))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    if tokio::signal::ctrl_c().await.is_ok() {
        tracing::info!("Received shutdown signal");
    }
}
