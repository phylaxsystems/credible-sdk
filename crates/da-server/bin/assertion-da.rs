use assertion_da_server::Config;

use clap::Parser;
use tokio_util::sync::CancellationToken;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize a tracing subscriber
    let _guard = rust_tracing::trace();

    let config = Config::parse();

    let server = config.build().await?;
    let cancellation_token = CancellationToken::new();

    let mut boxed_server_future = Box::pin(server.run(cancellation_token.clone()));

    tokio::select! {
        result = &mut boxed_server_future => {
           handle_server_result(result);
        },
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl-C signal, initiating graceful shutdown");
            cancellation_token.cancel();
            handle_server_result(boxed_server_future.await);

        }
    }

    Ok(())
}

/// Handle the result of the server
fn handle_server_result(result: Result<()>) {
    match result {
        Ok(()) => tracing::info!("Server shutdown gracefully"),
        Err(e) => {
            tracing::error!("Server encountered an error: {}", e);
        }
    }
}
