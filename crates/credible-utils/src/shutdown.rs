use tokio::signal::unix::{
    SignalKind,
    signal,
};

/// Waits for a SIGTERM signal.
pub async fn wait_for_sigterm() -> anyhow::Result<()> {
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|_| anyhow::anyhow!("Failed to install rustls crypto provider"))?;

    sigterm.recv().await;
    Ok(())
}
