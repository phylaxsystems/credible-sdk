use clap::Parser;
use rpc_proxy::{
    ProxyConfig,
    RpcProxyBuilder,
    fingerprint::CacheConfig,
};
use tracing_subscriber::{
    EnvFilter,
    layer::SubscriberExt,
    util::SubscriberInitExt,
};
use url::Url;

#[derive(Debug, Parser)]
#[command(author, version, about = "Credible RPC proxy", long_about = None)]
struct Cli {
    /// Address for the proxy HTTP server (e.g. 0.0.0.0:9547)
    #[arg(long = "listen", default_value = "127.0.0.1:9547")]
    listen_addr: String,
    /// JSON-RPC path exposed by the proxy
    #[arg(long = "rpc-path", default_value = "/rpc")]
    rpc_path: String,
    /// Upstream sequencer or node HTTP endpoint
    #[arg(long = "upstream", default_value = "http://127.0.0.1:8545")]
    upstream: String,
    /// Optional gRPC endpoint exposed by the sidecar
    #[arg(long = "sidecar-endpoint")]
    sidecar_endpoint: Option<String>,
    /// Dry-run mode: log rejections but forward everything (for validation)
    #[arg(long = "dry-run")]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let listen_addr = cli.listen_addr.parse()?;
    let upstream_http = Url::parse(&cli.upstream)?;
    let sidecar_endpoint = match cli.sidecar_endpoint {
        Some(ref value) => Some(Url::parse(value)?),
        None => None,
    };

    let config = ProxyConfig {
        bind_addr: listen_addr,
        rpc_path: cli.rpc_path,
        upstream_http,
        sidecar_endpoint,
        cache: CacheConfig::default(),
        dry_run: cli.dry_run,
    }
    .validate()?;

    if config.dry_run {
        tracing::warn!("DRY-RUN MODE ENABLED: Will log rejections but forward all transactions");
    }

    RpcProxyBuilder::new(config).build()?.serve().await?;
    Ok(())
}
