use rust_tracing::trace;
use clap::Parser;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

mod args;
use args::SidecarArgs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	trace();

	let args = SidecarArgs::parse();
	println!("Sidecar started with args: {:#?}", args);

	// Parse the RPC URL to get the socket address
	let rpc_url = &args.rollup.rpc_url;
	let addr = rpc_url
		.strip_prefix("http://")
		.unwrap_or(rpc_url)
		.parse::<std::net::SocketAddr>()
		.unwrap_or_else(|_| {
			eprintln!("Failed to parse RPC URL '{}', using default 0.0.0.0:9545", rpc_url);
			"0.0.0.0:9545".parse().unwrap()
		});

	println!("Attempting to bind RPC server to: {}", addr);

	let running = Arc::new(AtomicBool::new(true));
	let r = running.clone();

	ctrlc::set_handler(move || {
		println!("Received Ctrl+C, shutting down gracefully...");
		r.store(false, Ordering::SeqCst);
	}).expect("Error setting Ctrl-C handler");

	// Start the JSON-RPC server in a background task
	let _server_running = running.clone();
	tokio::spawn(async move {
		if let Err(e) = rpc::start_rpc_server(addr).await {
			eprintln!("RPC server error: {}", e);
		}
	});

	println!("Sidecar running. Press Ctrl+C to stop.");

	while running.load(Ordering::SeqCst) {
		tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
	}

	println!("Sidecar shutdown complete.");
	Ok(())
}
