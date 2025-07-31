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

	let running = Arc::new(AtomicBool::new(true));
	let r = running.clone();

	ctrlc::set_handler(move || {
		println!("Received Ctrl+C, shutting down gracefully...");
		r.store(false, Ordering::SeqCst);
	}).expect("Error setting Ctrl-C handler");

	println!("Sidecar running. Press Ctrl+C to stop.");

	while running.load(Ordering::SeqCst) {
		tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
	}

	println!("Sidecar shutdown complete.");
	Ok(())
}
