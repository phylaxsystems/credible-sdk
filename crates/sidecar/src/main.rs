use rust_tracing::trace;
use clap::Parser;

mod args;
use args::SidecarArgs;

fn main() {
	trace();

	let args = SidecarArgs::parse();
	println!("Sidecar started with args: {:#?}", args);
}