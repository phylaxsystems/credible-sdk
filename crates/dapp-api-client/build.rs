//! Build script for dapp-api-client crate
//!
//! This script:
//! 1. Conditionally fetches the `OpenAPI` specification from the dApp API when the 'regenerate' feature is enabled
//! 2. Generates Rust client code from the cached `OpenAPI` specification using progenitor

// Import modules from the build directory
#[path = "build/codegen.rs"]
mod codegen;

#[cfg(feature = "regenerate")]
#[path = "build/regenerate.rs"]
mod regenerate;

fn main() {
    const CLIENT_PATH: &str = "src/generated/client.rs";
    const SPEC_PATH: &str = "openapi/spec.json";

    // With regenerate feature: always fetch fresh spec and regenerate
    #[cfg(feature = "regenerate")]
    {
        println!("cargo:warning=Running OpenAPI spec regeneration...");
        if let Err(e) = regenerate::check_and_fetch_spec() {
            println!("cargo:warning=Failed to fetch OpenAPI spec: {e}");
            std::process::exit(1);
        }

        // Always regenerate when feature is enabled
        if let Err(e) = codegen::generate_client_code() {
            println!("cargo:warning=Failed to generate client code: {e}");
            std::process::exit(1);
        }
        return;
    }

    // Without regenerate feature: check what's available
    let client_exists = std::path::Path::new(CLIENT_PATH).exists();
    let spec_exists = std::path::Path::new(SPEC_PATH).exists();

    match (client_exists, spec_exists) {
        // Client exists - use it, no need for spec
        (true, _) => {
            println!("cargo:warning=Using existing client.rs");
        }
        // No client but spec exists - generate from cached spec
        (false, true) => {
            println!("cargo:warning=No client.rs found, generating from cached spec.json");
            if let Err(e) = codegen::generate_client_code() {
                println!("cargo:warning=Failed to generate client code: {e}");
                std::process::exit(1);
            }
        }
        // Neither exists - error out
        (false, false) => {
            eprintln!("Error: Neither client.rs nor spec.json found.");
            eprintln!(
                "Please run with --features=regenerate to fetch the OpenAPI spec and generate the client."
            );
            eprintln!("Example: cargo build --features=regenerate");
            std::process::exit(1);
        }
    }
}
