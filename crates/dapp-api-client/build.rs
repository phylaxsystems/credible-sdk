//! Build script for dapp-api-client crate
//!
//! This script:
//! 1. Conditionally fetches the OpenAPI specification from the dApp API when the 'regenerate' feature is enabled
//! 2. Generates Rust client code from the cached OpenAPI specification using progenitor

// Import modules from the build directory
#[path = "build/codegen.rs"]
mod codegen;

#[cfg(feature = "regenerate")]
#[path = "build/regenerate.rs"]
mod regenerate;

fn main() {
    // Only run spec fetching when 'regenerate' feature is enabled
    #[cfg(feature = "regenerate")]
    {
        println!("cargo:warning=Running OpenAPI spec regeneration...");
        if let Err(e) = regenerate::check_and_fetch_spec() {
            println!("cargo:warning=Failed to fetch OpenAPI spec: {e}");
            std::process::exit(1);
        }
    }

    #[cfg(not(feature = "regenerate"))]
    {
        println!(
            "cargo:warning=Skipping OpenAPI spec regeneration (enable with --features regenerate)"
        );
    }

    // Always generate client code from cached spec
    if let Err(e) = codegen::generate_client_code() {
        println!("cargo:warning=Failed to generate client code: {e}");
        std::process::exit(1);
    }
}
