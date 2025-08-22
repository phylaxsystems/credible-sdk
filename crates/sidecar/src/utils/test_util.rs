/// This gets invoked before any tests, when the cargo test framework loads the test library.
/// It injects itself into the binary when we have tests enabled. It does this by being an
/// optional feature.
#[ctor::ctor]
fn init_tests() {
    use tracing_subscriber::{
        filter::filter_fn,
        prelude::*,
    };
    if let Ok(v) = std::env::var("TEST_TRACE") {
        let level = match v.as_str() {
            "false" | "off" => return,
            "true" | "debug" | "on" => tracing::Level::DEBUG,
            "trace" => tracing::Level::TRACE,
            "info" => tracing::Level::INFO,
            "warn" => tracing::Level::WARN,
            "error" => tracing::Level::ERROR,
            _ => return,
        };

        let prefix_blacklist = &[
            "sled", // we dont want sled tree tracing
        ];

        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(filter_fn(move |metadata| {
                metadata.level() <= &level
                    && !prefix_blacklist
                        .iter()
                        .any(|prefix| metadata.target().starts_with(prefix))
            }))
            .init();
    }
}

// Test database type definitions
use std::convert::Infallible;

/// Error type used in tests, infallible for simplicity
pub type TestDbError = Infallible;

/// Re-export the engine_test procedural macro
/// 
/// This macro simplifies test setup by automatically creating a LocalInstance
/// and passing it to your test function.
/// 
/// # Usage
/// ```
/// use sidecar::utils::engine_test;
/// 
/// #[engine_test]
/// async fn test_something(mut instance: LocalInstance) {
///     instance.new_block().unwrap();
///     // Your test code here
/// }
/// ```
pub use sidecar_test_macros::engine_test;
