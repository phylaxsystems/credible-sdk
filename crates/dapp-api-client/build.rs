//! Build script for dapp-api-client crate
//!
//! This script:
//! 1. Conditionally fetches the OpenAPI specification from the dApp API when the 'regenerate' feature is enabled
//! 2. Generates Rust client code from the cached OpenAPI specification using progenitor

#[cfg(feature = "regenerate")]
use std::{
    fs,
    time::{
        Duration,
        SystemTime,
    },
};

fn main() {
    // Only run spec fetching when 'regenerate' feature is enabled
    #[cfg(feature = "regenerate")]
    {
        println!("cargo:warning=Running OpenAPI spec regeneration...");
        if let Err(e) = check_and_fetch_spec() {
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
    if let Err(e) = generate_client_code() {
        println!("cargo:warning=Failed to generate client code: {e}");
        std::process::exit(1);
    }
}

#[cfg(feature = "regenerate")]
fn check_and_fetch_spec() -> anyhow::Result<()> {
    use std::env;

    const CACHE_FILE: &str = "openapi/spec.json";

    // Check if force regeneration is requested
    let force_regenerate = env::var("FORCE_SPEC_REGENERATE")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false);

    // Check if cache exists
    if !force_regenerate && std::path::Path::new(CACHE_FILE).exists() {
        println!("cargo:warning=OpenAPI spec cache found at {CACHE_FILE}. Skipping fetch.");
        println!("cargo:warning=To force regeneration, set FORCE_SPEC_REGENERATE=true");
        return Ok(());
    }

    // Fetch new spec
    fetch_and_cache_spec()
}

#[cfg(feature = "regenerate")]
fn fetch_and_cache_spec() -> anyhow::Result<()> {
    use anyhow::Context;
    use std::env;

    // Respect DAPP_ENV environment variable
    let openapi_url = match env::var("DAPP_ENV").as_deref() {
        Ok("development") | Ok("dev") => "http://localhost:3000/api/v1/openapi",
        _ => "https://dapp.phylax.systems/api/v1/openapi",
    };
    const TIMEOUT_SECONDS: u64 = 30;

    println!("cargo:warning=Fetching OpenAPI spec from {openapi_url}");

    // Create HTTP client with timeout
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(TIMEOUT_SECONDS))
        .build()
        .context("Failed to create HTTP client")?;

    // Fetch the OpenAPI spec
    let response = client
        .get(openapi_url)
        .send()
        .context("Failed to send HTTP request")?;

    // Check response status
    if !response.status().is_success() {
        anyhow::bail!(
            "Failed to fetch OpenAPI spec: HTTP {} {}",
            response.status().as_u16(),
            response.status().canonical_reason().unwrap_or("Unknown")
        );
    }

    // Parse JSON to validate it
    let spec_text = response.text().context("Failed to read response body")?;
    let spec_json: serde_json::Value =
        serde_json::from_str(&spec_text).context("Failed to parse OpenAPI spec as JSON")?;

    // Validate OpenAPI spec structure
    validate_openapi_spec(&spec_json)?;

    println!("cargo:warning=Successfully fetched and validated OpenAPI spec");

    // Cache the spec with metadata
    cache_spec_with_metadata(spec_json)?;

    Ok(())
}

#[cfg(feature = "regenerate")]
fn validate_openapi_spec(spec: &serde_json::Value) -> anyhow::Result<()> {
    use anyhow::Context;

    // Check for required OpenAPI fields
    let openapi_version = spec
        .get("openapi")
        .and_then(|v| v.as_str())
        .context("Missing or invalid 'openapi' field")?;

    // Validate OpenAPI version (should be 3.x.x)
    if !openapi_version.starts_with("3.") {
        anyhow::bail!(
            "Unsupported OpenAPI version: {}. Expected 3.x.x",
            openapi_version
        );
    }

    // Check for required 'info' object
    let info = spec.get("info").context("Missing required 'info' field")?;

    // Validate info has title and version
    info.get("title")
        .and_then(|v| v.as_str())
        .context("Missing 'info.title' field")?;

    let api_version = info
        .get("version")
        .and_then(|v| v.as_str())
        .context("Missing 'info.version' field")?;

    // Check for 'paths' object
    spec.get("paths")
        .and_then(|v| v.as_object())
        .context("Missing or invalid 'paths' field")?;

    println!(
        "cargo:warning=Valid OpenAPI {openapi_version} spec detected (API version: {api_version})"
    );

    Ok(())
}

#[cfg(feature = "regenerate")]
fn cache_spec_with_metadata(mut spec: serde_json::Value) -> anyhow::Result<()> {
    use anyhow::Context;

    const CACHE_DIR: &str = "openapi";
    const CACHE_FILE: &str = "openapi/spec.json";
    const CACHE_VERSION: &str = "1.0";

    // Create openapi directory if it doesn't exist
    fs::create_dir_all(CACHE_DIR).context("Failed to create openapi directory")?;

    // Get current timestamp in ISO 8601 format
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Extract API version from spec
    let api_version = spec
        .get("info")
        .and_then(|info| info.get("version"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Add metadata to the spec
    let metadata = serde_json::json!({
        "cache_version": CACHE_VERSION,
        "fetched_at": timestamp,
        "fetched_at_iso": chrono::DateTime::from_timestamp(timestamp as i64, 0)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| "unknown".to_string()),
        "api_version": api_version,
    });

    // Add metadata as a top-level field
    if let Some(obj) = spec.as_object_mut() {
        obj.insert("x-cache-metadata".to_string(), metadata);
    }

    // Write the enhanced spec to file
    let pretty_json =
        serde_json::to_string_pretty(&spec).context("Failed to serialize spec to JSON")?;

    fs::write(CACHE_FILE, pretty_json).context("Failed to write spec to cache file")?;

    println!("cargo:warning=Cached OpenAPI spec to {CACHE_FILE}");

    Ok(())
}

fn generate_client_code() -> anyhow::Result<()> {
    use std::path::PathBuf;

    const SPEC_FILE: &str = "openapi/spec.json";
    const OUTPUT_DIR: &str = "src/generated";

    // Check if spec file exists
    if !std::path::Path::new(SPEC_FILE).exists() {
        // In CI environments, automatically fetch the spec if missing
        if std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok() {
            println!(
                "cargo:warning=OpenAPI spec not found in CI environment. Fetching automatically..."
            );
            fetch_spec_for_ci()?;
        } else {
            anyhow::bail!(
                "OpenAPI spec not found at {}. Run with --features=regenerate to fetch it.",
                SPEC_FILE
            );
        }
    }

    println!("cargo:warning=Generating client code from {SPEC_FILE}");

    // Get the output directory
    let out_dir = PathBuf::from(OUTPUT_DIR);

    // Create output directory if it doesn't exist
    std::fs::create_dir_all(&out_dir)?;

    // Load the spec
    let spec_str = std::fs::read_to_string(SPEC_FILE)?;
    let mut spec_value: serde_json::Value = serde_json::from_str(&spec_str)?;

    // Remove our custom metadata field that progenitor doesn't understand
    if let Some(obj) = spec_value.as_object_mut() {
        obj.remove("x-cache-metadata");
    }

    // Fix exclusiveMinimum fields (should be boolean in OpenAPI 3.0)
    fix_exclusive_minimum(&mut spec_value);

    // Simplify Authorization headers
    simplify_authorization_headers(&mut spec_value);

    // Add missing operation IDs
    add_operation_ids(&mut spec_value);

    // Fix problematic documentation that contains invalid doctest examples
    fix_documentation(&mut spec_value);

    // Parse as OpenAPI
    let spec: openapiv3::OpenAPI = serde_json::from_value(spec_value)?;

    // Configure and run progenitor
    let mut generator = progenitor::Generator::default();

    // Generate the client code
    let tokens = generator.generate_tokens(&spec)?;

    // Write to file
    let output_path = out_dir.join("client.rs");
    let content = tokens.to_string();

    // Wrap the generated code with allow attributes to suppress warnings
    let wrapped_content = format!(
        r#"#![allow(clippy::all)]
#![allow(irrefutable_let_patterns)]
#![allow(warnings)]

{content}
"#
    );

    // Write without formatting for now
    std::fs::write(&output_path, wrapped_content)?;

    println!(
        "cargo:warning=Generated client code at {}",
        output_path.display()
    );

    // Tell cargo to rerun if the spec file changes
    println!("cargo:rerun-if-changed={SPEC_FILE}");

    Ok(())
}

/// Minimal spec fetching for CI environments
/// This doesn't require the 'regenerate' feature and is always available
fn fetch_spec_for_ci() -> anyhow::Result<()> {
    use std::time::Duration;

    const OPENAPI_URL: &str = "https://dapp.phylax.systems/api/v1/openapi";
    const TIMEOUT_SECONDS: u64 = 30;
    const CACHE_DIR: &str = "openapi";
    const CACHE_FILE: &str = "openapi/spec.json";

    println!("cargo:warning=Fetching OpenAPI spec from {OPENAPI_URL}");

    // Create openapi directory if it doesn't exist
    std::fs::create_dir_all(CACHE_DIR)?;

    // Create HTTP client with timeout
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(TIMEOUT_SECONDS))
        .build()?;

    // Fetch the OpenAPI spec
    let response = client.get(OPENAPI_URL).send()?;

    // Check response status
    if !response.status().is_success() {
        anyhow::bail!(
            "Failed to fetch OpenAPI spec: HTTP {} {}",
            response.status().as_u16(),
            response.status().canonical_reason().unwrap_or("Unknown")
        );
    }

    // Get the response text
    let spec_text = response.text()?;

    // Parse as JSON to validate
    let spec_json: serde_json::Value = serde_json::from_str(&spec_text)?;

    // Basic validation - check for required OpenAPI fields
    let openapi_version = spec_json
        .get("openapi")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing 'openapi' field"))?;

    if !openapi_version.starts_with("3.") {
        anyhow::bail!("Unsupported OpenAPI version: {}", openapi_version);
    }

    // Write the spec to file
    std::fs::write(CACHE_FILE, &spec_text)?;

    println!("cargo:warning=Successfully fetched OpenAPI spec for CI build");

    Ok(())
}

fn fix_exclusive_minimum(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            // Fix exclusiveMinimum if it's a number
            if let Some(exclusive_min) = map.get("exclusiveMinimum") {
                if exclusive_min.is_number() {
                    map.insert(
                        "exclusiveMinimum".to_string(),
                        serde_json::Value::Bool(true),
                    );
                }
            }
            // Fix exclusiveMaximum if it's a number
            if let Some(exclusive_max) = map.get("exclusiveMaximum") {
                if exclusive_max.is_number() {
                    map.insert(
                        "exclusiveMaximum".to_string(),
                        serde_json::Value::Bool(true),
                    );
                }
            }
            // Recurse into all values
            for (_, v) in map.iter_mut() {
                fix_exclusive_minimum(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                fix_exclusive_minimum(v);
            }
        }
        _ => {}
    }
}

fn simplify_authorization_headers(value: &mut serde_json::Value) {
    // Traverse paths and operations to find Authorization parameters
    if let Some(paths) = value.get_mut("paths").and_then(|p| p.as_object_mut()) {
        for (_, path_item) in paths.iter_mut() {
            if let Some(path_obj) = path_item.as_object_mut() {
                for (method, operation) in path_obj.iter_mut() {
                    if let Some(op_obj) = operation.as_object_mut() {
                        // Check if this is an HTTP method
                        if ["get", "post", "put", "delete", "patch", "head", "options"]
                            .contains(&method.as_str())
                        {
                            // Look for parameters array
                            if let Some(params) =
                                op_obj.get_mut("parameters").and_then(|p| p.as_array_mut())
                            {
                                for param in params.iter_mut() {
                                    if let Some(param_obj) = param.as_object_mut() {
                                        // Check if this is an Authorization header parameter
                                        if param_obj.get("in").and_then(|v| v.as_str())
                                            == Some("header")
                                            && param_obj.get("name").and_then(|v| v.as_str())
                                                == Some("Authorization")
                                        {
                                            // Simplify the schema to just a string type without pattern
                                            if let Some(schema) = param_obj
                                                .get_mut("schema")
                                                .and_then(|s| s.as_object_mut())
                                            {
                                                // Remove pattern constraint
                                                schema.remove("pattern");
                                                // Ensure it's just a simple string
                                                schema.insert(
                                                    "type".to_string(),
                                                    serde_json::Value::String("string".to_string()),
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn add_operation_ids(value: &mut serde_json::Value) {
    if let Some(paths) = value.get_mut("paths").and_then(|p| p.as_object_mut()) {
        for (path, path_item) in paths.iter_mut() {
            if let Some(path_obj) = path_item.as_object_mut() {
                for (method, operation) in path_obj.iter_mut() {
                    if let Some(op_obj) = operation.as_object_mut() {
                        // Only add operationId if it doesn't exist and this is an HTTP method
                        if !op_obj.contains_key("operationId")
                            && ["get", "post", "put", "delete", "patch", "head", "options"]
                                .contains(&method.as_str())
                        {
                            // Generate operationId from method and path
                            let path_parts: Vec<&str> =
                                path.split('/').filter(|s| !s.is_empty()).collect();

                            let operation_id = format!(
                                "{}_{}",
                                method,
                                path_parts
                                    .join("_")
                                    .replace("{", "")
                                    .replace("}", "")
                                    .replace("-", "_")
                            );

                            op_obj.insert(
                                "operationId".to_string(),
                                serde_json::Value::String(operation_id),
                            );
                        }
                    }
                }
            }
        }
    }
}

fn fix_documentation(value: &mut serde_json::Value) {
    // Fix the info.description field that contains problematic doctest-like examples
    if let Some(info) = value.get_mut("info").and_then(|i| i.as_object_mut()) {
        if let Some(description) = info.get_mut("description").and_then(|d| d.as_str()) {
            // Replace the problematic Authorization: Bearer example with valid documentation
            let fixed_description = description.replace(
                "```\nAuthorization: Bearer <jwt_token>\n```",
                "```http\nAuthorization: Bearer <jwt_token>\n```",
            );

            info.insert(
                "description".to_string(),
                serde_json::Value::String(fixed_description),
            );
        }
    }

    // Also check for similar issues in operation descriptions
    if let Some(paths) = value.get_mut("paths").and_then(|p| p.as_object_mut()) {
        for (_, path_item) in paths.iter_mut() {
            if let Some(path_obj) = path_item.as_object_mut() {
                for (method, operation) in path_obj.iter_mut() {
                    if let Some(op_obj) = operation.as_object_mut() {
                        // Check if this is an HTTP method
                        if ["get", "post", "put", "delete", "patch", "head", "options"]
                            .contains(&method.as_str())
                        {
                            // Fix description if it exists
                            if let Some(desc) =
                                op_obj.get_mut("description").and_then(|d| d.as_str())
                            {
                                if desc.contains("Authorization: Bearer") {
                                    let fixed_desc = desc.replace(
                                        "```\nAuthorization: Bearer",
                                        "```http\nAuthorization: Bearer",
                                    );
                                    op_obj.insert(
                                        "description".to_string(),
                                        serde_json::Value::String(fixed_desc),
                                    );
                                }
                            }

                            // Fix summary if it exists
                            if let Some(summary) =
                                op_obj.get_mut("summary").and_then(|s| s.as_str())
                            {
                                if summary.contains("Authorization: Bearer") {
                                    let fixed_summary = summary.replace(
                                        "```\nAuthorization: Bearer",
                                        "```http\nAuthorization: Bearer",
                                    );
                                    op_obj.insert(
                                        "summary".to_string(),
                                        serde_json::Value::String(fixed_summary),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
