//! Module for regenerating OpenAPI specifications
//!
//! This module handles fetching and caching OpenAPI specs from the dApp API
//! when the 'regenerate' feature is enabled.

use anyhow::Context;
use std::{
    env,
    fs,
    time::{
        Duration,
        SystemTime,
    },
};

pub fn check_and_fetch_spec() -> anyhow::Result<()> {
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

fn fetch_and_cache_spec() -> anyhow::Result<()> {
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

fn validate_openapi_spec(spec: &serde_json::Value) -> anyhow::Result<()> {
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

fn cache_spec_with_metadata(mut spec: serde_json::Value) -> anyhow::Result<()> {
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
