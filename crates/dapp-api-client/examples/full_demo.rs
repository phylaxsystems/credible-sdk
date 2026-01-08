//! Full demonstration of dapp-api-client capabilities
//!
//! This example shows:
//! - Environment configuration
//! - Authentication setup
//! - Making API calls (when endpoints are available)
//! - Error handling

use dapp_api_client::{
    AuthConfig,
    Client,
    Config,
    Environment,
};
use std::env;

#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== dapp-api-client SDK Demo ===\n");

    // 1. Environment Configuration
    println!("1. Environment Configuration:");
    println!(
        "   - Development URL: {}",
        Environment::Development.base_url()
    );
    println!(
        "   - Production URL: {}",
        Environment::Production.base_url()
    );

    // Check if DAPP_ENV is set
    let env_setting = env::var("DAPP_ENV").unwrap_or_else(|_| "prod".to_string());
    println!("   - Current DAPP_ENV: {env_setting}");

    // Load config from environment
    let config = Config::from_env();
    println!("   - Loaded configuration from environment\n");

    // 2. Authentication Setup
    println!("2. Authentication Setup:");

    // Try to get token from environment
    let token = env::var("DAPP_API_TOKEN");

    #[allow(clippy::single_match_else)]
    match token {
        Ok(token_value) => {
            println!("   ✓ Found DAPP_API_TOKEN in environment");

            // Create auth config
            let auth_config = AuthConfig::bearer_token(token_value)?;
            println!("   ✓ Created bearer token authentication");

            // 3. Client Creation
            println!("\n3. Client Creation:");
            let client = Client::new_with_auth(config, auth_config)?;
            println!("   ✓ Client created successfully");
            println!("   - Base URL: {}", client.base_url());

            // 4. API Usage - Making a real call to GET /projects
            println!("\n4. API Usage - GET /projects:");

            // Get the inner generated client
            let api = client.inner();

            // Make the API call
            match api.get_projects(None, None, None).await {
                Ok(response) => {
                    let projects = response.into_inner();
                    println!("   ✓ Successfully retrieved projects");
                    println!("   - Found {} projects", projects.len());

                    // Display first few projects
                    #[allow(clippy::if_not_else)]
                    if !projects.is_empty() {
                        println!("\n   📋 Project Details:");
                        for (i, project) in projects.iter().take(5).enumerate() {
                            println!("   {}. ID: {}", i + 1, project.project_id);
                            println!("      Name: {}", project.project_name.as_str());
                            println!(
                                "      Description: {}",
                                project
                                    .project_description
                                    .as_ref()
                                    .map_or("No description", |d| d.as_str())
                            );
                            println!(
                                "      Created: {}",
                                project.created_at.format("%Y-%m-%d %H:%M:%S")
                            );
                            println!("      Manager: {}", project.project_manager.as_str());
                            println!("      Networks: {:?}", project.project_networks);
                            println!("      Saved Count: {}", project.saved_count);
                            println!();
                        }

                        if projects.len() > 5 {
                            println!("   ... and {} more projects", projects.len() - 5);
                        }
                    } else {
                        println!("   📝 No projects found");
                    }
                }
                Err(e) => {
                    println!("   ❌ Failed to get projects: {e}");

                    // Show error details
                    println!("   - Error details: {e:?}");

                    // Try to extract status code if available
                    if let Some(status) = e.status() {
                        println!("   - HTTP Status: {status}");
                    }
                }
            }

            // 5. Additional API examples
            println!("\n5. Additional Available Methods:");
            println!(
                "   - api.get_projects(network_id, user, show_archived) - List projects with filters"
            );
            println!("   - api.get_projects_saved(wallet_address) - Get saved projects");
            println!(
                "   - api.get_projects_project_id(project_id, include) - Get specific project"
            );
            println!("   - api.get_health() - Check API health");
        }
        Err(_) => {
            println!("   ⚠️  DAPP_API_TOKEN not found in environment");
            println!("\n   To use authentication:");
            println!("   export DAPP_API_TOKEN='your-api-token-here'");

            // Show unauthenticated client creation
            println!("\n   Creating unauthenticated client for demo...");
            let client = Client::new(config)?;
            println!("   ✓ Unauthenticated client created");
            println!("   - Base URL: {}", client.base_url());

            // 4. API Usage - Making a real call to GET /projects (public endpoint)
            println!("\n4. API Usage - GET /projects (public endpoint):");

            // Get the inner generated client
            let api = client.inner();

            // Make the API call - this should work without authentication
            match api.get_projects(None, None, None).await {
                Ok(response) => {
                    let projects = response.into_inner();
                    println!("   ✓ Successfully retrieved projects without authentication!");
                    println!("   - Found {} projects", projects.len());

                    // Display first few projects
                    #[allow(clippy::if_not_else)]
                    if !projects.is_empty() {
                        println!("\n   📋 Public Project Details:");
                        for (i, project) in projects.iter().take(3).enumerate() {
                            println!("   {}. ID: {}", i + 1, project.project_id);
                            println!("      Name: {}", project.project_name.as_str());
                            println!(
                                "      Created: {}",
                                project.created_at.format("%Y-%m-%d %H:%M:%S")
                            );
                        }

                        if projects.len() > 3 {
                            println!("   ... and {} more projects", projects.len() - 3);
                        }
                    } else {
                        println!("   📝 No projects found");
                    }
                }
                Err(e) => {
                    println!("   ❌ Failed to get projects: {e}");
                    println!("   - Error details: {e:?}");

                    // Try to extract status code if available
                    if let Some(status) = e.status() {
                        println!("   - HTTP Status: {status}");
                    }
                }
            }

            println!("\n   ℹ️  Note: Some endpoints require authentication");
            println!("   Set DAPP_API_TOKEN environment variable to access all endpoints");
        }
    }

    // 6. Error Handling Example
    println!("\n6. Error Handling:");
    println!("   The SDK provides these error types:");
    println!("   - HTTP client errors (network, timeout, etc.)");
    println!("   - API response errors (4xx, 5xx status codes)");
    println!("   - Authentication errors (invalid token, expired, etc.)");
    println!("   - Serialization errors (invalid response format)");

    // 7. Best Practices
    println!("\n7. Best Practices:");
    println!("   - Store tokens in environment variables");
    println!("   - Use Config::from_env() for automatic environment detection");
    println!("   - Handle errors appropriately in production code");
    println!("   - Use the generated client methods for type-safe API calls");
    println!("   - Check response status codes and handle different error scenarios");

    println!("\n=== Demo Complete ===");

    Ok(())
}
