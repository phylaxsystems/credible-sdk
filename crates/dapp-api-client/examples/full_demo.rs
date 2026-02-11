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
    generated::client::{
        Error as GeneratedError,
        types::{
            GetProjectsResponse,
            GetProjectsResponseItem,
        },
    },
};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== dapp-api-client SDK Demo ===\n");

    // 1. Environment Configuration
    let config = print_environment_configuration();
    print_authentication_setup();

    let token = env::var("DAPP_API_TOKEN");
    match token {
        Ok(token_value) => run_authenticated_demo(config, token_value).await?,
        Err(_) => run_unauthenticated_demo(config).await?,
    }

    print_error_handling();
    print_best_practices();
    println!("\n=== Demo Complete ===");

    Ok(())
}

fn print_environment_configuration() -> Config {
    println!("1. Environment Configuration:");
    println!(
        "   - Development URL: {}",
        Environment::Development.base_url()
    );
    println!(
        "   - Production URL: {}",
        Environment::Production.base_url()
    );

    let env_setting = env::var("DAPP_ENV").unwrap_or_else(|_| "prod".to_string());
    println!("   - Current DAPP_ENV: {env_setting}");

    let config = Config::from_env();
    println!("   - Loaded configuration from environment\n");
    config
}

fn print_authentication_setup() {
    println!("2. Authentication Setup:");
}

async fn run_authenticated_demo(
    config: Config,
    token_value: String,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("   ✓ Found DAPP_API_TOKEN in environment");

    let auth_config = AuthConfig::bearer_token(token_value)?;
    println!("   ✓ Created bearer token authentication");

    println!("\n3. Client Creation:");
    let client = Client::new_with_auth(config, auth_config)?;
    println!("   ✓ Client created successfully");
    println!("   - Base URL: {}", client.base_url());

    println!("\n4. API Usage - GET /projects:");
    let api = client.inner();
    match api.get_projects(None, None, None).await {
        Ok(response) => {
            let projects = response.into_inner();
            println!("   ✓ Successfully retrieved projects");
            println!("   - Found {} projects", projects.len());
            print_authenticated_project_details(&projects);
        }
        Err(e) => print_project_error(&e),
    }

    print_additional_methods();
    Ok(())
}

async fn run_unauthenticated_demo(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    println!("   ⚠️  DAPP_API_TOKEN not found in environment");
    println!("\n   To use authentication:");
    println!("   export DAPP_API_TOKEN='your-api-token-here'");

    println!("\n   Creating unauthenticated client for demo...");
    let client = Client::new(config)?;
    println!("   ✓ Unauthenticated client created");
    println!("   - Base URL: {}", client.base_url());

    println!("\n4. API Usage - GET /projects (public endpoint):");
    let api = client.inner();
    match api.get_projects(None, None, None).await {
        Ok(response) => {
            let projects = response.into_inner();
            println!("   ✓ Successfully retrieved projects without authentication!");
            println!("   - Found {} projects", projects.len());
            print_public_project_details(&projects);
        }
        Err(e) => print_project_error(&e),
    }

    println!("\n   ℹ️  Note: Some endpoints require authentication");
    println!("   Set DAPP_API_TOKEN environment variable to access all endpoints");
    Ok(())
}

fn print_authenticated_project_details(projects: &[GetProjectsResponseItem]) {
    if projects.is_empty() {
        println!("   📝 No projects found");
        return;
    }

    println!("\n   📋 Project Details:");
    for (i, project) in projects.iter().take(5).enumerate() {
        println!("   {}. ID: {}", i + 1, project.project_id);
        println!("      Name: {}", project.project_name.as_str());
        println!(
            "      Description: {}",
            project
                .project_description
                .as_deref()
                .map_or("No description", |v| v)
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
}

fn print_public_project_details(projects: &[GetProjectsResponseItem]) {
    if projects.is_empty() {
        println!("   📝 No projects found");
        return;
    }

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
}

fn print_project_error(e: &GeneratedError<GetProjectsResponse>) {
    println!("   ❌ Failed to get projects: {e}");
    println!("   - Error details: {e:?}");

    if let Some(status) = e.status() {
        println!("   - HTTP Status: {status}");
    }
}

fn print_additional_methods() {
    println!("\n5. Additional Available Methods:");
    println!("   - api.get_projects(network_id, user, show_archived) - List projects with filters");
    println!("   - api.get_projects_saved(wallet_address) - Get saved projects");
    println!("   - api.get_projects_project_id(project_id, include) - Get specific project");
    println!("   - api.get_health() - Check API health");
}

fn print_error_handling() {
    println!("\n6. Error Handling:");
    println!("   The SDK provides these error types:");
    println!("   - HTTP client errors (network, timeout, etc.)");
    println!("   - API response errors (4xx, 5xx status codes)");
    println!("   - Authentication errors (invalid token, expired, etc.)");
    println!("   - Serialization errors (invalid response format)");
}

fn print_best_practices() {
    println!("\n7. Best Practices:");
    println!("   - Store tokens in environment variables");
    println!("   - Use Config::from_env() for automatic environment detection");
    println!("   - Handle errors appropriately in production code");
    println!("   - Use the generated client methods for type-safe API calls");
    println!("   - Check response status codes and handle different error scenarios");
}
