//! Example demonstrating API usage with the dapp-api-client

use dapp_api_client::{
    AuthConfig,
    Client,
    Config,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment or use production
    let config = Config::from_env();

    // In a real application, you would load this from environment variables
    let token =
        std::env::var("DAPP_API_TOKEN").unwrap_or_else(|_| "your-api-token-here".to_string());

    // Create auth configuration
    let auth_config = AuthConfig::bearer_token(token)?;

    // Create the authenticated client
    let client = Client::new_with_auth(config, auth_config)?;

    println!("Connected to: {}", client.base_url());

    // Access the generated client for API calls
    let _api = client.inner();

    // Example: Get health status (assuming there's a health endpoint)
    // Uncomment and adjust based on actual API endpoints:
    /*
    match api.get_health().await {
        Ok(response) => {
            println!("API Health: {:?}", response);
        }
        Err(e) => {
            eprintln!("Failed to get health status: {}", e);
        }
    }
    */

    // Example: List projects (assuming there's a projects endpoint)
    // Uncomment and adjust based on actual API endpoints:
    /*
    match api.get_projects().await {
        Ok(projects) => {
            println!("Found {} projects", projects.len());
            for project in projects.iter().take(5) {
                println!("  - {}: {}", project.id, project.name);
            }
        }
        Err(e) => {
            eprintln!("Failed to list projects: {}", e);
        }
    }
    */

    println!("\nTo use this example:");
    println!("1. Set DAPP_API_TOKEN environment variable with your API token");
    println!("2. Optionally set DAPP_ENV to 'dev' or 'prod' (defaults to prod)");
    println!("3. Uncomment the API calls above based on available endpoints");

    Ok(())
}
