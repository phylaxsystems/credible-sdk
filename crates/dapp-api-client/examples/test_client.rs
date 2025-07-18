use dapp_api_client::{
    AuthConfig,
    Client,
    Config,
    Environment,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a client configuration from environment
    let config = Config::from_environment(Environment::Production);

    // Create an auth configuration
    let auth_config = AuthConfig::bearer_token("your-token-here".to_string())?;

    // Create the client
    let client = Client::new_with_auth(config, auth_config)?;

    println!("Client created successfully!");
    println!("Base URL: {}", client.base_url());

    // The client is now ready to use
    // You can access the generated client methods through client.inner()

    Ok(())
}
