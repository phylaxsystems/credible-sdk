# dapp-api-client

A Rust client library for interacting with the dApp API services.

## Features

- Auto-generated client from OpenAPI specification
- Bearer token authentication
- Environment-based configuration
- Type-safe API interactions

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dapp-api-client = { path = "../path/to/dapp-api-client" }
```

## Usage

### Basic Setup

```rust
use dapp_api_client::{Client, Config, Environment, AuthConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let config = Config::from_environment(Environment::Production);
    
    // Create auth configuration
    let auth_config = AuthConfig::bearer_token("your-token-here".to_string())?;
    
    // Create client
    let client = Client::new_with_auth(config, auth_config)?;
    
    // Use the client
    let api = client.inner();
    // ... make API calls ...
    
    Ok(())
}
```

### Configuration

The client can be configured in several ways:

```rust
// From a specific environment
let config = Config::from_environment(Environment::Development);

// From environment variables (DAPP_ENV)
let config = Config::from_env();

// Custom base URL
let config = Config::new("https://custom-api.example.com/api/v1".to_string());
```

### Environment Variables

- `DAPP_ENV`: Set to `development`/`dev` or `production`/`prod` (defaults to production)
- `DAPP_API_TOKEN`: Your API bearer token (for examples)

## Development

### Regenerating Client Code

To regenerate the client code from the latest OpenAPI specification:

```bash
cargo build --features regenerate
```

To force re-fetching the spec even if cached:

```bash
FORCE_SPEC_REGENERATE=true cargo build --features regenerate
```

### Running Examples

```bash
# Basic client creation
cargo run --example test_client

# API usage example
DAPP_API_TOKEN=your-token cargo run --example api_usage
```

### Running Tests

```bash
cargo test
```

## License

See the repository's LICENSE file.