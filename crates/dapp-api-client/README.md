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

The client code (`src/generated/client.rs`) is auto-generated from the dApp API's OpenAPI specification and is committed to version control. This ensures reliable builds and makes API changes visible in pull requests.

#### When to Regenerate

Regenerate the client when:
- The dApp API has been updated with new endpoints or changes
- You're preparing a new release that needs the latest API
- You're explicitly working on API integration updates

#### How to Regenerate

The easiest way is using the Makefile from the repository root:

```bash
# Regenerate from production API
make regenerate

# Regenerate from development API (localhost:3000)
make regenerate-dev
```

Or manually from the crate directory:

1. **Basic regeneration** (uses cached spec if available):
   ```bash
   cargo build --features regenerate
   ```

2. **Force fetch latest spec** from the API:
   ```bash
   FORCE_SPEC_REGENERATE=true cargo build --features regenerate
   ```

3. **Development environment** (fetch from localhost:3000):
   ```bash
   DAPP_ENV=development cargo build --features regenerate
   ```

#### Regeneration Process

1. The build script fetches the OpenAPI spec from the dApp API
2. Caches it in `openapi/spec.json` (git-ignored)
3. Generates new client code in `src/generated/client.rs`
4. Review the changes with `git diff` before committing
5. Commit with a message like: "chore: regenerate dapp-api-client from latest spec"

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