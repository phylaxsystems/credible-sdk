use std::str::FromStr;

use alloy::primitives::FixedBytes;
use assertion_da_client::DaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test with HTTP endpoint
    println!("Testing HTTP endpoint...");
    let _http_client = DaClient::new("http://localhost:5001")?;
    println!("✓ HTTP client created successfully");

    // Test with HTTPS endpoint
    println!("Testing HTTPS endpoint...");
    let https_client = DaClient::new("https://demo-21-assertion-da.phylax.systems")?;
    println!("✓ HTTPS client created successfully");

    let bytes: FixedBytes<32> =
        FixedBytes::from_str("43ccaf21bc5cf9efce72530ecfecbd6d513e895546749720048e0e39bbedce37") // example id
            .expect("REASON");
    let rax = https_client.fetch_assertion(bytes).await.unwrap();
    println!("Fetched assertion: {rax:?}");

    // Test with authentication
    println!("Testing authenticated client...");
    let _auth_client = DaClient::new_with_auth(
        "https://demo-21-assertion-da.phylax.systems",
        "Bearer test-token",
    )?;
    println!("✓ Authenticated client created successfully");

    println!(
        "\nClient creation tests passed! The new reqwest-based client should resolve TLS/SSL issues."
    );
    println!("Now you can use the client with both HTTP and HTTPS endpoints without TLS errors.");

    Ok(())
}
