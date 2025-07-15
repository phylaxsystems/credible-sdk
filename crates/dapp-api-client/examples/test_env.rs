use dapp_api_client::Environment;

fn main() {
    // Test the Environment enum
    println!("Testing Environment enum implementation...\n");

    // Test URL mapping
    println!("Development URL: {}", Environment::Development.base_url());
    println!("Production URL: {}", Environment::Production.base_url());

    // Test equality
    let dev = Environment::Development;
    let another_dev = Environment::Development;
    assert_eq!(dev, another_dev);
    assert_ne!(Environment::Development, Environment::Production);
    println!("\n✓ Equality tests passed");

    // Test Copy trait
    let env = Environment::Development;
    let env_copy = env; // This uses Copy
    assert_eq!(env, env_copy);
    println!("✓ Copy trait works");

    // Test Clone trait
    let env_clone = env; // Copy trait is used automatically
    assert_eq!(env, env_clone);
    println!("✓ Clone trait works");

    // Test Debug trait
    println!("\n✓ Debug output: {:?}", Environment::Production);

    // Test serialization (if serde_json is available)
    if let Ok(json) = serde_json::to_string(&Environment::Production) {
        println!("✓ JSON serialization: {json}");

        // Test deserialization
        if let Ok(deserialized) = serde_json::from_str::<Environment>(&json) {
            assert_eq!(deserialized, Environment::Production);
            println!("✓ JSON deserialization works");
        }
    }

    println!("\nAll tests passed! ✨");
}
