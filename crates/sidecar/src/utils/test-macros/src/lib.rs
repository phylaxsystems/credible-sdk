use proc_macro::TokenStream;
use quote::quote;
use syn::{
    ItemFn, Ident,
    parse_macro_input,
};

/// Contains info about a single transport variant. Used in the macro below to generate tests
/// for different variants.
struct TransportVariant {
    /// Name of the test variant, and what the test should be prefixed with
    test_name: &'static str,
    /// `crate::...` import of the transport
    transport_type: &'static str,
    /// If we need to modify the arguments to spawn the transport in any way
    options: fn(&proc_macro2::TokenStream) -> proc_macro2::TokenStream,
}

const VARIANTS: &[TransportVariant] = &[TransportVariant {
    test_name: "mock",
    transport_type: "crate::utils::LocalInstanceMockDriver",
    options: |_| quote! {},
}];

/// Procedural macro for sidecar engine tests.
///
/// This macro automatically creates a LocalInstance and passes it to your test function.
/// The test function must be async and take a single parameter of type LocalInstance.
///
/// # Example
/// ```
/// #[engine_test(all)]
/// async fn test_transaction_processing(mut instance: LocalInstance) {
///     instance.new_block().unwrap();
///     // Your test code here
/// }
/// ```
// TODO: we should expand this macro to generate tests for multiple transports.
// For example, we can from one test generate multiple tests for different transports
// if we genericize `LocalInstnace` to accept different transports. The syntax would
// then look something like:
// ```
// #[engine_test(all)] // can also specify transports for testing only specific ones
// async fn test_transaction_processing(mut instance: LocalInstance) {
//     instance.new_block().unwrap();
//     // Your test code here
// }
// ```
// and would generate tests test_transaction_processing_mock,
// test_transaction_processing_http, etc...
#[proc_macro_attribute]
pub fn engine_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    
    let fn_name = &input_fn.sig.ident;
    let fn_body = &input_fn.block;
    let fn_vis = &input_fn.vis;

    // Parse which variants to test - require arguments
    let variants_to_test: Vec<&'static str> = if attr.is_empty() {
        panic!("engine_test macro requires an argument: use #[engine_test(all)] or #[engine_test(mock)]");
    } else {
        let attr_str = attr.to_string();
        if attr_str.trim() == "all" {
            VARIANTS.iter().map(|v| v.test_name).collect()
        } else {
            // Check if the attribute matches any variant test_name
            let requested_variant = attr_str.trim();
            if let Some(variant) = VARIANTS.iter().find(|v| v.test_name == requested_variant) {
                vec![variant.test_name]
            } else {
                panic!("Unknown variant '{}'. Available variants: {} or 'all'", 
                       requested_variant, 
                       VARIANTS.iter().map(|v| v.test_name).collect::<Vec<_>>().join(", "));
            }
        }
    };

    // Generate a test function for each requested variant
    let test_functions = variants_to_test.iter().filter_map(|variant_name| {
        VARIANTS.iter().find(|v| v.test_name == *variant_name).map(|variant| {
            // Always prefix with the test_name
            let test_fn_name = Ident::new(&format!("{}_{}", variant.test_name, fn_name), fn_name.span());
            
            let transport_type: proc_macro2::TokenStream = variant.transport_type.parse().unwrap();
            let _options = (variant.options)(&quote! {});

            quote! {
                #[tokio::test]
                #fn_vis async fn #test_fn_name() {
                    use crate::utils::LocalInstance;
                    use crate::utils::instance::TestTransport;

                    let mut instance = <#transport_type>::new()
                        .await
                        .expect("Failed to create LocalInstance");

                    let test_fn = |mut instance: LocalInstance<#transport_type>| async move #fn_body;
                    test_fn(instance).await;
                }
            }
        })
    }).collect::<Vec<_>>();

    let output = quote! {
        #(#test_functions)*
    };

    TokenStream::from(output)
}
