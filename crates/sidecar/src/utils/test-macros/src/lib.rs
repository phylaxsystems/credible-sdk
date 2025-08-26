use proc_macro::TokenStream;
use quote::quote;
use syn::{
    ItemFn,
    parse_macro_input,
};

/// Procedural macro for sidecar engine tests.
///
/// This macro automatically creates a LocalInstance and passes it to your test function.
/// The test function must be async and take a single parameter of type LocalInstance.
///
/// # Example
/// ```
/// #[engine_test]
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
pub fn engine_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let fn_name = &input_fn.sig.ident;
    let fn_body = &input_fn.block;
    let fn_vis = &input_fn.vis;

    let output = quote! {
        #[tokio::test]
        #fn_vis async fn #fn_name() {
            use crate::utils::LocalInstance;

            let mut instance = LocalInstance::new()
                .await
                .expect("Failed to create LocalInstance");

            let test_fn = |mut instance: LocalInstance| async move #fn_body;
            test_fn(instance).await;
        }
    };

    TokenStream::from(output)
}
