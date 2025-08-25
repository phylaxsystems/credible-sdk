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
