//! Procedural macros for state worker integration tests.
//!
//! This crate provides the `database_test` macro which automatically generates
//! test functions for multiple database backends (Redis, MDBX).
#![allow(clippy::missing_panics_doc)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Ident,
    ItemFn,
    parse_macro_input,
};

/// Contains info about a single database backend variant.
struct BackendVariant {
    /// Name of the test variant, and what the test should be prefixed with
    test_name: &'static str,
    /// Constructor method on `TestInstance`
    constructor: &'static str,
}

const VARIANTS: &[BackendVariant] = &[
    BackendVariant {
        test_name: "redis",
        constructor: "new_redis",
    },
    BackendVariant {
        test_name: "mdbx",
        constructor: "new_mdbx",
    },
];

/// Procedural macro for database integration tests.
///
/// This macro automatically creates a `TestInstance` for each specified backend
/// and passes it to your test function. The test function must be async and take
/// a single parameter of type `TestInstance`.
///
/// # Arguments
///
/// - `all` - Generate tests for all backends (redis, mdbx)
/// - `redis` - Generate test only for Redis backend
/// - `mdbx` - Generate test only for MDBX backend
///
/// # Example
///
/// ```rust,ignore
/// use crate::integration_tests::TestInstance;
///
/// #[database_test(all)]
/// async fn test_state_changes(instance: TestInstance) {
///     instance.http_server_mock.send_new_head();
///     // Your test code here
/// }
/// ```
///
/// This generates:
/// - `redis_test_state_changes`
/// - `mdbx_test_state_changes`
///
/// # Panics
///
/// Will panic if no argument is provided or if an unknown backend is specified.
#[proc_macro_attribute]
pub fn database_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let fn_name = &input_fn.sig.ident;
    let fn_body = &input_fn.block;
    let fn_vis = &input_fn.vis;
    let fn_attrs = &input_fn.attrs;

    // Parse which variants to test - require arguments
    let variants_to_test: Vec<&'static str> = if attr.is_empty() {
        panic!(
            "database_test macro requires an argument: use #[database_test(all)], \
             #[database_test(redis)], or #[database_test(mdbx)]"
        );
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
                panic!(
                    "Unknown backend '{}'. Available backends: {} or 'all'",
                    requested_variant,
                    VARIANTS
                        .iter()
                        .map(|v| v.test_name)
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
    };

    // Generate a test function for each requested variant
    let test_functions = variants_to_test
        .iter()
        .filter_map(|variant_name| {
            VARIANTS
                .iter()
                .find(|v| v.test_name == *variant_name)
                .map(|variant| {
                    let test_fn_name = Ident::new(
                        &format!("{}_{}", variant.test_name, fn_name),
                        fn_name.span(),
                    );
                    let constructor =
                        Ident::new(variant.constructor, proc_macro2::Span::call_site());

                    quote! {
                        #(#fn_attrs)*
                        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
                        #fn_vis async fn #test_fn_name() {
                            let instance = super::TestInstance::#constructor()
                                .await
                                .expect(concat!(
                                    "Failed to create ",
                                    stringify!(#test_fn_name),
                                    " test instance"
                                ));

                            let test_fn = |instance: super::TestInstance| async move #fn_body;
                            test_fn(instance).await
                        }
                    }
                })
        })
        .collect::<Vec<_>>();

    let output = quote! {
        #(#test_functions)*
    };

    TokenStream::from(output)
}

/// Procedural macro for traced database integration tests.
///
/// Same as `database_test` but adds `#[tracing_test::traced_test]` attribute
/// for tests that need to check log output with `logs_contain()`.
///
/// # Example
///
/// ```rust,ignore
/// use crate::integration_tests::TestInstance;
///
/// #[traced_database_test(all)]
/// async fn test_critical_alert(instance: TestInstance) {
///     // ... trigger critical alert ...
///     assert!(logs_contain("critical"));
/// }
/// ```
#[proc_macro_attribute]
pub fn traced_database_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let fn_name = &input_fn.sig.ident;
    let fn_body = &input_fn.block;
    let fn_vis = &input_fn.vis;
    let fn_attrs = &input_fn.attrs;

    // Parse which variants to test - require arguments
    let variants_to_test: Vec<&'static str> = if attr.is_empty() {
        panic!(
            "traced_database_test macro requires an argument: use #[traced_database_test(all)], \
             #[traced_database_test(redis)], or #[traced_database_test(mdbx)]"
        );
    } else {
        let attr_str = attr.to_string();
        if attr_str.trim() == "all" {
            VARIANTS.iter().map(|v| v.test_name).collect()
        } else {
            let requested_variant = attr_str.trim();
            if let Some(variant) = VARIANTS.iter().find(|v| v.test_name == requested_variant) {
                vec![variant.test_name]
            } else {
                panic!(
                    "Unknown backend '{}'. Available backends: {} or 'all'",
                    requested_variant,
                    VARIANTS
                        .iter()
                        .map(|v| v.test_name)
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
    };

    // Generate a test function for each requested variant
    let test_functions = variants_to_test
        .iter()
        .filter_map(|variant_name| {
            VARIANTS
                .iter()
                .find(|v| v.test_name == *variant_name)
                .map(|variant| {
                    let test_fn_name = Ident::new(
                        &format!("{}_{}", variant.test_name, fn_name),
                        fn_name.span(),
                    );
                    let constructor =
                        Ident::new(variant.constructor, proc_macro2::Span::call_site());

                    quote! {
                        #(#fn_attrs)*
                        #[tracing_test::traced_test]
                        #[tokio::test]
                        #fn_vis async fn #test_fn_name() {
                            let instance = super::TestInstance::#constructor()
                                .await
                                .expect(concat!(
                                    "Failed to create ",
                                    stringify!(#test_fn_name),
                                    " test instance"
                                ));

                            let test_fn = |instance: super::TestInstance| async move #fn_body;
                            test_fn(instance).await
                        }
                    }
                })
        })
        .collect::<Vec<_>>();

    let output = quote! {
        #(#test_functions)*
    };

    TokenStream::from(output)
}
