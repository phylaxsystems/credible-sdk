/// Macro to build the appropriate EVM based on feature flags.
///
/// This macro handles the complex cfg selects for different EVM types:
/// - Linea EVM when `linea` feature is enabled
/// - Optimism EVM when `optimism` feature is enabled (but not `linea`)
/// - Ethereum mainnet EVM as default
///
/// # Arguments
/// * `$db` - Database reference
/// * `$env` - EVM environment reference
/// * `$inspector` - Inspector instance
///
/// # Returns
/// Returns the constructed EVM instance
///
/// # Example
/// ```rust,ignore
/// let mut evm = build_evm_by_features!(&mut db, &env, inspector);
/// ```
#[macro_export]
macro_rules! build_evm_by_features {
    ($db:expr, $env:expr, $inspector:expr) => {{
        // ensure only one EVM feature is enabled
        #[cfg(all(feature = "linea", feature = "optimism"))]
        compile_error!("Cannot enable both 'linea' and 'optimism' features simultaneously. Please enable only one EVM feature at a time.");

        #[cfg(feature = "linea")]
        {
            $crate::evm::linea::build_linea_evm($db, $env, $inspector)
        }

        #[cfg(all(feature = "optimism", not(feature = "linea")))]
        {
            $crate::evm::build_evm::build_optimism_evm($db, $env, $inspector)
        }

        #[cfg(all(not(feature = "optimism"), not(feature = "linea")))]
        {
            $crate::evm::build_evm::build_eth_evm($db, $env, $inspector)
        }
    }};
}

/// Macro to conditionally wrap a `TxEnv` for Optimism if needed.
///
/// This macro either returns the original `TxEnv` unchanged, or wraps it in
/// `OpTransaction::new()` if the optimism feature is enabled.
///
/// # Arguments
/// * `$tx_env` - The transaction environment to potentially wrap
///
/// # Returns
/// Returns either the original `TxEnv` or `OpTransaction::new(TxEnv)` for Optimism
///
/// # Example
/// ```rust,ignore
/// let tx_env = wrap_tx_env_for_optimism!(tx_env);
/// ```
#[macro_export]
macro_rules! wrap_tx_env_for_optimism {
    ($tx_env:expr) => {{
        // ensure only one EVM feature is enabled
        #[cfg(all(feature = "linea", feature = "optimism"))]
        compile_error!("Cannot enable both 'linea' and 'optimism' features simultaneously. Please enable only one EVM feature at a time.");

        #[cfg(all(feature = "optimism", not(feature = "linea")))]
        {
            op_revm::OpTransaction::new($tx_env)
        }

        #[cfg(any(feature = "linea", not(feature = "optimism")))]
        {
            $tx_env
        }
    }};
}

#[cfg(test)]
mod tests {
    use crate::{
        evm::build_evm::evm_env,
        inspectors::CallTracer,
        primitives::{
            BlockEnv,
            SpecId,
            TxEnv,
        },
    };
    use revm::database::InMemoryDB;

    #[test]
    fn test_build_evm_by_features_compiles() {
        let mut db = InMemoryDB::default();
        let env = evm_env(1, SpecId::default(), BlockEnv::default());
        let inspector = CallTracer::default();

        // This should compile for any feature combination
        let _evm = build_evm_by_features!(&mut db, &env, inspector);
    }

    #[test]
    fn test_wrap_tx_env_for_optimism_compiles() {
        let tx_env = TxEnv::default();

        // This should compile for any feature combination
        let _wrapped_tx = wrap_tx_env_for_optimism!(tx_env);
    }
}
