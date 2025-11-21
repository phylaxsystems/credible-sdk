/// Macro for logging critical errors. It is a wrapper around `tracing::error!` with alert = true
/// and severity = "critical".
#[macro_export]
macro_rules! critical {
    ($($arg:tt)*) => {
        ::tracing::error!(
            alert = true,
            severity = "critical",
            $($arg)*
        )
    };
}

/// Macro to wrap async code in panic recovery using `LocalSet` for !Send types
/// Returns Ok(true) to break loop, Ok(false) to continue, Err on panic
#[macro_export]
macro_rules! with_panic_recovery {
    ($async_block:expr) => {{
        async {
            let local = tokio::task::LocalSet::new();
            let handle = local.spawn_local(async move {
                $async_block.await
            });

            match local.run_until(handle).await {
                Ok(Ok(should_break)) => Ok(should_break),
                Ok(Err(e)) => {
                    tracing::error!(error = ?e, "Error in recovery block, restarting...");
                    Err(())
                }
                Err(join_err) => {
                    if join_err.is_panic() {
                        tracing::error!(
                            panic = ?join_err,
                            "Panic caught. Recovering and restarting..."
                        );
                    } else {
                        tracing::error!(
                            error = ?join_err,
                            "Task cancelled or failed, restarting..."
                        );
                    }
                    Err(())
                }
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_panic_recovery() {
        let result = with_panic_recovery!(async { Ok::<bool, anyhow::Error>(false) }).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_error_recovery() {
        let result = with_panic_recovery!(async {
            Err::<bool, anyhow::Error>(anyhow::anyhow!("test error"))
        })
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_panic_caught() {
        let result = with_panic_recovery!(async {
            panic!("test panic");
            #[allow(unreachable_code)]
            Ok::<bool, anyhow::Error>(false)
        })
        .await;
        assert!(result.is_err());
    }
}
