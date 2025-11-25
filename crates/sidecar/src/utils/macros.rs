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
