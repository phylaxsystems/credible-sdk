//! Geth version validation for prestateTracer diffMode correctness.
//!
//! Geth versions before 1.16.6 have a bug where the `prestateTracer` diffMode
//! incorrectly reports post-Cancun SELFDESTRUCT operations as full account
//! deletions (pre present, post absent), even though the contract still exists
//! per EIP-6780 semantics.
//!
//! Issue: <https://github.com/ethereum/go-ethereum/issues/33049>
//! Fix: <https://github.com/ethereum/go-ethereum/pull/33050>

/// Minimum required Geth version for correct prestateTracer diffMode behavior.
pub const MIN_GETH_VERSION: (u64, u64, u64) = (1, 16, 6);

/// Error returned when the connected Geth node version is too old.
#[derive(Debug)]
pub struct GethVersionError {
    pub current: (u64, u64, u64),
    pub minimum: (u64, u64, u64),
}

impl std::fmt::Display for GethVersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (cur_major, cur_minor, cur_patch) = self.current;
        let (min_major, min_minor, min_patch) = self.minimum;
        write!(
            f,
            "Geth version {cur_major}.{cur_minor}.{cur_patch} is below minimum required version \
             {min_major}.{min_minor}.{min_patch}. Geth versions before {min_major}.{min_minor}.{min_patch} \
             have a known prestateTracer diffMode bug that incorrectly reports post-Cancun \
             SELFDESTRUCT as account deletions, violating EIP-6780. \
             See https://github.com/ethereum/go-ethereum/issues/33049 for details. \
             Please upgrade your Geth node."
        )
    }
}

impl std::error::Error for GethVersionError {}

/// Parse a Geth version string and extract the semantic version tuple.
///
/// Expected formats:
/// - `Geth/v1.16.6-stable-abc123/linux-amd64/go1.23`
/// - `Geth/v1.16.6/linux-amd64/go1.23`
/// - `Geth/v1.16.6-unstable/...`
///
/// Returns `Some((major, minor, patch))` if this is a Geth client with
/// a parseable version, `None` otherwise.
pub fn parse_geth_version(client_version: &str) -> Option<(u64, u64, u64)> {
    // Strip "Geth/" prefix (case-sensitive, but also check lowercase)
    let remainder = client_version
        .strip_prefix("Geth/")
        .or_else(|| client_version.strip_prefix("geth/"))?;

    // Strip optional 'v' prefix
    let remainder = remainder.strip_prefix('v').unwrap_or(remainder);

    // Find the end of the version number (before "-" or "/" or end of string)
    let version_end = remainder.find(['-', '/']).unwrap_or(remainder.len());
    let version = remainder.get(..version_end)?;

    // Parse "major.minor.patch"
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;

    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_geth_version_standard() {
        assert_eq!(
            parse_geth_version("Geth/v1.16.6-stable-abc123/linux-amd64/go1.23"),
            Some((1, 16, 6))
        );
    }

    #[test]
    fn test_parse_geth_version_without_suffix() {
        assert_eq!(
            parse_geth_version("Geth/v1.16.6/linux-amd64/go1.23"),
            Some((1, 16, 6))
        );
    }

    #[test]
    fn test_parse_geth_version_unstable() {
        assert_eq!(
            parse_geth_version("Geth/v1.17.0-unstable-deadbeef/linux-amd64/go1.24"),
            Some((1, 17, 0))
        );
    }

    #[test]
    fn test_parse_geth_version_old() {
        assert_eq!(
            parse_geth_version("Geth/v1.16.5-stable-abc123/linux-amd64/go1.23"),
            Some((1, 16, 5))
        );
    }

    #[test]
    fn test_parse_geth_version_without_v_prefix() {
        assert_eq!(
            parse_geth_version("Geth/1.16.6-stable/linux-amd64/go1.23"),
            Some((1, 16, 6))
        );
    }

    #[test]
    fn test_parse_geth_version_not_geth() {
        assert_eq!(
            parse_geth_version("Erigon/v2.60.0/linux-amd64/go1.23"),
            None
        );
        assert_eq!(
            parse_geth_version("Nethermind/v1.25.0/linux-x64/dotnet8"),
            None
        );
    }

    #[test]
    fn test_parse_geth_version_invalid() {
        assert_eq!(parse_geth_version("Geth/invalid"), None);
        assert_eq!(parse_geth_version("Geth/v1.16"), None);
        assert_eq!(parse_geth_version(""), None);
    }

    #[test]
    fn test_version_comparison() {
        let min = MIN_GETH_VERSION;

        // Versions that should pass
        assert!((1, 16, 6) >= min);
        assert!((1, 16, 7) >= min);
        assert!((1, 17, 0) >= min);
        assert!((2, 0, 0) >= min);

        // Versions that should fail
        assert!((1, 16, 5) < min);
        assert!((1, 15, 10) < min);
        assert!((0, 99, 99) < min);
    }

    #[test]
    fn test_parsed_version_comparison() {
        let old = parse_geth_version("Geth/v1.16.5-stable-abc/linux-amd64/go1.23").unwrap();
        let exact = parse_geth_version("Geth/v1.16.6-stable-abc/linux-amd64/go1.23").unwrap();
        let new = parse_geth_version("Geth/v1.17.0-stable-abc/linux-amd64/go1.23").unwrap();

        assert!(old < MIN_GETH_VERSION);
        assert!(exact >= MIN_GETH_VERSION);
        assert!(new >= MIN_GETH_VERSION);
    }
}
