use crate::{
    inspectors::{
        phevm::PhevmOutcome,
        precompiles::deduct_gas_and_check,
        sol_primitives::PhEvm,
    },
    primitives::{
        Address,
        Bytes,
    },
};

use alloy_sol_types::SolCall;
use std::{
    collections::HashSet,
    str::FromStr,
};
use tracing::warn;

use super::BASE_COST;

const SANCTIONS_CHECK_COST: u64 = 18;
pub const OFAC_CSV_PATH_ENV: &str = "PH_OFAC_SANCTIONS_CSV_PATH";
const FALLBACK_SANCTIONS_CSV: &str = include_str!("ofac_eth_fallback.csv");

#[derive(Debug, thiserror::Error)]
pub enum OfacError {
    #[error("Failed to decode revertIfSanctioned input: {0}")]
    FailedToDecodeCall(#[source] alloy_sol_types::Error),
    #[error("Address {0} is on the OFAC sanctions list")]
    AddressSanctioned(Address),
}

/// Reverts when `account` is present in the OFAC sanctions set.
pub fn revert_if_sanctioned(
    input_bytes: &Bytes,
    gas_limit: u64,
    sanctions: &HashSet<Address>,
) -> Result<PhevmOutcome, OfacError> {
    let mut gas_left = gas_limit;
    if let Some(oog) =
        deduct_gas_and_check(&mut gas_left, BASE_COST + SANCTIONS_CHECK_COST, gas_limit)
    {
        return Ok(oog);
    }

    let call = PhEvm::revertIfSanctionedCall::abi_decode(input_bytes)
        .map_err(OfacError::FailedToDecodeCall)?;

    if sanctions.contains(&call.account) {
        return Err(OfacError::AddressSanctioned(call.account));
    }

    Ok(PhevmOutcome::new(Bytes::default(), gas_limit - gas_left))
}

/// Loads OFAC sanctions addresses from `PH_OFAC_SANCTIONS_CSV_PATH` or falls back to the bundled snapshot.
pub fn load_sanctions_from_env_or_fallback() -> HashSet<Address> {
    if let Ok(path) = std::env::var(OFAC_CSV_PATH_ENV) {
        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                let parsed = parse_addresses_from_csv(&contents);
                if parsed.is_empty() {
                    warn!(
                        path = %path,
                        "OFAC CSV from PH_OFAC_SANCTIONS_CSV_PATH produced zero addresses, using fallback snapshot"
                    );
                } else {
                    return parsed;
                }
            }
            Err(err) => {
                warn!(
                    path = %path,
                    error = %err,
                    "Failed to read PH_OFAC_SANCTIONS_CSV_PATH, using fallback snapshot"
                );
            }
        }
    }

    parse_addresses_from_csv(FALLBACK_SANCTIONS_CSV)
}

/// Parses ethereum addresses from CSV-like input.
pub fn parse_addresses_from_csv(contents: &str) -> HashSet<Address> {
    let mut sanctions = HashSet::new();

    for line in contents.lines() {
        for token in line.split(',') {
            if let Some(address) = parse_address_token(token) {
                sanctions.insert(address);
            }
        }
    }

    sanctions
}

fn parse_address_token(token: &str) -> Option<Address> {
    let value =
        token.trim_matches(|c: char| c.is_ascii_whitespace() || c == '"' || c == '\'' || c == ';');

    if value.len() != 42 || !value.starts_with("0x") {
        return None;
    }

    if !value[2..].chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    Address::from_str(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_addresses_from_csv_extracts_eth_addresses() {
        let csv = "address\n0x8576aCc5C05D6cE88f4e49bF65BDF0C62F91353C\n";
        let sanctions = parse_addresses_from_csv(csv);
        let expected = Address::from_str("0x8576aCc5C05D6cE88f4e49bF65BDF0C62F91353C").unwrap();
        assert!(sanctions.contains(&expected));
    }

    #[test]
    fn test_parse_addresses_from_csv_ignores_non_eth_tokens() {
        let csv = "value\nhello,0x8576aCc5C05D6cE88f4e49bF65BDF0C62F91353C,123\n";
        let sanctions = parse_addresses_from_csv(csv);
        let expected = Address::from_str("0x8576aCc5C05D6cE88f4e49bF65BDF0C62F91353C").unwrap();
        assert_eq!(sanctions.len(), 1);
        assert!(sanctions.contains(&expected));
    }
}
