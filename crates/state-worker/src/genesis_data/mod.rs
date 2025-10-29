mod linea_devnet;
mod linea_sepolia;

use serde_json::Value;

/// Return the embedded genesis JSON for the given chain id, if available.
pub(crate) fn for_chain_id(chain_id: u64) -> Option<&'static Value> {
    match chain_id {
        linea_sepolia::CHAIN_ID => Some(&linea_sepolia::GENESIS),
        linea_devnet::CHAIN_ID => Some(&linea_devnet::GENESIS),
        _ => None,
    }
}
