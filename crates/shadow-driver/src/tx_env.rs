//! Shared helpers for encoding transaction data into sidecar transport protobufs.

use alloy::{
    consensus::{
        Transaction as _,
        TxType,
    },
    primitives::U256,
    rpc::types::Transaction,
};
use sidecar::transport::grpc::pb::{
    AccessListItem as ProtoAccessListItem,
    Authorization as ProtoAuthorization,
    TransactionEnv,
};

/// Converts an Alloy RPC transaction into a sidecar protobuf `TransactionEnv`.
#[must_use]
pub fn to_proto_tx_env(tx: &Transaction) -> TransactionEnv {
    let tx_type = match tx.inner.tx_type() {
        TxType::Legacy => 0,
        TxType::Eip2930 => 1,
        TxType::Eip1559 => 2,
        TxType::Eip4844 => 3,
        TxType::Eip7702 => 4,
    };

    let access_list: Vec<ProtoAccessListItem> = tx
        .access_list()
        .map(|al| {
            al.0.iter()
                .map(|item| {
                    ProtoAccessListItem {
                        address: item.address.to_vec(),
                        storage_keys: item.storage_keys.iter().map(|k| k.to_vec()).collect(),
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let blob_hashes: Vec<Vec<u8>> = tx
        .blob_versioned_hashes()
        .map(|hashes| hashes.iter().map(|h| h.to_vec()).collect())
        .unwrap_or_default();

    let max_fee_per_blob_gas = tx
        .max_fee_per_blob_gas()
        .map_or_else(|| u128_to_bytes(0), u128_to_bytes);

    let authorization_list: Vec<ProtoAuthorization> = tx
        .authorization_list()
        .map(|auths| {
            auths
                .iter()
                .filter_map(|auth| {
                    let sig = auth.signature().ok()?;
                    Some(ProtoAuthorization {
                        chain_id: U256::from(auth.chain_id).to_be_bytes_vec(),
                        address: auth.address.to_vec(),
                        nonce: auth.nonce,
                        y_parity: u32::from(sig.v()),
                        r: sig.r().to_be_bytes_vec(),
                        s: sig.s().to_be_bytes_vec(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let (gas_price, gas_priority_fee) = match tx.inner.tx_type() {
        TxType::Legacy | TxType::Eip2930 => {
            (u128_to_bytes(tx.inner.gas_price().unwrap_or(0)), None)
        }
        TxType::Eip1559 | TxType::Eip4844 | TxType::Eip7702 => {
            let max_fee = tx.inner.max_fee_per_gas();
            let max_priority = tx.inner.max_priority_fee_per_gas();
            (u128_to_bytes(max_fee), max_priority.map(u128_to_bytes))
        }
    };

    TransactionEnv {
        tx_type,
        caller: tx.inner.signer().to_vec(),
        gas_limit: tx.inner.gas_limit(),
        gas_price,
        transact_to: tx.to().map(|a| a.to_vec()).unwrap_or_default(),
        value: u256_to_bytes(tx.value()),
        data: tx.input().to_vec(),
        nonce: tx.nonce(),
        chain_id: tx.chain_id(),
        access_list,
        gas_priority_fee,
        blob_hashes,
        max_fee_per_blob_gas,
        authorization_list,
    }
}

/// Encodes a `u128` as big-endian bytes.
#[must_use]
pub fn u128_to_bytes(value: u128) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Encodes a `U256` as big-endian bytes.
#[must_use]
pub fn u256_to_bytes(value: U256) -> Vec<u8> {
    value.to_be_bytes_vec()
}

#[cfg(test)]
mod tests {
    use super::{
        u128_to_bytes,
        u256_to_bytes,
    };
    use alloy::primitives::U256;

    #[test]
    fn u128_to_bytes_is_big_endian() {
        let value: u128 = 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10;
        let bytes = u128_to_bytes(value);
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[0], 0x01);
        assert_eq!(bytes[15], 0x10);
    }

    #[test]
    fn u256_to_bytes_is_big_endian() {
        let value = U256::from(0x0102_u64);
        let bytes = u256_to_bytes(value);
        assert_eq!(bytes.len(), 32);
        assert_eq!(bytes[30], 0x01);
        assert_eq!(bytes[31], 0x02);
    }
}
