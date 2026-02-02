use alloy_primitives::{
    B256,
    keccak256,
};
use revm::context::TxEnv;

/// Compute a content hash of a `TxEnv` that is invariant to nonce, gas_price, and
/// gas_priority_fee.
///
/// Fields included: tx_type, caller, gas_limit, kind, value, data, chain_id,
/// access_list, blob_hashes, max_fee_per_blob_gas, authorization_list.
///
/// This allows deduplicating transactions whose content is identical even when the
/// sender bumps nonce or adjusts gas parameters.
#[allow(clippy::cast_possible_truncation)]
pub fn tx_content_hash(tx: &TxEnv) -> B256 {
    // Pre-allocate a reasonable buffer size
    let mut buf: Vec<u8> = Vec::with_capacity(256);

    // tx_type (1 byte)
    buf.push(tx.tx_type);

    // caller (20 bytes)
    buf.extend_from_slice(tx.caller.as_slice());

    // gas_limit (8 bytes BE)
    buf.extend_from_slice(&tx.gas_limit.to_be_bytes());

    // kind: discriminant + optional address
    match &tx.kind {
        revm::primitives::TxKind::Create => buf.push(0),
        revm::primitives::TxKind::Call(addr) => {
            buf.push(1);
            buf.extend_from_slice(addr.as_slice());
        }
    }

    // value (32 bytes BE)
    buf.extend_from_slice(&tx.value.to_be_bytes::<32>());

    // data (length-prefixed)
    buf.extend_from_slice(&(tx.data.len() as u32).to_be_bytes());
    buf.extend_from_slice(&tx.data);

    // chain_id (optional: 0 byte for None, 1 byte + 8 bytes BE for Some)
    match tx.chain_id {
        None => buf.push(0),
        Some(id) => {
            buf.push(1);
            buf.extend_from_slice(&id.to_be_bytes());
        }
    }

    // access_list (length-prefixed items)
    buf.extend_from_slice(&(tx.access_list.0.len() as u32).to_be_bytes());
    for item in &tx.access_list.0 {
        buf.extend_from_slice(item.address.as_slice());
        buf.extend_from_slice(&(item.storage_keys.len() as u32).to_be_bytes());
        for key in &item.storage_keys {
            buf.extend_from_slice(key.as_slice());
        }
    }

    // blob_hashes (length-prefixed)
    buf.extend_from_slice(&(tx.blob_hashes.len() as u32).to_be_bytes());
    for hash in &tx.blob_hashes {
        buf.extend_from_slice(hash.as_slice());
    }

    // max_fee_per_blob_gas (16 bytes BE)
    buf.extend_from_slice(tx.max_fee_per_blob_gas.to_be_bytes().as_slice());

    // authorization_list (length-prefixed)
    buf.extend_from_slice(&(tx.authorization_list.len() as u32).to_be_bytes());
    for auth in &tx.authorization_list {
        let inner: &alloy::eips::eip7702::Authorization = match auth {
            alloy::signers::Either::Left(signed) => signed.inner(),
            alloy::signers::Either::Right(recovered) => recovered,
        };
        buf.extend_from_slice(&inner.chain_id().to_be_bytes::<32>());
        buf.extend_from_slice(inner.address().as_slice());
        buf.extend_from_slice(&inner.nonce().to_be_bytes());
    }

    keccak256(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::{
        eips::eip7702::{
            Authorization,
            SignedAuthorization,
        },
        signers::Either,
    };
    use alloy_primitives::U256;
    use revm::{
        context::transaction::{
            AccessList,
            AccessListItem,
        },
        primitives::{
            Address,
            Bytes,
            TxKind,
        },
    };

    fn base_tx() -> TxEnv {
        TxEnv {
            tx_type: 2,
            caller: Address::from([0x01; 20]),
            gas_limit: 21_000,
            gas_price: 100,
            kind: TxKind::Call(Address::from([0x02; 20])),
            value: U256::from(1000),
            data: Bytes::from(vec![0xde, 0xad]),
            nonce: 5,
            chain_id: Some(1),
            access_list: AccessList::default(),
            gas_priority_fee: Some(10),
            blob_hashes: Vec::new(),
            max_fee_per_blob_gas: 0,
            authorization_list: Vec::new(),
        }
    }

    #[test]
    fn same_tx_produces_same_hash() {
        let tx = base_tx();
        assert_eq!(tx_content_hash(&tx), tx_content_hash(&tx));
    }

    #[test]
    fn nonce_change_does_not_affect_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.nonce = 999;
        assert_eq!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn gas_price_change_does_not_affect_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.gas_price = 999_999;
        assert_eq!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn gas_priority_fee_change_does_not_affect_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.gas_priority_fee = Some(999);
        assert_eq!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn caller_change_affects_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.caller = Address::from([0xff; 20]);
        assert_ne!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn value_change_affects_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.value = U256::from(9999);
        assert_ne!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn data_change_affects_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.data = Bytes::from(vec![0xbe, 0xef]);
        assert_ne!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn kind_change_affects_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.kind = TxKind::Create;
        assert_ne!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn chain_id_change_affects_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.chain_id = Some(42);
        assert_ne!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn gas_limit_change_affects_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.gas_limit = 100_000;
        assert_ne!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn access_list_change_affects_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.access_list = AccessList(vec![AccessListItem {
            address: Address::from([0x03; 20]),
            storage_keys: vec![B256::from([0x04; 32])],
        }]);
        assert_ne!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn tx_type_change_affects_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        tx2.tx_type = 3;
        assert_ne!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }

    #[test]
    fn authorization_list_change_affects_hash() {
        let tx1 = base_tx();
        let mut tx2 = base_tx();
        let auth = Authorization {
            chain_id: U256::from(1),
            address: Address::from([0x05; 20]),
            nonce: 0,
        };
        let signed = SignedAuthorization::new_unchecked(auth, 0, U256::ZERO, U256::ZERO);
        tx2.authorization_list = vec![Either::Left(signed)];
        assert_ne!(tx_content_hash(&tx1), tx_content_hash(&tx2));
    }
}
