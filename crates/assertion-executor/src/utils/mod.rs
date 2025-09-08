pub mod reorg_utils;

use crate::primitives::{
    Address,
    TxEnv,
    TxKind,
};

use alloy_consensus::TxEnvelope;

/// Used to fill a `TxEnv` with the data from a `TxEnvelope`
pub fn fill_tx_env(input_tx: TxEnvelope, tx_env: &mut TxEnv, sender: Address) {
    tx_env.caller = sender;
    match input_tx {
        TxEnvelope::Legacy(tx) => {
            let tx = tx.tx();
            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = tx.gas_price;
            tx_env.gas_priority_fee = None;
            tx_env.kind = tx.to;
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = tx.chain_id;
            tx_env.nonce = tx.nonce;
            tx_env.access_list = vec![].into();
            tx_env.blob_hashes.clear();
            tx_env.max_fee_per_blob_gas = 0;
            tx_env.authorization_list = vec![];
        }
        TxEnvelope::Eip2930(tx) => {
            let tx = tx.tx();
            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = tx.gas_price;
            tx_env.gas_priority_fee = None;
            tx_env.kind = tx.to;
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = Some(tx.chain_id);
            tx_env.nonce = tx.nonce;
            tx_env.access_list = tx.access_list.0.clone().into();
            tx_env.blob_hashes.clear();
            tx_env.max_fee_per_blob_gas = 0;
            tx_env.authorization_list = vec![];
        }
        TxEnvelope::Eip1559(tx) => {
            let tx = tx.tx();
            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = tx.max_fee_per_gas;
            tx_env.gas_priority_fee = Some(tx.max_priority_fee_per_gas);
            tx_env.kind = tx.to;
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = Some(tx.chain_id);
            tx_env.nonce = tx.nonce;
            tx_env.access_list = tx.access_list.0.clone().into();
            tx_env.blob_hashes.clear();
            tx_env.max_fee_per_blob_gas = 0;
            tx_env.authorization_list = vec![];
        }
        TxEnvelope::Eip4844(tx) => {
            let tx = tx.tx().tx();
            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = tx.max_fee_per_gas;
            tx_env.gas_priority_fee = Some(tx.max_priority_fee_per_gas);
            tx_env.kind = TxKind::Call(tx.to);
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = Some(tx.chain_id);
            tx_env.nonce = tx.nonce;
            tx_env.access_list = tx.access_list.0.clone().into();
            tx_env.blob_hashes.clone_from(&tx.blob_versioned_hashes);
            tx_env.max_fee_per_blob_gas = tx.max_fee_per_blob_gas;
            tx_env.authorization_list = vec![];
        }
        TxEnvelope::Eip7702(tx) => {
            let tx = tx.tx();
            let auth_list =
                tx.authorization_list
                    .iter()
                    .cloned()
                    .fold(vec![], |mut auth_list, auth_item| {
                        let auth_item_revm =
                        revm::context_interface::transaction::SignedAuthorization::new_unchecked(
                            auth_item.inner().to_owned(),
                            auth_item.y_parity(),
                            auth_item.r(),
                            auth_item.s(),
                        );

                        auth_list.push(alloy_signer::Either::Left(auth_item_revm));
                        auth_list
                    });

            tx_env.authorization_list = auth_list;

            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = tx.max_fee_per_gas;
            tx_env.gas_priority_fee = Some(tx.max_priority_fee_per_gas);
            tx_env.kind = TxKind::Call(tx.to);
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = Some(tx.chain_id);
            tx_env.nonce = tx.nonce;
            tx_env.access_list = tx.access_list.0.clone().into();
            tx_env.blob_hashes.clear();
            tx_env.max_fee_per_blob_gas = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        primitives::{
            Address,
            Bytes,
            U256,
        },
        test_utils::random_bytes,
    };
    use alloy_consensus::{
        Signed,
        TxEip7702,
    };
    use alloy_eips::eip7702::{
        Authorization,
        SignedAuthorization,
    };
    use rand::random;

    use alloy::primitives::Signature;
    use alloy_rpc_types::AccessList;

    #[test]
    fn test_fill_tx_env_eip7702() {
        let gas_limit = random();
        let max_fee_per_gas = random();
        let max_priority_fee_per_gas = random();
        let to = random_bytes().into();
        let value = random_bytes().into();
        let input = Bytes::default();
        let chain_id = random();
        let nonce = random();
        let access_list = AccessList::default();

        let auth_item_address = Address::random();
        let auth_item_chain_id = U256::default();
        let auth_item_nonce = random();
        let sender = Address::random();

        let mut authorization_list = vec![];

        for _ in 0..random::<u8>() {
            let auth_item = Authorization {
                address: auth_item_address,
                chain_id: auth_item_chain_id,
                nonce: auth_item_nonce,
            };

            let signed_auth_item = SignedAuthorization::new_unchecked(
                auth_item,
                1,
                random_bytes().into(),
                random_bytes().into(),
            );

            authorization_list.push(signed_auth_item);
        }

        let tx = TxEip7702 {
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to,
            value,
            input,
            chain_id,
            nonce,
            access_list,
            authorization_list,
        };

        let primitive_signature =
            Signature::new(random_bytes().into(), random_bytes().into(), random());

        let signed_tx = Signed::new_unchecked(tx.clone(), primitive_signature, random_bytes());

        let tx_envelope = TxEnvelope::Eip7702(signed_tx);

        let mut tx_env = TxEnv::default();

        fill_tx_env(tx_envelope, &mut tx_env, sender);

        assert_eq!(tx_env.gas_limit, tx.gas_limit);
        assert_eq!(tx_env.gas_price, tx.max_fee_per_gas);
        assert_eq!(tx_env.gas_priority_fee, Some(tx.max_priority_fee_per_gas));
        assert_eq!(tx_env.value, value);
        assert_eq!(tx_env.data, Bytes::default());
        assert_eq!(tx_env.chain_id, Some(chain_id));
        assert_eq!(tx_env.max_fee_per_blob_gas, 0);
        for (i, auth_item) in tx.authorization_list.iter().enumerate() {
            let tx_env_item = tx_env.authorization_list[i].clone();
            assert_eq!(tx_env_item.address, auth_item.address);
            assert_eq!(tx_env_item.chain_id, auth_item.chain_id);
            assert_eq!(tx_env_item.nonce, auth_item.nonce);

            let auth_item_sig = auth_item.signature().unwrap();

            let tx_env_item_sig = tx_env_item.unwrap_left().signature().unwrap();
            assert_eq!(tx_env_item_sig.v(), auth_item_sig.v());
            assert_eq!(tx_env_item_sig.r(), auth_item_sig.r());
            assert_eq!(tx_env_item_sig.s(), auth_item_sig.s());
        }
    }
}
