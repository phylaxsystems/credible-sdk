pub mod reorg_utils;

use crate::primitives::{
    Address,
    TxEnv,
    TxKind,
    U256,
};

use alloy_consensus::TxEnvelope;
use revm::primitives::AuthorizationList;

/// Used to fill a TxEnv with the data from a TxEnvelope
pub fn fill_tx_env(input_tx: TxEnvelope, tx_env: &mut TxEnv, sender: Address) {
    tx_env.caller = sender;
    match input_tx {
        TxEnvelope::Legacy(tx) => {
            let tx = tx.tx();
            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = U256::from(tx.gas_price);
            tx_env.gas_priority_fee = None;
            tx_env.transact_to = tx.to;
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = tx.chain_id;
            tx_env.nonce = Some(tx.nonce);
            tx_env.access_list.clear();
            tx_env.blob_hashes.clear();
            tx_env.max_fee_per_blob_gas.take();
            tx_env.authorization_list = None;
        }
        TxEnvelope::Eip2930(tx) => {
            let tx = tx.tx();
            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = U256::from(tx.gas_price);
            tx_env.gas_priority_fee = None;
            tx_env.transact_to = tx.to;
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = Some(tx.chain_id);
            tx_env.nonce = Some(tx.nonce);
            tx_env.access_list.clone_from(&tx.access_list.0);
            tx_env.blob_hashes.clear();
            tx_env.max_fee_per_blob_gas.take();
            tx_env.authorization_list = None;
        }
        TxEnvelope::Eip1559(tx) => {
            let tx = tx.tx();
            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = U256::from(tx.max_fee_per_gas);
            tx_env.gas_priority_fee = Some(U256::from(tx.max_priority_fee_per_gas));
            tx_env.transact_to = tx.to;
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = Some(tx.chain_id);
            tx_env.nonce = Some(tx.nonce);
            tx_env.access_list.clone_from(&tx.access_list.0);
            tx_env.blob_hashes.clear();
            tx_env.max_fee_per_blob_gas.take();
            tx_env.authorization_list = None;
        }
        TxEnvelope::Eip4844(tx) => {
            let tx = tx.tx().tx();
            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = U256::from(tx.max_fee_per_gas);
            tx_env.gas_priority_fee = Some(U256::from(tx.max_priority_fee_per_gas));
            tx_env.transact_to = TxKind::Call(tx.to);
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = Some(tx.chain_id);
            tx_env.nonce = Some(tx.nonce);
            tx_env.access_list.clone_from(&tx.access_list.0);
            tx_env.blob_hashes.clone_from(&tx.blob_versioned_hashes);
            tx_env.max_fee_per_blob_gas = Some(U256::from(tx.max_fee_per_blob_gas));
            tx_env.authorization_list = None;
        }
        TxEnvelope::Eip7702(tx) => {
            let tx = tx.tx();
            let auth_list =
                tx.authorization_list
                    .iter()
                    .cloned()
                    .fold(vec![], |mut auth_list, auth_item| {
                        let auth_item_revm = revm::primitives::SignedAuthorization::new_unchecked(
                            auth_item.inner().to_owned(),
                            auth_item.y_parity(),
                            auth_item.r(),
                            auth_item.s(),
                        );

                        auth_list.push(auth_item_revm);
                        auth_list
                    });

            tx_env.authorization_list = Some(AuthorizationList::Signed(auth_list));

            tx_env.gas_limit = tx.gas_limit;
            tx_env.gas_price = U256::from(tx.max_fee_per_gas);
            tx_env.gas_priority_fee = Some(U256::from(tx.max_priority_fee_per_gas));
            tx_env.transact_to = tx.to.into();
            tx_env.value = tx.value;
            tx_env.data = tx.input.clone();
            tx_env.chain_id = Some(tx.chain_id);
            tx_env.nonce = Some(tx.nonce);
            tx_env.access_list.clone_from(&tx.access_list.0);
            tx_env.blob_hashes.clear();
            tx_env.max_fee_per_blob_gas.take();
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
    use alloy_eip7702::{
        Authorization,
        SignedAuthorization,
    };
    use rand::random;

    #[allow(deprecated)]
    use alloy::primitives::PrimitiveSignature;

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
        let access_list = Default::default();

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
            PrimitiveSignature::new(random_bytes().into(), random_bytes().into(), random());

        let signed_tx = Signed::new_unchecked(tx.clone(), primitive_signature, random_bytes());

        let tx_envelope = TxEnvelope::Eip7702(signed_tx);

        let mut tx_env = TxEnv::default();

        fill_tx_env(tx_envelope, &mut tx_env, sender);

        assert_eq!(tx_env.gas_limit, tx.gas_limit);
        assert_eq!(tx_env.gas_price, U256::from(tx.max_fee_per_gas));
        assert_eq!(
            tx_env.gas_priority_fee,
            Some(U256::from(tx.max_priority_fee_per_gas))
        );
        assert_eq!(tx_env.value, value);
        assert_eq!(tx_env.data, Bytes::default());
        assert_eq!(tx_env.chain_id, Some(chain_id));
        assert_eq!(tx_env.max_fee_per_blob_gas, None);
        for (i, auth_item) in tx.authorization_list.iter().enumerate() {
            let tx_env_item = match &tx_env.authorization_list {
                Some(AuthorizationList::Signed(authorization_list)) => {
                    authorization_list[i].clone()
                }
                _ => panic!("AuthorizationList not found"),
            };
            assert_eq!(tx_env_item.address, auth_item.address);
            assert_eq!(tx_env_item.chain_id, auth_item.chain_id);
            assert_eq!(tx_env_item.nonce, auth_item.nonce);

            let tx_env_item_sig = tx_env_item.signature().unwrap();
            let auth_item_sig = auth_item.signature().unwrap();

            let expected_parity = auth_item_sig.v();

            assert_eq!(tx_env_item_sig.v(), expected_parity);
            assert_eq!(tx_env_item_sig.r(), auth_item_sig.r());
            assert_eq!(tx_env_item_sig.s(), auth_item_sig.s());
        }
    }
}
