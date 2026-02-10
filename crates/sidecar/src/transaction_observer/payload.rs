use super::{
    IncidentData,
    IncidentReport,
    ReconstructableTx,
    TransactionObserverError,
};
use chrono::Utc;
use credible_utils::hex::encode_hex_prefixed;
use dapp_api_client::generated::client::types as dapp_types;
use revm::{
    context::{
        BlockEnv,
        TxEnv,
    },
    context_interface::{
        either::Either,
        transaction::{
            AccessList,
            RecoveredAuthorization,
            SignedAuthorization,
        },
    },
    primitives::{
        Address,
        TxKind,
        U256,
    },
};
use serde::Serialize;
use tracing::warn;

const ENFORCER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize)]
struct IncidentPayload {
    failures: Vec<FailurePayload>,
    enforcer_version: String,
    incident_timestamp: String,
    transaction_data: TransactionDataPayload,
    block_env: BlockEnvPayload,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    previous_transactions: Vec<TransactionDataPayload>,
}

#[derive(Serialize)]
struct BlockEnvPayload {
    basefee: String,
    beneficiary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    blob_excess_gas_and_price: Option<BlobExcessGasAndPricePayload>,
    difficulty: String,
    gas_limit: String,
    number: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    prevrandao: Option<String>,
    timestamp: String,
}

#[derive(Serialize)]
struct BlobExcessGasAndPricePayload {
    blob_gasprice: String,
    excess_blob_gas: String,
}

#[derive(Serialize)]
struct FailurePayload {
    assertion_adopter_address: String,
    assertion_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    assertion_fn_selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revert_reason: Option<String>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum TransactionDataPayload {
    Legacy(TransactionDataLegacyPayload),
    AccessList(TransactionDataAccessListPayload),
    FeeMarket(TransactionDataFeeMarketPayload),
    Blob(TransactionDataBlobPayload),
    Authorization(TransactionDataAuthorizationPayload),
}

#[derive(Serialize)]
struct TransactionDataLegacyPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    from_address: String,
    gas_limit: String,
    gas_price: String,
    nonce: String,
    to_address: String,
    transaction_hash: String,
    #[serde(rename = "type")]
    tx_type: u64,
    value: String,
}

#[derive(Serialize)]
struct TransactionDataAccessListPayload {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    access_list: Vec<AccessListItemPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    from_address: String,
    gas_limit: String,
    gas_price: String,
    nonce: String,
    to_address: String,
    transaction_hash: String,
    #[serde(rename = "type")]
    tx_type: u64,
    value: String,
}

#[derive(Serialize)]
struct TransactionDataFeeMarketPayload {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    access_list: Vec<AccessListItemPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    from_address: String,
    gas_limit: String,
    max_fee_per_gas: String,
    max_priority_fee_per_gas: String,
    nonce: String,
    to_address: String,
    transaction_hash: String,
    #[serde(rename = "type")]
    tx_type: u64,
    value: String,
}

#[derive(Serialize)]
struct TransactionDataBlobPayload {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    access_list: Vec<AccessListItemPayload>,
    blob_versioned_hashes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    from_address: String,
    gas_limit: String,
    max_fee_per_blob_gas: String,
    max_fee_per_gas: String,
    max_priority_fee_per_gas: String,
    nonce: String,
    to_address: String,
    transaction_hash: String,
    #[serde(rename = "type")]
    tx_type: u64,
    value: String,
}

#[derive(Serialize)]
struct TransactionDataAuthorizationPayload {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    access_list: Vec<AccessListItemPayload>,
    authorization_list: Vec<AuthorizationListItemPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    from_address: String,
    gas_limit: String,
    max_fee_per_gas: String,
    max_priority_fee_per_gas: String,
    nonce: String,
    to_address: String,
    transaction_hash: String,
    #[serde(rename = "type")]
    tx_type: u64,
    value: String,
}

#[derive(Serialize)]
struct AccessListItemPayload {
    address: String,
    storage_keys: Vec<String>,
}

#[derive(Serialize)]
struct AuthorizationListItemPayload {
    chain_id: String,
    address: String,
    nonce: String,
    v: String,
    r: String,
    s: String,
}

struct TransactionCommonFields {
    transaction_hash: String,
    chain_id: Option<String>,
    nonce: String,
    gas_limit: String,
    to_address: String,
    from_address: String,
    value: String,
    tx_type: u64,
    data: Option<String>,
}

pub(super) fn build_incident_body(
    report: &IncidentReport,
) -> Result<dapp_types::PostEnforcerIncidentsBody, TransactionObserverError> {
    let incident_timestamp = format_incident_timestamp(report.incident_timestamp)?;
    let failures = report
        .failures
        .iter()
        .map(build_failure_payload)
        .collect::<Vec<_>>();
    let transaction_data = build_transaction_data_payload(&report.transaction_data)?;
    let block_env = build_block_env_payload(&report.block_env);
    let previous_transactions = build_previous_transactions_payload(&report.prev_txs)?;

    let payload = IncidentPayload {
        failures,
        enforcer_version: ENFORCER_VERSION.to_string(),
        incident_timestamp,
        transaction_data,
        block_env,
        previous_transactions,
    };

    serde_json::to_value(payload)
        .and_then(serde_json::from_value)
        .map_err(|e| {
            TransactionObserverError::PublishFailed {
                reason: format!("Failed to build incident payload: {e}"),
            }
        })
}

fn format_incident_timestamp(timestamp: u64) -> Result<String, TransactionObserverError> {
    let seconds = i64::try_from(timestamp).map_err(|_| {
        TransactionObserverError::PublishFailed {
            reason: format!("Incident timestamp out of range: {timestamp}"),
        }
    })?;
    let date_time = chrono::DateTime::<Utc>::from_timestamp(seconds, 0).ok_or_else(|| {
        TransactionObserverError::PublishFailed {
            reason: format!("Invalid incident timestamp: {timestamp}"),
        }
    })?;
    Ok(date_time.to_rfc3339())
}

fn build_block_env_payload(block_env: &BlockEnv) -> BlockEnvPayload {
    let blob_excess_gas_and_price = block_env.blob_excess_gas_and_price.map(|blob| {
        BlobExcessGasAndPricePayload {
            excess_blob_gas: blob.excess_blob_gas.to_string(),
            blob_gasprice: blob.blob_gasprice.to_string(),
        }
    });
    let prevrandao = block_env
        .prevrandao
        .map(|prevrandao| encode_hex_prefixed(prevrandao.as_slice()));

    BlockEnvPayload {
        number: block_env.number.to_string(),
        beneficiary: encode_hex_prefixed(block_env.beneficiary.as_slice()),
        timestamp: block_env.timestamp.to_string(),
        gas_limit: block_env.gas_limit.to_string(),
        basefee: block_env.basefee.to_string(),
        difficulty: block_env.difficulty.to_string(),
        prevrandao,
        blob_excess_gas_and_price,
    }
}

fn build_failure_payload(failure: &IncidentData) -> FailurePayload {
    let revert_reason = if failure.revert_data.is_empty() {
        None
    } else {
        Some(encode_hex_prefixed(failure.revert_data.as_ref()))
    };

    FailurePayload {
        assertion_adopter_address: encode_hex_prefixed(failure.adopter_address.as_slice()),
        assertion_id: encode_hex_prefixed(failure.assertion_id.as_slice()),
        assertion_fn_selector: Some(encode_hex_prefixed(failure.assertion_fn.as_slice())),
        revert_reason,
    }
}

fn build_previous_transactions_payload(
    previous_txs: &[ReconstructableTx],
) -> Result<Vec<TransactionDataPayload>, TransactionObserverError> {
    previous_txs
        .iter()
        .map(build_transaction_data_payload)
        .collect()
}

fn build_transaction_data_payload(
    transaction: &ReconstructableTx,
) -> Result<TransactionDataPayload, TransactionObserverError> {
    let (tx_hash, tx_env) = transaction;
    let chain_id = tx_env
        .chain_id
        .filter(|chain_id| *chain_id > 0)
        .map(|chain_id| chain_id.to_string());
    let common_fields = build_common_transaction_fields(tx_hash.as_slice(), tx_env, chain_id);
    match tx_env.tx_type {
        0 => Ok(build_legacy_payload(common_fields, tx_env)),
        1 => Ok(build_access_list_payload(common_fields, tx_env)),
        2 => Ok(build_fee_market_payload(common_fields, tx_env)),
        3 => Ok(build_blob_payload(common_fields, tx_env)),
        4 => Ok(build_authorization_payload(common_fields, tx_env)),
        tx_type => {
            Err(TransactionObserverError::PublishFailed {
                reason: format!("Unsupported transaction type: {tx_type}"),
            })
        }
    }
}

fn build_common_transaction_fields(
    tx_hash: &[u8],
    tx_env: &TxEnv,
    chain_id: Option<String>,
) -> TransactionCommonFields {
    TransactionCommonFields {
        transaction_hash: encode_hex_prefixed(tx_hash),
        chain_id,
        nonce: tx_env.nonce.to_string(),
        gas_limit: tx_env.gas_limit.to_string(),
        to_address: tx_kind_to_address(&tx_env.kind, true),
        from_address: encode_hex_prefixed(tx_env.caller.as_slice()),
        value: tx_env.value.to_string(),
        tx_type: u64::from(tx_env.tx_type),
        data: (!tx_env.data.is_empty()).then(|| encode_hex_prefixed(tx_env.data.as_ref())),
    }
}

fn build_legacy_payload(
    common_fields: TransactionCommonFields,
    tx_env: &TxEnv,
) -> TransactionDataPayload {
    TransactionDataPayload::Legacy(TransactionDataLegacyPayload {
        chain_id: common_fields.chain_id,
        data: common_fields.data,
        from_address: common_fields.from_address,
        gas_limit: common_fields.gas_limit,
        gas_price: tx_env.gas_price.to_string(),
        nonce: common_fields.nonce,
        to_address: common_fields.to_address,
        transaction_hash: common_fields.transaction_hash,
        tx_type: common_fields.tx_type,
        value: common_fields.value,
    })
}

fn build_access_list_payload(
    common_fields: TransactionCommonFields,
    tx_env: &TxEnv,
) -> TransactionDataPayload {
    TransactionDataPayload::AccessList(TransactionDataAccessListPayload {
        access_list: access_list_payload(&tx_env.access_list),
        chain_id: common_fields.chain_id,
        data: common_fields.data,
        from_address: common_fields.from_address,
        gas_limit: common_fields.gas_limit,
        gas_price: tx_env.gas_price.to_string(),
        nonce: common_fields.nonce,
        to_address: common_fields.to_address,
        transaction_hash: common_fields.transaction_hash,
        tx_type: common_fields.tx_type,
        value: common_fields.value,
    })
}

fn build_fee_market_payload(
    common_fields: TransactionCommonFields,
    tx_env: &TxEnv,
) -> TransactionDataPayload {
    TransactionDataPayload::FeeMarket(TransactionDataFeeMarketPayload {
        access_list: access_list_payload(&tx_env.access_list),
        chain_id: common_fields.chain_id,
        data: common_fields.data,
        from_address: common_fields.from_address,
        gas_limit: common_fields.gas_limit,
        max_fee_per_gas: tx_env.gas_price.to_string(),
        max_priority_fee_per_gas: tx_env.gas_priority_fee.unwrap_or_default().to_string(),
        nonce: common_fields.nonce,
        to_address: common_fields.to_address,
        transaction_hash: common_fields.transaction_hash,
        tx_type: common_fields.tx_type,
        value: common_fields.value,
    })
}

fn build_blob_payload(
    common_fields: TransactionCommonFields,
    tx_env: &TxEnv,
) -> TransactionDataPayload {
    TransactionDataPayload::Blob(TransactionDataBlobPayload {
        access_list: access_list_payload(&tx_env.access_list),
        blob_versioned_hashes: tx_env
            .blob_hashes
            .iter()
            .map(|hash| encode_hex_prefixed(hash.as_slice()))
            .collect(),
        chain_id: common_fields.chain_id,
        data: common_fields.data,
        from_address: common_fields.from_address,
        gas_limit: common_fields.gas_limit,
        max_fee_per_blob_gas: tx_env.max_fee_per_blob_gas.to_string(),
        max_fee_per_gas: tx_env.gas_price.to_string(),
        max_priority_fee_per_gas: tx_env.gas_priority_fee.unwrap_or_default().to_string(),
        nonce: common_fields.nonce,
        to_address: common_fields.to_address,
        transaction_hash: common_fields.transaction_hash,
        tx_type: common_fields.tx_type,
        value: common_fields.value,
    })
}

fn build_authorization_payload(
    common_fields: TransactionCommonFields,
    tx_env: &TxEnv,
) -> TransactionDataPayload {
    TransactionDataPayload::Authorization(TransactionDataAuthorizationPayload {
        access_list: access_list_payload(&tx_env.access_list),
        authorization_list: authorization_list_payload(&tx_env.authorization_list),
        chain_id: common_fields.chain_id,
        data: common_fields.data,
        from_address: common_fields.from_address,
        gas_limit: common_fields.gas_limit,
        max_fee_per_gas: tx_env.gas_price.to_string(),
        max_priority_fee_per_gas: tx_env.gas_priority_fee.unwrap_or_default().to_string(),
        nonce: common_fields.nonce,
        to_address: common_fields.to_address,
        transaction_hash: common_fields.transaction_hash,
        tx_type: common_fields.tx_type,
        value: common_fields.value,
    })
}

fn access_list_payload(access_list: &AccessList) -> Vec<AccessListItemPayload> {
    access_list
        .iter()
        .map(|item| {
            AccessListItemPayload {
                address: encode_hex_prefixed(item.address.as_slice()),
                storage_keys: item
                    .storage_keys
                    .iter()
                    .map(|key| encode_hex_prefixed(key.as_slice()))
                    .collect(),
            }
        })
        .collect()
}

fn authorization_list_payload(
    authorization_list: &[Either<SignedAuthorization, RecoveredAuthorization>],
) -> Vec<AuthorizationListItemPayload> {
    authorization_list
        .iter()
        .map(|authorization| {
            let (authorization, r, s, v) = match authorization {
                Either::Left(signed) => (signed.inner(), signed.r(), signed.s(), signed.y_parity()),
                Either::Right(recovered) => {
                    warn!(
                        target = "transaction_observer",
                        "Authorization list entry missing signature, using zeroed values"
                    );
                    (&**recovered, U256::ZERO, U256::ZERO, 0)
                }
            };

            AuthorizationListItemPayload {
                chain_id: u256_to_hex(*authorization.chain_id()),
                address: encode_hex_prefixed(authorization.address().as_slice()),
                nonce: u64_to_hex(authorization.nonce()),
                v: u64_to_hex(u64::from(v)),
                r: u256_to_hex(r),
                s: u256_to_hex(s),
            }
        })
        .collect()
}

fn tx_kind_to_address(kind: &TxKind, allow_empty: bool) -> String {
    match kind {
        TxKind::Call(to) => encode_hex_prefixed(to.as_slice()),
        TxKind::Create => {
            if allow_empty {
                String::new()
            } else {
                encode_hex_prefixed(Address::ZERO.as_slice())
            }
        }
    }
}

fn u256_to_hex(value: U256) -> String {
    format!("0x{value:x}")
}

fn u64_to_hex(value: u64) -> String {
    format!("0x{value:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::{
        context_interface::transaction::AccessList,
        primitives::{
            Address,
            Bytes,
            FixedBytes,
            TxKind,
            U256,
        },
    };
    use serde_json::Value;

    #[test]
    fn build_previous_transactions_payload_returns_full_transaction_object() {
        let tx_hash = FixedBytes::from([0x11; 32]);
        let tx_env = TxEnv {
            caller: Address::from([0x01; 20]),
            gas_limit: 21_000,
            gas_price: 100u128,
            kind: TxKind::Call(Address::from([0x02; 20])),
            value: U256::from(5u64),
            data: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
            nonce: 7,
            chain_id: Some(1),
            tx_type: 2,
            access_list: AccessList::default(),
            gas_priority_fee: Some(3u128),
            blob_hashes: Vec::new(),
            max_fee_per_blob_gas: 0,
            authorization_list: Vec::new(),
        };

        let payload =
            build_previous_transactions_payload(&[(tx_hash, tx_env)]).expect("build payload");
        let payload_value = serde_json::to_value(payload).expect("serialize payload");
        let payload_array = payload_value.as_array().expect("payload array");
        assert_eq!(payload_array.len(), 1);

        let tx_value = payload_array[0].as_object().expect("transaction object");
        let expected_hash = encode_hex_prefixed(tx_hash.as_slice());
        let expected_to = encode_hex_prefixed([0x02; 20]);
        let expected_from = encode_hex_prefixed([0x01; 20]);
        let expected_data = encode_hex_prefixed([0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(
            tx_value.get("transaction_hash").and_then(Value::as_str),
            Some(expected_hash.as_str())
        );
        assert_eq!(tx_value.get("chain_id").and_then(Value::as_str), Some("1"));
        assert_eq!(tx_value.get("nonce").and_then(Value::as_str), Some("7"));
        assert_eq!(
            tx_value.get("gas_limit").and_then(Value::as_str),
            Some("21000")
        );
        assert_eq!(
            tx_value.get("to_address").and_then(Value::as_str),
            Some(expected_to.as_str())
        );
        assert_eq!(
            tx_value.get("from_address").and_then(Value::as_str),
            Some(expected_from.as_str())
        );
        assert_eq!(tx_value.get("value").and_then(Value::as_str), Some("5"));
        assert_eq!(
            tx_value.get("max_fee_per_gas").and_then(Value::as_str),
            Some("100")
        );
        assert_eq!(
            tx_value
                .get("max_priority_fee_per_gas")
                .and_then(Value::as_str),
            Some("3")
        );
        assert_eq!(tx_value.get("type").and_then(Value::as_u64), Some(2));
        assert_eq!(
            tx_value.get("data").and_then(Value::as_str),
            Some(expected_data.as_str())
        );
    }
}
