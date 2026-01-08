use super::{
    IncidentData,
    IncidentReport,
    ReconstructableTx,
    TransactionObserverError,
};
use chrono::{
    NaiveDateTime,
    Utc,
};
use dapp_api_client::generated::types as dapp_types;
use revm::{
    context::{
        BlockEnv,
        TxEnv,
    },
    context_interface::either::Either,
    context_interface::transaction::{
        AccessList,
        RecoveredAuthorization,
        SignedAuthorization,
    },
    primitives::{
        Address,
        TxKind,
        U256,
    },
};
use serde_json::{
    Map,
    Value,
};
use tracing::warn;

pub(super) fn build_incident_body(
    report: &IncidentReport,
) -> Result<dapp_types::PostEnforcerIncidentsBody, TransactionObserverError> {
    let incident_timestamp = format_incident_timestamp(report.incident_timestamp)?;
    let failures = report
        .failures
        .iter()
        .map(build_failure_payload)
        .collect::<Result<Vec<_>, _>>()?;
    let transaction_data = build_transaction_data_payload(&report.transaction_data)?;

    let mut body = Map::new();
    body.insert("failures".to_string(), Value::Array(failures));
    body.insert(
        "incident_timestamp".to_string(),
        Value::String(incident_timestamp),
    );
    body.insert("transaction_data".to_string(), transaction_data);
    body.insert(
        "block_env".to_string(),
        Value::Object(build_block_env_payload(&report.block_env)),
    );

    let previous_transactions = build_previous_transactions_payload(&report.prev_txs)?;
    if !previous_transactions.is_empty() {
        body.insert(
            "previous_transactions".to_string(),
            Value::Array(previous_transactions),
        );
    }

    serde_json::from_value(Value::Object(body)).map_err(|e| {
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
    let naive = NaiveDateTime::from_timestamp_opt(seconds, 0).ok_or_else(|| {
        TransactionObserverError::PublishFailed {
            reason: format!("Invalid incident timestamp: {timestamp}"),
        }
    })?;
    Ok(chrono::DateTime::<Utc>::from_utc(naive, Utc).to_rfc3339())
}

fn build_block_env_payload(block_env: &BlockEnv) -> Map<String, Value> {
    let mut payload = Map::new();
    payload.insert(
        "number".to_string(),
        Value::String(block_env.number.to_string()),
    );
    payload.insert(
        "beneficiary".to_string(),
        Value::String(bytes_to_hex(block_env.beneficiary.as_slice())),
    );
    payload.insert(
        "timestamp".to_string(),
        Value::String(block_env.timestamp.to_string()),
    );
    payload.insert(
        "gas_limit".to_string(),
        Value::String(block_env.gas_limit.to_string()),
    );
    payload.insert(
        "basefee".to_string(),
        Value::String(block_env.basefee.to_string()),
    );
    payload.insert(
        "difficulty".to_string(),
        Value::String(block_env.difficulty.to_string()),
    );

    if let Some(prevrandao) = block_env.prevrandao {
        payload.insert(
            "prevrandao".to_string(),
            Value::String(bytes_to_hex(prevrandao.as_slice())),
        );
    }

    if let Some(blob) = block_env.blob_excess_gas_and_price {
        let mut blob_payload = Map::new();
        blob_payload.insert(
            "excess_blob_gas".to_string(),
            Value::String(blob.excess_blob_gas.to_string()),
        );
        blob_payload.insert(
            "blob_gasprice".to_string(),
            Value::String(blob.blob_gasprice.to_string()),
        );
        payload.insert(
            "blob_excess_gas_and_price".to_string(),
            Value::Object(blob_payload),
        );
    }

    payload
}

fn build_failure_payload(failure: &IncidentData) -> Result<Value, TransactionObserverError> {
    let mut payload = Map::new();
    payload.insert(
        "assertion_adopter_address".to_string(),
        Value::String(bytes_to_hex(failure.adopter_address.as_slice())),
    );
    payload.insert(
        "assertion_id".to_string(),
        Value::String(bytes_to_hex(failure.assertion_id.as_slice())),
    );
    payload.insert(
        "assertion_fn_selector".to_string(),
        Value::String(bytes_to_hex(failure.assertion_fn.as_slice())),
    );

    if !failure.revert_data.is_empty() {
        payload.insert(
            "revert_reason".to_string(),
            Value::String(bytes_to_hex(failure.revert_data.as_ref())),
        );
    }

    Ok(Value::Object(payload))
}

fn build_previous_transactions_payload(
    previous_txs: &[ReconstructableTx],
) -> Result<Vec<Value>, TransactionObserverError> {
    previous_txs
        .iter()
        .map(|(_, tx_env)| build_previous_transaction_payload(tx_env))
        .collect()
}

fn build_previous_transaction_payload(tx_env: &TxEnv) -> Result<Value, TransactionObserverError> {
    let mut payload = Map::new();
    payload.insert(
        "from".to_string(),
        Value::String(bytes_to_hex(tx_env.caller.as_slice())),
    );
    payload.insert(
        "to".to_string(),
        Value::String(tx_kind_to_address(&tx_env.kind, false)),
    );
    payload.insert(
        "value".to_string(),
        Value::String(tx_env.value.to_string()),
    );
    if !tx_env.data.is_empty() {
        payload.insert(
            "calldata".to_string(),
            Value::String(bytes_to_hex(tx_env.data.as_ref())),
        );
    }
    Ok(Value::Object(payload))
}

fn build_transaction_data_payload(
    transaction: &ReconstructableTx,
) -> Result<Value, TransactionObserverError> {
    let (tx_hash, tx_env) = transaction;
    let chain_id = match tx_env.chain_id {
        Some(chain_id) if chain_id > 0 => chain_id,
        _ => {
            return Err(TransactionObserverError::PublishFailed {
                reason: "Transaction chain_id missing or invalid".to_string(),
            });
        }
    };

    let mut payload = Map::new();
    payload.insert(
        "transaction_hash".to_string(),
        Value::String(bytes_to_hex(tx_hash.as_slice())),
    );
    payload.insert("chain_id".to_string(), Value::Number(chain_id.into()));
    payload.insert(
        "nonce".to_string(),
        Value::String(tx_env.nonce.to_string()),
    );
    payload.insert(
        "gas_limit".to_string(),
        Value::String(tx_env.gas_limit.to_string()),
    );
    payload.insert(
        "to_address".to_string(),
        Value::String(tx_kind_to_address(&tx_env.kind, true)),
    );
    payload.insert(
        "from_address".to_string(),
        Value::String(bytes_to_hex(tx_env.caller.as_slice())),
    );
    payload.insert(
        "value".to_string(),
        Value::String(tx_env.value.to_string()),
    );
    payload.insert(
        "type".to_string(),
        Value::Number((tx_env.tx_type as u64).into()),
    );
    if !tx_env.data.is_empty() {
        payload.insert(
            "data".to_string(),
            Value::String(bytes_to_hex(tx_env.data.as_ref())),
        );
    }

    match tx_env.tx_type {
        0 => {
            payload.insert(
                "gas_price".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
        }
        1 => {
            payload.insert(
                "gas_price".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
            let access_list = access_list_payload(&tx_env.access_list);
            if !access_list.is_empty() {
                payload.insert("access_list".to_string(), Value::Array(access_list));
            }
        }
        2 => {
            payload.insert(
                "max_fee_per_gas".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
            payload.insert(
                "max_priority_fee_per_gas".to_string(),
                Value::String(tx_env.gas_priority_fee.unwrap_or_default().to_string()),
            );
            let access_list = access_list_payload(&tx_env.access_list);
            if !access_list.is_empty() {
                payload.insert("access_list".to_string(), Value::Array(access_list));
            }
        }
        3 => {
            payload.insert(
                "max_fee_per_blob_gas".to_string(),
                Value::String(tx_env.max_fee_per_blob_gas.to_string()),
            );
            payload.insert(
                "max_fee_per_gas".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
            payload.insert(
                "max_priority_fee_per_gas".to_string(),
                Value::String(tx_env.gas_priority_fee.unwrap_or_default().to_string()),
            );
            payload.insert(
                "blob_versioned_hashes".to_string(),
                Value::Array(
                    tx_env
                        .blob_hashes
                        .iter()
                        .map(|hash| Value::String(bytes_to_hex(hash.as_slice())))
                        .collect(),
                ),
            );
            let access_list = access_list_payload(&tx_env.access_list);
            if !access_list.is_empty() {
                payload.insert("access_list".to_string(), Value::Array(access_list));
            }
        }
        4 => {
            payload.insert(
                "max_fee_per_gas".to_string(),
                Value::String(tx_env.gas_price.to_string()),
            );
            payload.insert(
                "max_priority_fee_per_gas".to_string(),
                Value::String(tx_env.gas_priority_fee.unwrap_or_default().to_string()),
            );
            let access_list = access_list_payload(&tx_env.access_list);
            if !access_list.is_empty() {
                payload.insert("access_list".to_string(), Value::Array(access_list));
            }
            payload.insert(
                "authorization_list".to_string(),
                Value::Array(authorization_list_payload(&tx_env.authorization_list)),
            );
        }
        tx_type => {
            return Err(TransactionObserverError::PublishFailed {
                reason: format!("Unsupported transaction type: {tx_type}"),
            });
        }
    }

    Ok(Value::Object(payload))
}

fn access_list_payload(access_list: &AccessList) -> Vec<Value> {
    access_list
        .iter()
        .map(|item| {
            let mut payload = Map::new();
            payload.insert(
                "address".to_string(),
                Value::String(bytes_to_hex(item.address.as_slice())),
            );
            payload.insert(
                "storage_keys".to_string(),
                Value::Array(
                    item.storage_keys
                        .iter()
                        .map(|key| Value::String(bytes_to_hex(key.as_slice())))
                        .collect(),
                ),
            );
            Value::Object(payload)
        })
        .collect()
}

fn authorization_list_payload(
    authorization_list: &[Either<SignedAuthorization, RecoveredAuthorization>],
) -> Vec<Value> {
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

            let mut payload = Map::new();
            payload.insert(
                "chain_id".to_string(),
                Value::String(u256_to_hex(*authorization.chain_id())),
            );
            payload.insert(
                "address".to_string(),
                Value::String(bytes_to_hex(authorization.address().as_slice())),
            );
            payload.insert(
                "nonce".to_string(),
                Value::String(u64_to_hex(authorization.nonce())),
            );
            payload.insert("v".to_string(), Value::String(u64_to_hex(u64::from(v))));
            payload.insert("r".to_string(), Value::String(u256_to_hex(r)));
            payload.insert("s".to_string(), Value::String(u256_to_hex(s)));
            Value::Object(payload)
        })
        .collect()
}

fn tx_kind_to_address(kind: &TxKind, allow_empty: bool) -> String {
    match kind {
        TxKind::Call(to) => bytes_to_hex(to.as_slice()),
        TxKind::Create => {
            if allow_empty {
                String::new()
            } else {
                bytes_to_hex(Address::ZERO.as_slice())
            }
        }
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn u256_to_hex(value: U256) -> String {
    format!("0x{value:x}")
}

fn u64_to_hex(value: u64) -> String {
    format!("0x{value:x}")
}
