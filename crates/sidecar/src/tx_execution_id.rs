use revm::primitives::alloy_primitives::TxHash;
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
    de::{
        self,
        MapAccess,
        Visitor,
    },
    ser::SerializeStruct,
};
use std::{
    fmt,
    str::FromStr,
};

/// Unique identifier for a transaction execution within the sidecar.
///
/// Each block the sidecar builds has multiple attached *iterations* to it.
/// Iterations can be thought of as sub-blocks that sequencer/driver might pick
/// for a variety of reasons. This struct is used to identify specific
/// txs for specific blocks and iterations, since multiple hashes can
/// belong to multiple iterations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxExecutionId {
    /// What block number the transaction was meant for.
    pub block_number: u64,
    /// What iteration the transaction should be executed at.
    pub iteration_id: u64,
    // Transaction hash.
    pub tx_hash: TxHash,
}

/// Unique identifier for a block execution within the sidecar.
///
/// Represents the block (blockEnv) status for each iteration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockExecutionId {
    /// What block number the transaction was meant for.
    pub block_number: u64,
    /// What iteration the transaction should be executed at.
    pub iteration_id: u64,
}

impl TxExecutionId {
    pub const fn new(block_number: u64, iteration_id: u64, tx_hash: TxHash) -> Self {
        Self {
            block_number,
            iteration_id,
            tx_hash,
        }
    }

    pub fn as_block_execution_id(&self) -> BlockExecutionId {
        BlockExecutionId {
            block_number: self.block_number,
            iteration_id: self.iteration_id,
        }
    }

    pub fn from_hash(tx_hash: TxHash) -> Self {
        Self::new(0, 0, tx_hash)
    }

    /// Return the transaction hash formatted with `0x` prefix.
    pub fn tx_hash_hex(self) -> String {
        format!("{:#x}", self.tx_hash)
    }
}

impl fmt::Display for TxExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#x}", self.tx_hash)
    }
}

impl Serialize for TxExecutionId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TxExecutionId", 3)?;
        state.serialize_field("block_number", &self.block_number)?;
        state.serialize_field("iteration_id", &self.iteration_id)?;
        state.serialize_field("tx_hash", &self.tx_hash_hex())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for TxExecutionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            BlockNumber,
            IterationId,
            TxHash,
        }

        struct TxExecutionIdVisitor;

        impl<'de> Visitor<'de> for TxExecutionIdVisitor {
            type Value = TxExecutionId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct tx_execution_id")
            }

            fn visit_map<V>(self, mut map: V) -> Result<TxExecutionId, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut block_number = None;
                let mut iteration_id = None;
                let mut tx_hash = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::BlockNumber => {
                            if block_number.is_some() {
                                return Err(de::Error::duplicate_field("block_number"));
                            }
                            block_number = Some(map.next_value().map_err(|e| {
                                de::Error::custom(format!("invalid block_number: {e}"))
                            })?);
                        }
                        Field::IterationId => {
                            if iteration_id.is_some() {
                                return Err(de::Error::duplicate_field("iteration_id"));
                            }
                            iteration_id = Some(map.next_value().map_err(|e| {
                                de::Error::custom(format!("invalid iteration_id: {e}"))
                            })?);
                        }
                        Field::TxHash => {
                            if tx_hash.is_some() {
                                return Err(de::Error::duplicate_field("tx_hash"));
                            }
                            let value: String = map.next_value()?;
                            let parsed_hash = normalize_hash(&value)
                                .map_err(|e| de::Error::custom(format!("invalid tx_hash: {e}")))?;
                            tx_hash = Some(parsed_hash);
                        }
                    }
                }

                let block_number =
                    block_number.ok_or_else(|| de::Error::missing_field("block_number"))?;
                let iteration_id =
                    iteration_id.ok_or_else(|| de::Error::missing_field("iteration_id"))?;
                let tx_hash = tx_hash.ok_or_else(|| de::Error::missing_field("tx_hash"))?;

                Ok(TxExecutionId {
                    block_number,
                    iteration_id,
                    tx_hash,
                })
            }
        }

        deserializer.deserialize_struct(
            "TxExecutionId",
            &["block_number", "iteration_id", "tx_hash"],
            TxExecutionIdVisitor,
        )
    }
}

fn normalize_hash(value: &str) -> Result<TxHash, alloy::primitives::hex::FromHexError> {
    let trimmed = value.trim();
    let normalized = if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
        trimmed.to_owned()
    } else {
        format!("0x{trimmed}")
    };
    TxHash::from_str(&normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_prefixed_hash() {
        let json = r#"{
            "block_number": 1,
            "iteration_id": 2,
            "tx_hash": "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
        }"#;

        let parsed: TxExecutionId = serde_json::from_str(json).expect("should parse");
        assert_eq!(parsed.block_number, 1);
        assert_eq!(parsed.iteration_id, 2);
        assert_eq!(
            parsed.tx_hash,
            TxHash::from_str("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
                .unwrap()
        );
    }

    #[test]
    fn deserializes_unprefixed_hash() {
        let json = r#"{
            "block_number": 3,
            "iteration_id": 4,
            "tx_hash": "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
        }"#;

        let parsed: TxExecutionId = serde_json::from_str(json).expect("should parse");
        assert_eq!(parsed.block_number, 3);
        assert_eq!(parsed.iteration_id, 4);
        assert_eq!(
            parsed.tx_hash,
            TxHash::from_str("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
                .unwrap()
        );
    }
}
