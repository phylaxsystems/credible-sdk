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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxExecutionId {
    pub block_number: u64,
    pub iteration_id: u64,
    pub tx_hash: TxHash,
}

impl TxExecutionId {
    pub const fn new(block_number: u64, iteration_id: u64, tx_hash: TxHash) -> Self {
        Self {
            block_number,
            iteration_id,
            tx_hash,
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
                            let normalized_hash = value.trim();
                            let parsed_hash = TxHash::from_str(normalized_hash)
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
