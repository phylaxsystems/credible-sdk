use crate::cache::sources::{
    Source,
    json_rpc_db::{
        JsonRpcDb,
        JsonRpcDbError,
    },
};
use assertion_executor::{
    db::DatabaseRef,
    primitives::{
        AccountInfo,
        Address,
        B256,
        Bytecode,
    },
};
use revm::primitives::{
    StorageKey,
    StorageValue,
};
use std::sync::atomic::AtomicU64;

#[derive(Debug)]
pub struct Sequencer {
    json_rpc_db: JsonRpcDb,
}

impl Sequencer {
    pub async fn try_new(rpc_url: &str) -> Result<Self, JsonRpcDbError> {
        Ok(Sequencer {
            json_rpc_db: JsonRpcDb::try_new(rpc_url).await?,
        })
    }
}

impl Source for Sequencer {
    // The Sequencer is always synced.
    #[inline]
    fn is_synced(&self, _current_block_number: &AtomicU64) -> bool {
        true
    }

    #[inline]
    fn name(&self) -> &'static str {
        "sequencer"
    }
}

impl DatabaseRef for Sequencer {
    type Error = super::SourceError;
    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.json_rpc_db.basic_ref(address).map_err(Into::into)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.json_rpc_db
            .code_by_hash_ref(code_hash)
            .map_err(Into::into)
    }

    fn block_hash_ref(&self, block_number: u64) -> Result<B256, Self::Error> {
        self.json_rpc_db
            .block_hash_ref(block_number)
            .map_err(Into::into)
    }

    fn storage_ref(
        &self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        self.json_rpc_db
            .storage_ref(address, index)
            .map_err(Into::into)
    }
}
