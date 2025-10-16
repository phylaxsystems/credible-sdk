use alloy::{
    primitives::B256,
    providers::{
        Provider,
        ProviderBuilder,
        RootProvider,
    },
};
use anyhow::Result;

pub struct Client {
    provider: RootProvider,
}

impl Client {
    pub async fn try_new_with_rpc_url(rpc_url: &str) -> Result<Self> {
        // Create a provider (this needs to be done in an async context)
        let provider = ProviderBuilder::new().connect(rpc_url).await?;

        Ok(Self {
            provider: provider.root().clone(),
        })
    }

    pub async fn get_block_state_root(&self, block_number: u64) -> Result<B256> {
        // Fetch the block header
        let block = self
            .provider
            .get_block_by_number(block_number.into())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Block not found"))?;

        // Get the state root from the header
        Ok(block.header.state_root)
    }
}
