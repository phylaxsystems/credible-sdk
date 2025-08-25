use super::{
    harness::{
        execute_txs,
        validate_tx_hashes,
    },
    tx_utils::{
        add_assertion_tx,
        register_assertion_adopter_tx,
        remove_assertion_tx,
    },
};
use alloy_node_bindings::AnvilInstance;
use alloy_primitives::B256;
use alloy_rpc_types::TransactionRequest;
use alloy_signer::k256::ecdsa::SigningKey;
use alloy_signer_local::LocalSigner;
use assertion_da_client::{
    DaClient,
    DaSubmissionResponse,
};
use assertion_executor::{
    primitives::{
        Address,
        FixedBytes,
        U256,
        bytes,
    },
    store::{
        AssertionStore,
        BlockTag,
        Indexer,
    },
};
use int_test_utils::{
    Contracts,
    assertion_src,
    deploy_contracts,
    deploy_test_da,
    get_anvil_deployer,
};
use rand::RngCore;
use std::net::TcpListener;

use alloy_provider::{
    Provider,
    RootProvider,
    ext::AnvilApi,
};

use alloy::sol_types::SolValue;

// Helper function to get a random available port
fn get_available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to address")
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

pub struct TestCtx {
    pub _anvil: AnvilInstance,
    pub provider: RootProvider,
    pub da_client: DaClient,
    pub store: AssertionStore,
    pub indexer: Indexer,
    pub _da_server_handle: tokio::task::JoinHandle<std::result::Result<(), anyhow::Error>>,
    pub contracts: Contracts,
    pub deployer: Address,
    pub protocols: Vec<Address>,
    pub assertion_ids: Vec<B256>,
}

async fn provider_from_anvil(anvil: &AnvilInstance) -> RootProvider {
    use alloy_provider::ProviderBuilder;
    use alloy_transport_ws::WsConnect;
    ProviderBuilder::new()
        .connect_ws(WsConnect::new(anvil.ws_endpoint()))
        .await
        .unwrap()
        .root()
        .clone()
}

pub async fn setup_int_test_indexer(block_tag: BlockTag, time_lock_blocks: u64) -> TestCtx {
    let port = get_available_port();
    let anvil = alloy_node_bindings::Anvil::new().port(port).spawn();
    let deployer = get_anvil_deployer(&anvil);

    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let da_signer = SigningKey::from_bytes(&bytes.into()).unwrap();

    let contracts = deploy_contracts(
        &anvil,
        da_signer.clone(),
        std::path::PathBuf::from("../../lib/credible-layer-contracts"),
        time_lock_blocks as usize,
    )
    .unwrap();
    let state_oracle = contracts.state_oracle;

    let (_da_server_handle, da_server_addr) = deploy_test_da(da_signer.clone()).await;

    let da_server_addr = format!("http://{da_server_addr}");
    let da_client = DaClient::new(da_server_addr.as_str()).unwrap();

    let store = AssertionStore::new_ephemeral().unwrap();

    // Create provider from anvil ws endpoint
    let provider = provider_from_anvil(&anvil).await;
    provider.anvil_set_auto_mine(false).await.unwrap();
    provider.anvil_auto_impersonate_account(true).await.unwrap();

    let db = sled::Config::tmp().unwrap().open().unwrap();
    let indexer_config = assertion_executor::store::IndexerCfg {
        provider: provider.clone().root().clone(),
        db,
        store: store.clone(),
        da_client,
        state_oracle,
        executor_config: assertion_executor::ExecutorConfig::default(),
        await_tag: block_tag,
    };

    let indexer = Indexer::new(indexer_config);

    let wallet = LocalSigner::from(deployer);
    let address: Address = wallet.address();

    TestCtx {
        _anvil: anvil,
        provider,
        deployer: address,
        da_client: DaClient::new(da_server_addr.to_string().as_str()).unwrap(),
        store,
        indexer,
        _da_server_handle,
        contracts,
        protocols: Vec::new(),
        assertion_ids: Vec::new(),
    }
}

impl TestCtx {
    async fn register_assertion_adopter_tx(&self, contract_address: Address) -> TransactionRequest {
        register_assertion_adopter_tx(
            self.contracts.state_oracle,
            contract_address,
            self.contracts.admin_verifier,
            self.deployer,
        )
    }
    async fn deploy_protocol(&self, address: Address) {
        self.provider.anvil_set_code(address, bytes!("6080604052348015600e575f5ffd5b50600436106026575f3560e01c80638da5cb5b14602a575b5f5ffd5b60306044565b604051603b919060a3565b60405180910390f35b5f5f9054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f608f826068565b9050919050565b609d816087565b82525050565b5f60208201905060b45f8301846096565b9291505056fea26469706673582212200418876045980ffda5556093234df1e2a927436407716d25b3a69631380a2d9c64736f6c634300081c0033" )).await.unwrap();
        let mut bytes = Vec::with_capacity(32);
        bytes.resize(12, 0);
        bytes.extend_from_slice(&self.deployer.abi_encode_packed());
        let fixed = FixedBytes::<32>::from_slice(&bytes);
        self.provider
            .anvil_set_storage_at(address, U256::ZERO, fixed)
            .await
            .unwrap();
    }

    pub async fn deploy_and_register_protocol(&mut self, address: Address) {
        self.deploy_protocol(address).await;
        let register_tx = self.register_assertion_adopter_tx(address).await;
        let tx_hashes = execute_txs(vec![register_tx], self).await;
        self.provider.anvil_mine(Some(1), None).await.unwrap();
        validate_tx_hashes(tx_hashes, self).await;
        self.protocols.push(address);
    }

    pub async fn submit_to_da_no_args(&self) -> DaSubmissionResponse {
        let src = assertion_src();
        self.da_client
            .submit_assertion("SimpleCounterAssertion".into(), src, "0.8.28".to_string())
            .await
            .unwrap()
    }
    pub async fn submit_to_da_with_args(&self) -> DaSubmissionResponse {
        let src = assertion_src();
        self.da_client
            .submit_assertion_with_args(
                "SimpleCounterAssertionWithArgs".into(),
                src,
                "0.8.28".to_string(),
                "constructor(uint256)".to_string(),
                vec!["3".to_string()],
            )
            .await
            .unwrap()
    }

    pub async fn submit_to_da_malformed(&self) -> DaSubmissionResponse {
        self.da_client
            .submit_assertion_with_args(
                "SimpleCounterAssertionMalformed".into(),
                "contract SimpleCounterAssertionMalformed {
                    function triggers() public pure returns (bool) {
                        return true;
                    }
                }"
                .into(),
                "0.8.28".to_string(),
                "constructor(uint256)".to_string(),
                vec!["3".to_string()],
            )
            .await
            .unwrap()
    }

    pub async fn add_assertion_tx(&mut self, contract_address: Address) -> TransactionRequest {
        let da_submission_response = self.submit_to_da_no_args().await;
        self.assertion_ids.push(da_submission_response.id);
        add_assertion_tx(
            self.contracts.state_oracle,
            contract_address,
            self.deployer,
            da_submission_response,
        )
    }

    pub async fn add_assertion_tx_with_args(
        &mut self,
        contract_address: Address,
    ) -> TransactionRequest {
        let da_submission_response = self.submit_to_da_with_args().await;
        self.assertion_ids.push(da_submission_response.id);
        add_assertion_tx(
            self.contracts.state_oracle,
            contract_address,
            self.deployer,
            da_submission_response,
        )
    }

    pub async fn add_assertion_tx_malformed(
        &mut self,
        contract_address: Address,
    ) -> TransactionRequest {
        let da_submission_response = self.submit_to_da_malformed().await;
        self.assertion_ids.push(da_submission_response.id);
        add_assertion_tx(
            self.contracts.state_oracle,
            contract_address,
            self.deployer,
            da_submission_response,
        )
    }

    pub fn remove_assertion_tx(
        &self,
        contract_address: Address,
        assertion_index: usize,
    ) -> TransactionRequest {
        remove_assertion_tx(
            self.contracts.state_oracle,
            contract_address,
            self.assertion_ids[assertion_index],
            self.deployer,
        )
    }
}
