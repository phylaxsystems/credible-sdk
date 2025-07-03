use alloy::{
    hex,
    signers::k256::ecdsa::SigningKey,
    signers::k256::elliptic_curve::rand_core::OsRng,
};
use assertion_da_client::DaClient;
use int_test_utils::deploy_test_da;
use pcl_common::args::CliArgs;
use pcl_core::config::CliConfig;
use pcl_core::{
    assertion_da::DaStoreArgs,
    error::DaSubmitError,
};
use pcl_phoundry::build_and_flatten::BuildAndFlattenArgs;
use std::{
    collections::HashMap,
    path::PathBuf,
};

#[derive(Debug, Default)]
pub struct TestSetup {
    pub root: Option<PathBuf>,
    pub assertion_contract: Option<String>,
    pub constructor_args: Vec<String>,
    pub json: bool,
}

impl TestSetup {
    pub fn new() -> Self {
        Self {
            root: None,
            assertion_contract: None,
            json: false,
            constructor_args: vec![],
        }
    }

    pub fn set_root(&mut self, root: PathBuf) {
        self.root = Some(root);
    }

    pub fn set_assertion_contract(&mut self, assertion_contract: String) {
        self.assertion_contract = Some(assertion_contract);
    }

    pub fn set_constructor_args(&mut self, constructor_args: Vec<String>) {
        self.constructor_args = constructor_args;
    }

    #[allow(dead_code)]
    pub fn set_json(&mut self, json: bool) {
        self.json = json;
    }

    pub async fn build(&self) -> Result<TestRunner, DaSubmitError> {
        let (_handle, da_url) = deploy_test_da(SigningKey::random(&mut OsRng)).await;
        let build_and_flatten_args = BuildAndFlattenArgs {
            root: Some(
                self.root
                    .clone()
                    .unwrap_or(PathBuf::from("../../../testdata/mock-protocol")),
            ),
            assertion_contract: self
                .assertion_contract
                .clone()
                .unwrap_or("NoArgsAssertion".to_string()),
        };

        let da_store_args = DaStoreArgs {
            url: format!("http://{da_url}"),
            args: build_and_flatten_args,
            constructor_args: self.constructor_args.clone(),
        };

        let cli_config = CliConfig {
            auth: None,
            assertions_for_submission: HashMap::new(),
        };

        let cli_args: CliArgs = CliArgs {
            json: self.json,
            config_dir: None,
        };

        let test_runner = TestRunner {
            cli_args,
            cli_config,
            da_store_args,
            da_client: DaClient::new(&format!("http://{da_url}")).unwrap(),
            _da_handle: _handle,
        };
        Ok(test_runner)
    }
}

pub struct TestRunner {
    pub cli_args: CliArgs,
    pub da_store_args: DaStoreArgs,
    pub cli_config: CliConfig,
    pub da_client: DaClient,
    pub _da_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}
impl TestRunner {
    pub async fn run(&mut self) -> Result<(), DaSubmitError> {
        self.da_store_args
            .run(&self.cli_args, &mut self.cli_config)
            .await?;
        Ok(())
    }
    pub async fn assert_assertion_as_expected(&self, assertion_id: String) {
        let assertion_for_submission = self
            .cli_config
            .assertions_for_submission
            .get(&assertion_id.clone().into())
            .unwrap();

        let assertion = self
            .da_client
            .fetch_assertion(
                assertion_for_submission
                    .assertion_id
                    .clone()
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            assertion.prover_signature,
            hex::decode(assertion_for_submission.signature.clone()).unwrap()
        );
        assert_eq!(
            self.da_store_args.constructor_args,
            assertion_for_submission.constructor_args
        );
    }
}
