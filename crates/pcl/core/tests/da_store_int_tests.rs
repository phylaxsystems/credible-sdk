#![cfg(feature = "full-test")]

mod common;

#[cfg(test)]
mod tests {
    use super::common::da_store_harness::TestSetup;
    use alloy::primitives::{
        Address,
        U256,
    };
    use assertion_da_client::DaClientError;
    use pcl_core::error::DaSubmitError;
    use std::{
        fs::File,
        io::Write,
        path::PathBuf,
    };

    // Build And flatten edges -
    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_store_directory_does_not_exist() {
        let mut test_setup = TestSetup::new();
        test_setup.set_root(PathBuf::from("dir-does-not-exist"));
        let mut test_runner = test_setup.build().await.unwrap();
        let res = test_runner.run().await;
        assert!(matches!(
            res,
            Err(DaSubmitError::PhoundryError(ref boxed_error))
            if matches!(**boxed_error, pcl_phoundry::error::PhoundryError::DirectoryNotFound(_))
        ));
    }

    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_store_no_source_files() {
        let mut test_setup = TestSetup::new();
        let temp_dir = tempfile::tempdir().unwrap();
        test_setup.set_root(temp_dir.path().to_path_buf());
        std::fs::create_dir_all(temp_dir.path().join("assertions/src")).unwrap();
        let mut test_runner = test_setup.build().await.unwrap();
        let res = test_runner.run().await;
        assert!(
            matches!(
                res,
                Err(DaSubmitError::PhoundryError(ref boxed_error))
                if matches!(**boxed_error, pcl_phoundry::error::PhoundryError::NoSourceFilesFound)
            ),
            "Result: {res:#?}",
        );
    }

    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_store_invalid_contract() {
        let mut test_setup = TestSetup::new();
        let temp_dir = tempfile::tempdir().unwrap();
        test_setup.set_root(temp_dir.path().to_path_buf());
        std::fs::create_dir_all(temp_dir.path().join("assertions/src")).unwrap();

        let mut file =
            File::create(temp_dir.path().join("assertions/src/InvalidContract.sol")).unwrap();
        file.write_all(b"pragma solidity ^0.8.0;\n").unwrap();
        file.write_all(b"contract InvalidContract {\n    function constructor() {}\n}\n")
            .unwrap();
        test_setup.set_assertion_contract("InvalidContract".to_string());
        let mut test_runner = test_setup.build().await.unwrap();
        let res = test_runner.run().await;
        assert!(matches!(
            res,
            Err(DaSubmitError::PhoundryError(ref boxed_error))
            if matches!(**boxed_error, pcl_phoundry::error::PhoundryError::CompilationError(..))
        ));
    }

    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_store_contract_does_not_exist() {
        let mut test_setup = TestSetup::new();
        test_setup.set_assertion_contract("ContractDoesNotExist".to_string());
        let mut test_runner = test_setup.build().await.unwrap();
        let res = test_runner.run().await;
        assert!(matches!(
            res,
            Err(DaSubmitError::PhoundryError(ref boxed_error))
            if matches!(**boxed_error, pcl_phoundry::error::PhoundryError::ContractNotFound(ref s) if s == "ContractDoesNotExist")
        ));
    }

    // No solidity files in directory.
    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_store_no_solidity_files() {
        let mut test_setup = TestSetup::new();
        let temp_dir = tempfile::tempdir().unwrap();
        test_setup.set_root(temp_dir.path().to_path_buf());
        let mut test_runner = test_setup.build().await.unwrap();
        let res = test_runner.run().await;
        assert!(matches!(res, Err(DaSubmitError::PhoundryError(_))));
    }

    // Test DA Submission
    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_store_once() {
        let test_setup = TestSetup::new();
        let mut test_runner = test_setup.build().await.unwrap();
        test_runner.run().await.unwrap();

        test_runner
            .assert_assertion_as_expected("NoArgsAssertion".to_string())
            .await;
    }

    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_store_already_exists() {
        let test_setup = TestSetup::new();
        let mut test_runner = test_setup.build().await.unwrap();

        // Submit assertion first time
        test_runner.run().await.unwrap();

        //assert that the assertion is in the config file
        test_runner
            .assert_assertion_as_expected("NoArgsAssertion".to_string())
            .await;

        // Try to submit same assertion again
        let result = test_runner.run().await;
        assert!(result.is_ok());
        test_runner
            .assert_assertion_as_expected("NoArgsAssertion".to_string())
            .await;
    }
    // Test DA Submission with invalid url
    #[tokio::test]
    async fn test_da_submission_with_invalid_url() {
        let test_setup = TestSetup::new();
        let mut test_runner = test_setup.build().await.unwrap();
        // Override the DA URL to an invalid one
        test_runner.da_store_args.da_url = "not-a-url".to_string();

        let res = test_runner.run().await;
        assert!(matches!(
            res,
            Err(DaSubmitError::DaClientError(DaClientError::UrlParse(..)))
        ));
    }

    // Test DA Submission with constructor args
    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_submission_with_constructor_args_not_supplied() {
        let mut test_setup = TestSetup::new();
        test_setup.set_assertion_contract("MockAssertion".to_string());
        let mut test_runner = test_setup.build().await.unwrap();
        let res = test_runner.run().await;
        assert!(matches!(
            res,
            Err(DaSubmitError::InvalidConstructorArgs(_, _))
        ));
    }

    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_submission_with_constructor_args_supplied() {
        let mut test_setup = TestSetup::new();
        test_setup.set_assertion_contract("MockAssertion".to_string());
        test_setup.set_constructor_args(vec![Address::random().to_string()]);
        let mut test_runner = test_setup.build().await.unwrap();
        test_runner.run().await.unwrap();
    }

    //test submit with incorrect arg types
    #[tokio::test]
    #[cfg(feature = "full-test")]
    async fn test_da_submission_with_constructor_args_supplied_invalid_type() {
        let mut test_setup = TestSetup::new();
        test_setup.set_assertion_contract("MockAssertion".to_string());
        test_setup.set_constructor_args(vec![U256::MAX.to_string()]);
        let mut test_runner = test_setup.build().await.unwrap();
        let res = test_runner.run().await;
        assert!(matches!(
            res,
            Err(DaSubmitError::DaClientError(
                DaClientError::JsonRpcError { .. }
            ))
        ));
    }
}
