use clap::{
    Parser,
    ValueHint,
};
use foundry_cli::opts::{
    BuildOpts,
    ProjectPathOpts,
};

use std::path::PathBuf;

use crate::compile::compile;
use crate::error::PhoundryError;

/// Command-line arguments for building assertion contracts and tests.
#[derive(Debug, Default, Parser)]
#[clap(about = "Build contracts using Phorge")]
pub struct BuildArgs {
    /// Root directory of the project
    #[clap(
        long,
        value_hint = ValueHint::DirPath,
        help = "Root directory of the project"
    )]
    pub root: Option<PathBuf>,
}

impl BuildArgs {
    /// Builds the assertion contract and tests
    ///
    /// # Returns
    ///
    /// - `Ok(())`
    /// - `Err(PhoundryError)` if any step in the process fails
    pub fn run(&self) -> Result<(), Box<PhoundryError>> {
        let build_cmd = BuildOpts {
            project_paths: ProjectPathOpts {
                root: self.root.clone(),
                // FIXME(Odysseas): this essentially hard-codes the location of the assertions to live in
                // assertions/src
                contracts: Some(PathBuf::from("assertions/src")),
                ..Default::default()
            },
            ..Default::default()
        };

        foundry_cli::utils::load_dotenv();

        compile(build_cmd)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // Helper function to create a temporary Solidity project with valid contracts
    fn setup_valid_test_project() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let project_root = temp_dir.path().join("test_project");
        fs::create_dir_all(&project_root).unwrap();

        // Create assertions/src directory structure
        let contract_dir = project_root.join("assertions").join("src");
        fs::create_dir_all(&contract_dir).unwrap();

        // Create a valid test contract
        let contract_path = contract_dir.join("ValidContract.sol");
        fs::write(
            &contract_path,
            r#"// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract ValidContract {
    function test() public pure returns (bool) {
        return true;
    }
}"#,
        )
        .unwrap();

        (temp_dir, project_root)
    }

    // Helper function to create a temporary Solidity project with compilation errors
    fn setup_invalid_test_project() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let project_root = temp_dir.path().join("test_project");
        fs::create_dir_all(&project_root).unwrap();

        // Create assertions/src directory structure
        let contract_dir = project_root.join("assertions").join("src");
        fs::create_dir_all(&contract_dir).unwrap();

        // Create a contract with compilation errors
        let contract_path = contract_dir.join("InvalidContract.sol");
        fs::write(
            &contract_path,
            r#"// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract InvalidContract {
    // Missing semicolon - syntax error
    uint256 public value = 42
    
    // Invalid function syntax - missing parentheses
    function test public pure returns (bool) {
        // Type mismatch error
        return "not a boolean";
    }
    
    // Undefined variable error
    function anotherTest() public pure returns (uint256) {
        return undefinedVariable;
    }
    
    // Missing closing brace
}"#,
        )
        .unwrap();

        (temp_dir, project_root)
    }

    // Helper function to create an empty project (no source files)
    fn setup_empty_test_project() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let project_root = temp_dir.path().join("test_project");
        fs::create_dir_all(&project_root).unwrap();

        // Create assertions/src directory but leave it empty
        let contract_dir = project_root.join("assertions").join("src");
        fs::create_dir_all(&contract_dir).unwrap();

        (temp_dir, project_root)
    }

    #[test]
    fn test_build_args_new() {
        let args = BuildArgs { root: None };

        assert!(args.root.is_none());
    }

    #[test]
    fn test_build_args_with_root() {
        let root_path = PathBuf::from("/test/path");
        let args = BuildArgs {
            root: Some(root_path.clone()),
        };

        assert_eq!(args.root, Some(root_path));
    }

    #[test]
    fn test_compilation_with_invalid_contract() {
        let (_temp_dir, project_root) = setup_invalid_test_project();

        let args = BuildArgs {
            root: Some(project_root),
        };

        let result = args.run();

        // Compilation should fail due to syntax errors
        assert!(
            result.is_err(),
            "Expected compilation to fail with invalid contract"
        );
    }

    #[test]
    fn test_compilation_with_empty_directory() {
        let (_temp_dir, project_root) = setup_empty_test_project();

        let args = BuildArgs {
            root: Some(project_root),
        };

        let result = args.run();

        // Compilation should fail due to no source files
        assert!(
            result.is_err(),
            "Expected compilation to fail with empty directory"
        );
    }

    #[test]
    fn test_compilation_with_nonexistent_directory() {
        let temp_dir = TempDir::new().unwrap();
        let nonexistent_path = temp_dir.path().join("nonexistent_project");

        let args = BuildArgs {
            root: Some(nonexistent_path),
        };

        let result = args.run();

        assert!(
            result.is_err(),
            "Expected compilation to fail with nonexistent directory"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_build_integration_with_valid_contract() {
        let (_temp_dir, project_root) = setup_valid_test_project();

        let args = BuildArgs {
            root: Some(project_root),
        };

        let result = args.run();

        assert!(result.is_ok());
    }
}
