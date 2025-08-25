use alloy::signers::k256::ecdsa::SigningKey;
use std::process::{
    Command,
    ExitStatus,
    Stdio,
};

use alloy::{
    node_bindings::AnvilInstance,
    primitives::Address,
};
use std::{
    io,
    str::FromStr,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DeployContractsError {
    #[error("Command execution failed. Exit Status: {0}, Error: {1}")]
    CommandError(ExitStatus, String),
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
}

pub fn get_anvil_deployer(anvil_instance: &AnvilInstance) -> SigningKey {
    // Get the first private key from anvil's default accounts
    anvil_instance.keys()[0].clone().into()
}

/// Deploy the CREATE3 factory contract
///
/// # Arguments
///
/// * `anvil` - The Anvil instance to deploy to
/// * `project_root` - Root directory of the credible-layer-contracts repo
///
///
/// # Returns
///
/// * `Result<(), DeployContractsError>` - Success or error
pub fn deploy_create_factory(
    funder_private_key: &SigningKey,
    project_root: std::path::PathBuf,
    rpc_url: &str,
) -> Result<(), DeployContractsError> {
    let funder_pk_bytes = funder_private_key.to_bytes();
    let script_path = project_root.join("shell/deploy_create_x.sh");
    let fmt_funder_key = format!("{funder_pk_bytes:x}");

    // Execute the shell script
    let mut cmd = Command::new("bash");
    cmd.arg(script_path)
        .env("RPC_URL", rpc_url)
        .env("FUNDER_PRIVATE_KEY", fmt_funder_key)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = cmd.output()?;
    if !output.status.success() {
        return Err(DeployContractsError::CommandError(
            output.status,
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    Ok(())
}

#[derive(Debug)]
pub struct Contracts {
    pub state_oracle: Address,
    pub admin_verifier: Address,
    pub da_verifier: Address,
}

/// Deploy contracts using the provided configuration
///
/// # Arguments
///
/// * `anvil` - The Anvil instance to deploy to
/// * `assertion_da_private_key` - Private key for the DA prover
/// * `project_root` - Root directory of the credible-layer-contracts repo
/// * `state_oracle_assertion_timelock_blocks` - Number of blocks to timelock assertions for
///
/// # Returns
///
/// * `Result<Contracts, DeployContractsError>` - The deployed contract addresses or an error
pub fn deploy_contracts(
    anvil: &AnvilInstance,
    assertion_da_private_key: SigningKey,
    project_root: std::path::PathBuf,
    state_oracle_assertion_timelock_blocks: usize,
) -> Result<Contracts, DeployContractsError> {
    let deployer_private_key = get_anvil_deployer(anvil);

    let rpc_url = anvil.endpoint();
    // Validate inputs
    deploy_create_factory(
        &deployer_private_key,
        project_root.clone(),
        &anvil.endpoint(),
    )?;

    let project_root = project_root.to_string_lossy();

    let deployer_pk_bytes = deployer_private_key.to_bytes();
    // Build the bash script with the provided arguments
    let mut cmd = Command::new("forge");
    cmd.arg("script")
        .arg("DeployCore")
        .arg("--rpc-url")
        .arg(rpc_url)
        .arg("--private-key")
        .arg(format!("{deployer_pk_bytes:x}"))
        .arg("--root")
        .arg(project_root.as_ref())
        .arg("--broadcast")
        .env("STATE_ORACLE_MAX_ASSERTIONS_PER_AA", "10")
        .env(
            "STATE_ORACLE_ASSERTION_TIMELOCK_BLOCKS",
            state_oracle_assertion_timelock_blocks.to_string(),
        )
        .env("STATE_ORACLE_ADMIN_ADDRESS", {
            let address = Address::from_private_key(&deployer_private_key);
            format!("{address:#x}")
        })
        .env("DA_PROVER_ADDRESS", {
            let address = Address::from_private_key(&assertion_da_private_key);
            format!("{address:#x}")
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = cmd.output()?;

    // Check if the script executed successfully
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut state_oracle = None;
        let mut admin_verifier = None;
        let mut da_verifier = None;
        for line in stdout.lines() {
            if let Some(addr) = line.strip_prefix("  State Oracle Proxy deployed at ") {
                state_oracle = Some(addr.trim().to_string());
            } else if let Some(addr) = line.strip_prefix("  Admin Verifier deployed at ") {
                admin_verifier = Some(addr.trim().to_string());
            } else if let Some(addr) = line.strip_prefix("  DA Verifier deployed at ") {
                da_verifier = Some(addr.trim().to_string());
            }
        }
        match (state_oracle, admin_verifier, da_verifier) {
            (Some(state_oracle), Some(admin_verifier), Some(da_verifier)) => {
                let state_oracle = Address::from_str(&state_oracle).map_err(|e| {
                    DeployContractsError::CommandError(
                        output.status,
                        format!("Failed to parse state_oracle address: {e}"),
                    )
                })?;
                let admin_verifier = Address::from_str(&admin_verifier).map_err(|e| {
                    DeployContractsError::CommandError(
                        output.status,
                        format!("Failed to parse admin_verifier address: {e}"),
                    )
                })?;
                let da_verifier = Address::from_str(&da_verifier).map_err(|e| {
                    DeployContractsError::CommandError(
                        output.status,
                        format!("Failed to parse da_verifier address: {e}"),
                    )
                })?;
                Ok(Contracts {
                    state_oracle,
                    admin_verifier,
                    da_verifier,
                })
            }
            _ => {
                Err(DeployContractsError::CommandError(
                    output.status,
                    format!("Failed to parse contract addresses from output: {stdout}"),
                ))
            }
        }
    } else {
        Err(DeployContractsError::CommandError(
            output.status,
            String::from_utf8_lossy(&output.stderr).to_string(),
        ))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use alloy::{
        network::{
            EthereumWallet,
            TransactionBuilder,
        },
        node_bindings::{
            Anvil,
            AnvilInstance,
        },
        primitives::{
            Address,
            TxKind,
            U256,
        },
        providers::{
            Provider,
            ProviderBuilder,
        },
        rpc::types::TransactionRequest,
        signers::{
            k256::ecdsa::SigningKey,
            local::LocalSigner,
        },
    };
    use std::error::Error;

    fn setup_anvil() -> Result<AnvilInstance, Box<dyn Error>> {
        // Configure and spawn anvil
        let anvil = Anvil::new().spawn();

        Ok(anvil)
    }

    #[test]
    fn test_deploy_contracts() -> Result<(), Box<dyn Error>> {
        let anvil = setup_anvil()?;

        use alloy::signers::k256::elliptic_curve::rand_core::OsRng;

        let result = deploy_contracts(
            &anvil,
            SigningKey::random(&mut OsRng), // Using same key for both roles in test
            std::path::PathBuf::from("../../lib/credible-layer-contracts"),
            5,
        );

        // Anvil instance will automatically be killed when dropped

        // Check deployment result
        assert!(
            result.is_ok(),
            "Contract deployment failed: {:?}",
            result.err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_deploy_contracts_fails_without_funds() -> Result<(), Box<dyn Error>> {
        let anvil = setup_anvil()?;

        // Generate a key and drain its funds
        let deployer_key = get_anvil_deployer(&anvil);
        let deployer_address = Address::from_private_key(&deployer_key);

        // Send all funds to a different address
        let drain_to = Address::random();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint().parse()?);
        let balance = provider.get_balance(deployer_address).await?;

        let wallet = LocalSigner::from(deployer_key.clone());
        let wallet = EthereumWallet::from(wallet);
        let chain_id = provider.get_chain_id().await?;

        // Build the transaction
        let nonce = provider.get_transaction_count(deployer_address).await?;

        let gas_price = provider.get_gas_price().await?;
        let gas_limit = U256::from(21_000u64);
        let gas_cost = gas_limit * U256::from(gas_price);
        let value = if balance > gas_cost {
            balance - gas_cost
        } else {
            U256::ZERO
        };

        let tx = TransactionRequest {
            from: Some(deployer_address),
            to: Some(TxKind::Call(drain_to)),
            value: Some(value),
            chain_id: Some(chain_id),
            nonce: Some(nonce),
            max_fee_per_gas: Some(gas_price),
            max_priority_fee_per_gas: Some(50),
            gas: Some(gas_limit.try_into().unwrap()),
            gas_price: Some(gas_price),

            ..Default::default()
        };

        let tx = tx.build(&wallet).await?;
        let pending_tx = provider.send_tx_envelope(tx).await?;
        let rcpt = pending_tx.get_receipt().await?;

        // Assert the transaction receipt exists and succeeded
        assert!(
            rcpt.status(),
            "Draining transaction failed or was not mined"
        );

        println!("Deploying contracts");
        let result = deploy_contracts(
            &anvil,
            deployer_key,
            std::path::PathBuf::from("../../lib/credible-layer-contracts"),
            5,
        );

        // Check deployment fails
        assert!(
            result.is_err(),
            "Contract deployment should have failed but succeeded"
        );
        let res_str = result.err().unwrap().to_string();
        assert!(
            res_str.contains("Insufficient funds"),
            "Expected error to contain 'Insufficient funds', got: {res_str}"
        );
        Ok(())
    }

    #[test]
    fn test_deploy_create_factory() -> Result<(), Box<dyn Error>> {
        let anvil = setup_anvil()?;

        let deployer_key = get_anvil_deployer(&anvil);
        let result = deploy_create_factory(
            &deployer_key,
            std::path::PathBuf::from("../../lib/credible-layer-contracts"),
            &anvil.endpoint(),
        );

        // Check deployment result
        assert!(
            result.is_ok(),
            "Deployer deployment failed: {:?}",
            result.err()
        );
        Ok(())
    }
}
