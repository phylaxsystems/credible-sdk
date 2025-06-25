use alloy::sol_types::{
    sol,
    SolCall,
};
use alloy_primitives::B256;
use alloy_provider::network::TransactionBuilder;
use alloy_rpc_types::TransactionRequest;
use assertion_da_client::DaSubmissionResponse;
use assertion_executor::primitives::Address;

sol! {
    function addAssertion(address contractAddress, bytes32 assertionId, bytes calldata metadata, bytes calldata proof);
    function removeAssertion(address contractAddress, bytes32 assertionId);
    function registerAssertionAdopter(address contractAddress, address adminVerifier, bytes calldata data);
    function owner() external view returns (address);
}

/// Generates a TransactionRequest for addAssertion
pub fn add_assertion_tx(
    state_oracle: Address,
    contract_address: Address,
    deployer: Address,
    da_submission_response: DaSubmissionResponse,
) -> TransactionRequest {
    let assertion_id = da_submission_response.id;
    let input = addAssertionCall {
        contractAddress: contract_address,
        assertionId: assertion_id,
        metadata: Default::default(),
        proof: da_submission_response.prover_signature,
    }
    .abi_encode();
    TransactionRequest::default()
        .with_to(state_oracle)
        .with_input(input)
        .from(deployer)
}

/// Generates a TransactionRequest for removeAssertion
pub fn remove_assertion_tx(
    state_oracle: Address,
    contract_address: Address,
    assertion_id: B256,
    deployer: Address,
) -> TransactionRequest {
    let input = removeAssertionCall {
        contractAddress: contract_address,
        assertionId: assertion_id,
    }
    .abi_encode();
    TransactionRequest::default()
        .with_to(state_oracle)
        .with_input(input)
        .from(deployer)
}

/// Helper function to create a transaction to register an assertion adopter
pub fn register_assertion_adopter_tx(
    state_oracle: Address,
    contract_address: Address,
    admin_verifier: Address,
    deployer: Address,
) -> TransactionRequest {
    TransactionRequest::default()
        .with_to(state_oracle)
        .with_input(
            registerAssertionAdopterCall {
                contractAddress: contract_address,
                adminVerifier: admin_verifier,
                data: Default::default(),
            }
            .abi_encode(),
        )
        .from(deployer)
}
