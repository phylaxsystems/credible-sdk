use crate::primitives::{
    Address,
    address,
};

/// Used to deploy the assertion contract to the forked db, and to call assertion functions.
pub const CALLER: Address = address!("00000000000000000000000000000000000001A4");

/// The address of the assertion contract.
/// This is a fixed address that is used to deploy assertion contracts.
/// Deploying assertion contracts via the caller address @ nonce 0 results in this address
pub const ASSERTION_CONTRACT: Address = address!("63f9abbe8aa6ba1261ef3b0cbfb25a5ff8eeed10");

/// Precompile address
/// address(uint160(uint256(keccak256("Kim Jong Un Sucks"))))
pub const PRECOMPILE_ADDRESS: Address = address!("4461812e00718ff8D80929E3bF595AEaaa7b881E");

/// Default persistent accounts.
/// Journaled state of these accounts will be persisted across forks.
pub const DEFAULT_PERSISTENT_ACCOUNTS: [Address; 3] =
    [ASSERTION_CONTRACT, CALLER, PRECOMPILE_ADDRESS];
