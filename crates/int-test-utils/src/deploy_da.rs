use alloy::signers::k256::ecdsa::SigningKey;
use std::net::{
    IpAddr,
    Ipv4Addr,
    SocketAddr,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
/// Deploy a local DA server for testing to a random unused port.
///
/// # Returns
///
/// * `JoinHandle<()>` - Handle to the server task
/// * `SocketAddr` - Socket address the server is bound to
pub async fn deploy_test_da(
    private_key: SigningKey,
) -> (JoinHandle<anyhow::Result<()>>, SocketAddr) {
    let config = assertion_da_server::Config {
        db_path: Some(
            tempfile::tempdir()
                .unwrap()
                .path()
                .to_str()
                .unwrap()
                .to_string()
                .into(),
        ),
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        cache_size: 1024,
        private_key: format!("{:x}", private_key.to_bytes()),
        metrics_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        log_level: tracing::metadata::LevelFilter::current(),
    };

    let server = config.build().await.unwrap();
    let local_addr = server.listener.local_addr().unwrap();
    let handle = tokio::spawn(server.run(CancellationToken::new()));

    (handle, local_addr)
}

#[tokio::test]
async fn test_deploy_da() {
    use alloy::{
        primitives::B256,
        signers::k256::elliptic_curve::rand_core::OsRng,
    };
    use assertion_da_client::DaClient;

    let (_handle, addr) = deploy_test_da(SigningKey::random(&mut OsRng)).await;

    let url = format!("http://{addr}");

    let da_client = DaClient::new(&url).unwrap();

    let res = da_client
        .submit_assertion(
            "SimpleCounterAssertion".to_string(),
            assertion_src(),
            "0.8.28".to_string(),
        )
        .await
        .unwrap();

    assert_ne!(res.id, B256::ZERO);
}

pub fn assertion_src() -> String {
    let assertion = r#"
        // SPDX-License-Identifier: UNLICENSED
        pragma solidity ^0.8.13 ^0.8.28;

        // lib/credible-std/src/PhEvm.sol

        interface PhEvm {
            // An Ethereum log
            struct Log {
                // The topics of the log, including the signature, if any.
                bytes32[] topics;
                // The raw data of the log.
                bytes data;
                // The address of the log's emitter.
                address emitter;
            }

            // Call inputs for the getCallInputs precompile
            struct CallInputs {
                // The call data of the call.
                bytes input;
                /// The gas limit of the call.
                uint64 gas_limit;
                // The account address of bytecode that is going to be executed.
                //
                // Previously `context.code_address`.
                address bytecode_address;
                // Target address, this account storage is going to be modified.
                //
                // Previously `context.address`.
                address target_address;
                // This caller is invoking the call.
                //
                // Previously `context.caller`.
                address caller;
                // Call value.
                //
                // NOTE: This value may not necessarily be transferred from caller to callee, see [`CallValue`].
                //
                // Previously `transfer.value` or `context.apparent_value`.
                uint256 value;
            }

            //Forks to the state prior to the assertion triggering transaction.
            function forkPreTx() external;

            // Forks to the state after the assertion triggering transaction.
            function forkPostTx() external;

            // Loads a storage slot from an address
            function load(address target, bytes32 slot) external view returns (bytes32 data);

            // Get the logs from the assertion triggering transaction.
            function getLogs() external returns (Log[] memory logs);

            // Get the call inputs for a given target and selector
            function getCallInputs(address target, bytes4 selector) external view returns (CallInputs[] memory calls);

            // Get state changes for a given contract and storage slot.
            function getStateChanges(address contractAddress, bytes32 slot)
                external
                view
                returns (bytes32[] memory stateChanges);
        }

        // lib/credible-std/src/TriggerRecorder.sol

        interface TriggerRecorder {
            /// @notice Registers storage change trigger for all slots
            /// @param fnSelector The function selector of the assertion function.
            function registerStorageChangeTrigger(bytes4 fnSelector) external view;

            /// @notice Registers storage change trigger for a slot
            /// @param fnSelector The function selector of the assertion function.
            /// @param slot The storage slot to trigger on.
            function registerStorageChangeTrigger(bytes4 fnSelector, bytes32 slot) external view;

            /// @notice Registers balance change trigger for the AA
            /// @param fnSelector The function selector of the assertion function.
            function registerBalanceChangeTrigger(bytes4 fnSelector) external view;

            /// @notice Registers a call trigger for calls to the AA.
            /// @param fnSelector The function selector of the assertion function.
            /// @param triggerSelector The function selector of the trigger function.
            function registerCallTrigger(bytes4 fnSelector, bytes4 triggerSelector) external view;

            /// @notice Records a call trigger for the specified assertion function.
            /// A call trigger signifies that the assertion function should be called
            /// if the assertion adopter is called.
            /// @param fnSelector The function selector of the assertion function.
            function registerCallTrigger(bytes4 fnSelector) external view;
        }

        // lib/credible-std/src/Credible.sol

        /// @notice The Credible contract
        abstract contract Credible {
            //Precompile address -
            PhEvm constant ph = PhEvm(address(uint160(uint256(keccak256("Kim Jong Un Sucks")))));
        }

        // lib/credible-std/src/StateChanges.sol

        /**
         * @title StateChanges
         * @notice Helper contract for converting state changes from bytes32 arrays to typed arrays
         * @dev Inherits from Credible to access the PhEvm interface
         */
        contract StateChanges is Credible {
            /**
             * @notice Converts state changes for a slot to uint256 array
             * @param contractAddress The address of the contract to get state changes from
             * @param slot The storage slot to get state changes for
             * @return Array of state changes as uint256 values
             */
            function getStateChangesUint(address contractAddress, bytes32 slot) internal view returns (uint256[] memory) {
                bytes32[] memory stateChanges = ph.getStateChanges(contractAddress, slot);

                // Explicit cast to uint256[]
                uint256[] memory uintChanges;
                assembly {
                    uintChanges := stateChanges
                }

                return uintChanges;
            }

            /**
             * @notice Converts state changes for a slot to address array
             * @param contractAddress The address of the contract to get state changes from
             * @param slot The storage slot to get state changes for
             * @return Array of state changes as address values
             */
            function getStateChangesAddress(address contractAddress, bytes32 slot) internal view returns (address[] memory) {
                bytes32[] memory stateChanges = ph.getStateChanges(contractAddress, slot);

                assembly {
                    // Zero out the upper 96 bits for each element to ensure clean address casting
                    for { let i := 0 } lt(i, mload(stateChanges)) { i := add(i, 1) } {
                        let addr :=
                            and(
                                mload(add(add(stateChanges, 0x20), mul(i, 0x20))),
                                0x000000000000000000000000ffffffffffffffffffffffffffffffffffffffff
                            )
                        mstore(add(add(stateChanges, 0x20), mul(i, 0x20)), addr)
                    }
                }

                // Explicit cast to address[]
                address[] memory addressChanges;
                assembly {
                    addressChanges := stateChanges
                }

                return addressChanges;
            }

            /**
             * @notice Converts state changes for a slot to boolean array
             * @param contractAddress The address of the contract to get state changes from
             * @param slot The storage slot to get state changes for
             * @return Array of state changes as boolean values
             */
            function getStateChangesBool(address contractAddress, bytes32 slot) internal view returns (bool[] memory) {
                bytes32[] memory stateChanges = ph.getStateChanges(contractAddress, slot);

                assembly {
                    // Convert each bytes32 to bool
                    for { let i := 0 } lt(i, mload(stateChanges)) { i := add(i, 1) } {
                        // Any non-zero value is true, zero is false
                        let boolValue := iszero(iszero(mload(add(add(stateChanges, 0x20), mul(i, 0x20)))))
                        mstore(add(add(stateChanges, 0x20), mul(i, 0x20)), boolValue)
                    }
                }

                // Explicit cast to bool[]
                bool[] memory boolChanges;
                assembly {
                    boolChanges := stateChanges
                }

                return boolChanges;
            }

            /**
             * @notice Gets raw state changes as bytes32 array
             * @param contractAddress The address of the contract to get state changes from
             * @param slot The storage slot to get state changes for
             * @return Array of state changes as bytes32 values
             */
            function getStateChangesBytes32(address contractAddress, bytes32 slot) internal view returns (bytes32[] memory) {
                return ph.getStateChanges(contractAddress, slot);
            }

            /**
             * @notice Calculates the storage slot for a mapping with a given key and offset
             * @param slot The base storage slot of the mapping
             * @param key The key in the mapping
             * @param offset Additional offset to add to the calculated slot
             * @return The storage slot for the mapping entry
             */
            function getSlotMapping(bytes32 slot, uint256 key, uint256 offset) private pure returns (bytes32) {
                return bytes32(uint256(keccak256(abi.encodePacked(key, slot))) + offset);
            }

            // Helper functions for mapping access with keys

            /**
             * @notice Gets uint256 state changes for a mapping entry
             * @param contractAddress The contract address
             * @param slot The mapping's slot
             * @param key The mapping key
             * @return Array of state changes as uint256 values
             */
            function getStateChangesUint(address contractAddress, bytes32 slot, uint256 key)
                internal
                view
                returns (uint256[] memory)
            {
                return getStateChangesUint(contractAddress, slot, key, 0);
            }

            /**
             * @notice Gets address state changes for a mapping entry
             * @param contractAddress The contract address
             * @param slot The mapping's slot
             * @param key The mapping key
             * @return Array of state changes as address values
             */
            function getStateChangesAddress(address contractAddress, bytes32 slot, uint256 key)
                internal
                view
                returns (address[] memory)
            {
                return getStateChangesAddress(contractAddress, slot, key, 0);
            }

            /**
             * @notice Gets boolean state changes for a mapping entry
             * @param contractAddress The contract address
             * @param slot The mapping's slot
             * @param key The mapping key
             * @return Array of state changes as boolean values
             */
            function getStateChangesBool(address contractAddress, bytes32 slot, uint256 key)
                internal
                view
                returns (bool[] memory)
            {
                return getStateChangesBool(contractAddress, slot, key, 0);
            }

            /**
             * @notice Gets bytes32 state changes for a mapping entry
             * @param contractAddress The contract address
             * @param slot The mapping's slot
             * @param key The mapping key
             * @return Array of state changes as bytes32 values
             */
            function getStateChangesBytes32(address contractAddress, bytes32 slot, uint256 key)
                internal
                view
                returns (bytes32[] memory)
            {
                return getStateChangesBytes32(contractAddress, slot, key, 0);
            }

            // Helper functions for mapping access with keys and offsets

            /**
             * @notice Gets uint256 state changes for a mapping entry with offset
             * @param contractAddress The contract address
             * @param slot The mapping's slot
             * @param key The mapping key
             * @param slotOffset Additional offset to add to the slot
             * @return Array of state changes as uint256 values
             */
            function getStateChangesUint(address contractAddress, bytes32 slot, uint256 key, uint256 slotOffset)
                internal
                view
                returns (uint256[] memory)
            {
                return getStateChangesUint(contractAddress, getSlotMapping(slot, key, slotOffset));
            }

            /**
             * @notice Gets address state changes for a mapping entry with offset
             * @param contractAddress The contract address
             * @param slot The mapping's slot
             * @param key The mapping key
             * @param slotOffset Additional offset to add to the slot
             * @return Array of state changes as address values
             */
            function getStateChangesAddress(address contractAddress, bytes32 slot, uint256 key, uint256 slotOffset)
                internal
                view
                returns (address[] memory)
            {
                return getStateChangesAddress(contractAddress, getSlotMapping(slot, key, slotOffset));
            }

            /**
             * @notice Gets boolean state changes for a mapping entry with offset
             * @param contractAddress The contract address
             * @param slot The mapping's slot
             * @param key The mapping key
             * @param slotOffset Additional offset to add to the slot
             * @return Array of state changes as boolean values
             */
            function getStateChangesBool(address contractAddress, bytes32 slot, uint256 key, uint256 slotOffset)
                internal
                view
                returns (bool[] memory)
            {
                return getStateChangesBool(contractAddress, getSlotMapping(slot, key, slotOffset));
            }

            /**
             * @notice Gets bytes32 state changes for a mapping entry with offset
             * @param contractAddress The contract address
             * @param slot The mapping's slot
             * @param key The mapping key
             * @param slotOffset Additional offset to add to the slot
             * @return Array of state changes as bytes32 values
             */
            function getStateChangesBytes32(address contractAddress, bytes32 slot, uint256 key, uint256 slotOffset)
                internal
                view
                returns (bytes32[] memory)
            {
                return getStateChangesBytes32(contractAddress, getSlotMapping(slot, key, slotOffset));
            }
        }

        // lib/credible-std/src/Assertion.sol

        /// @notice Assertion interface for the PhEvm precompile
        abstract contract Assertion is Credible, StateChanges {
            //Trigger recorder address
            TriggerRecorder constant triggerRecorder = TriggerRecorder(address(uint160(uint256(keccak256("TriggerRecorder")))));

            /// @notice Used to record fn selectors and their triggers.
            function triggers() external view virtual;

            /// @notice Registers a call trigger for the AA without specifying an AA function selector.
            /// This will trigger the assertion function on any call to the AA.
            /// @param fnSelector The function selector of the assertion function.
            function registerCallTrigger(bytes4 fnSelector) internal view {
                triggerRecorder.registerCallTrigger(fnSelector);
            }

            /// @notice Registers a call trigger for calls to the AA with a specific AA function selector.
            /// @param fnSelector The function selector of the assertion function.
            /// @param triggerSelector The function selector upon which the assertion will be triggered.
            function registerCallTrigger(bytes4 fnSelector, bytes4 triggerSelector) internal view {
                triggerRecorder.registerCallTrigger(fnSelector, triggerSelector);
            }

            /// @notice Registers storage change trigger for any slot
            /// @param fnSelector The function selector of the assertion function.
            function registerStorageChangeTrigger(bytes4 fnSelector) internal view {
                triggerRecorder.registerStorageChangeTrigger(fnSelector);
            }

            /// @notice Registers storage change trigger for a specific slot
            /// @param fnSelector The function selector of the assertion function.
            /// @param slot The storage slot to trigger on.
            function registerStorageChangeTrigger(bytes4 fnSelector, bytes32 slot) internal view {
                triggerRecorder.registerStorageChangeTrigger(fnSelector, slot);
            }

            /// @notice Registers balance change trigger for the AA
            /// @param fnSelector The function selector of the assertion function.
            function registerBalanceChangeTrigger(bytes4 fnSelector) internal view {
                triggerRecorder.registerBalanceChangeTrigger(fnSelector);
            }
        }

        // src/SimpleCounterAssertion.sol

        contract Counter {
            uint256 public number;

            function increment() public {
                number++;
            }
        }

        contract SimpleCounterAssertion is Assertion {
            event RunningAssertion(uint256 count);

            function assertCount() public {
                uint256 count = Counter(0x0101010101010101010101010101010101010101).number();
                emit RunningAssertion(count);
                if (count > 1) {
                    revert("Counter cannot be greater than 1");
                }
            }

            function triggers() external view override {
                registerCallTrigger(this.assertCount.selector);
            }
        }

        contract SimpleCounterAssertionWithArgs is Assertion {
            event RunningAssertion(uint256 count);

            uint256 public limit; 
            
            constructor(uint256 _limit) {
                limit = _limit;
            }

            function assertCount() public {
                uint256 count = Counter(0x0101010101010101010101010101010101010101).number();
                emit RunningAssertion(count);
                if (count > limit) {
                    revert("Counter cannot be greater than limit");
                }
            }

            function triggers() external view override {
                registerCallTrigger(this.assertCount.selector);
            }
        }
    "#;
    assertion.to_string()
}
