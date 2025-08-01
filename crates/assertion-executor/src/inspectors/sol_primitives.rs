use alloy_sol_types::sol;

sol! {
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
            // id of the call, used to pass to forkCallPre and forkCallPost cheatcodes to access the state
            // before and after the execution of the call.
            uint256 id;
        }

        //Forks to the state prior to the assertion triggering transaction.
        function forkPreTx() external;

        // Forks to the state after the assertion triggering transaction.
        function forkPostTx() external;

        // Forks to the state before the execution of the call.
        // Id can be obtained from the CallInputs struct returned by getCallInputs.
        function forkPreCall(uint256 id) external;

        // Forks to the state after the execution of the call.
        // Id can be obtained from the CallInputs struct returned by getCallInputs.
        function forkPostCall(uint256 id) external;

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

        // Get assertion adopter contract address associated with the assertion triggering transaction.
        function getAssertionAdopter() external view returns (address);
    }

    interface Console {
        // Log a message to the console.
        function log(string message) external;
    }

    #[derive(Debug)]
    error Error(string);
}

sol! {
    interface ITriggerRecorder {

        /// @notice Records a call trigger for the specified assertion function.
        /// A call trigger signifies that the assertion function should be called
        /// if the assertion adopter is called.
        /// @param fnSelector The function selector of the assertion function.
        function registerCallTrigger(bytes4 fnSelector) external view;

        /// @notice Registers a call trigger for calls to the AA.
        /// @param fnSelector The function selector of the assertion function.
        /// @param triggerSelector The function selector of the trigger function.
        function registerCallTrigger(bytes4 fnSelector, bytes4 triggerSelector) external view;

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


    }
    interface console {
        function log(string message) external;
    }
}
