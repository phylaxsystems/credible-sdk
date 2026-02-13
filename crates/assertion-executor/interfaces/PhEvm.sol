// SPDX-License-Identifier: MIT
pragma solidity ^0.8.13;

/// @title PhEvm
/// @author Phylax Systems
/// @notice Precompile interface for accessing transaction state within assertions
/// @dev This interface provides access to the Credible Layer's execution environment,
/// allowing assertions to inspect transaction state, logs, call inputs, and storage changes.
/// The precompile is available at a deterministic address during assertion execution.
interface PhEvm {
    /// @notice Represents an Ethereum log emitted during transaction execution
    /// @dev Used by getLogs() to return transaction logs for inspection
    struct Log {
        /// @notice The topics of the log, including the event signature if any
        bytes32[] topics;
        /// @notice The raw ABI-encoded data of the log
        bytes data;
        /// @notice The address of the contract that emitted the log
        address emitter;
    }

    /// @notice Represents the inputs to a call made during transaction execution
    /// @dev Used by getCallInputs() and related functions to inspect call details
    struct CallInputs {
        /// @notice The calldata of the call
        bytes input;
        /// @notice The gas limit of the call
        uint64 gas_limit;
        /// @notice The address of the bytecode being executed (code address)
        address bytecode_address;
        /// @notice The target address whose storage may be modified
        address target_address;
        /// @notice The address that initiated this call
        address caller;
        /// @notice The ETH value sent with the call
        uint256 value;
        /// @notice Unique identifier for this call, used with forkPreCall/forkPostCall
        uint256 id;
    }

    /// @notice Contains data about the original assertion-triggering transaction
    /// @dev Provides access to transaction envelope data for inspection in assertions
    struct TxObject {
        /// @notice The address that initiated the transaction (tx.origin equivalent)
        address from;
        /// @notice The transaction recipient, or address(0) for contract creation
        address to;
        /// @notice The ETH value sent with the transaction
        uint256 value;
        /// @notice The chain ID, or 0 if not present
        uint64 chain_id;
        /// @notice The gas limit for the transaction
        uint64 gas_limit;
        /// @notice The gas price or max_fee_per_gas for EIP-1559 transactions
        uint128 gas_price;
        /// @notice The transaction calldata
        bytes input;
    }

    /// @notice Filter for narrowing down which calls to consider
    /// @dev Used by scalar cheatcodes (anyCall, countCalls, etc.) to filter calls
    struct CallFilter {
        /// @notice Call type: 0=any, 1=CALL, 2=STATICCALL, 3=DELEGATECALL, 4=CALLCODE
        uint8 callType;
        /// @notice Minimum call depth (inclusive), 0 means no minimum
        uint32 minDepth;
        /// @notice Maximum call depth (inclusive), 0 means no maximum
        uint32 maxDepth;
        /// @notice If true, only consider top-level calls (depth == 0)
        bool topLevelOnly;
    }

    /// @notice Specifies a point relative to a call's execution
    /// @dev Used by loadAtCall and slotDeltaAtCall
    enum CallPoint {
        PreCall,
        PostCall
    }

    /// @notice Context about the trigger that caused this assertion to run
    struct TriggerContext {
        /// @notice The call ID that triggered the assertion (from CallTracer)
        uint256 callId;
        /// @notice The caller of the triggering call
        address caller;
        /// @notice The target address of the triggering call
        address target;
        /// @notice The code address of the triggering call
        address codeAddress;
        /// @notice The function selector of the triggering call
        bytes4 selector;
        /// @notice The nesting depth of the triggering call
        uint32 depth;
    }

    /// @notice Key-value pair used for grouped address aggregates
    struct AddressUint {
        address key;
        uint256 value;
    }

    /// @notice Key-value pair used for grouped bytes32/topic aggregates
    struct Bytes32Uint {
        bytes32 key;
        uint256 value;
    }

    /// @notice Fork to the state before the assertion-triggering transaction
    /// @dev Allows inspection of pre-transaction state for comparison
    function forkPreTx() external;

    /// @notice Fork to the state after the assertion-triggering transaction
    /// @dev Allows inspection of post-transaction state for validation
    function forkPostTx() external;

    /// @notice Fork to the state before a specific call execution
    /// @dev Useful for inspecting state at specific points during transaction execution
    /// @param id The call identifier from CallInputs.id
    function forkPreCall(uint256 id) external;

    /// @notice Fork to the state after a specific call execution
    /// @dev Useful for inspecting state changes from specific calls
    /// @param id The call identifier from CallInputs.id
    function forkPostCall(uint256 id) external;

    /// @notice Load a storage slot value from any address
    /// @param target The address to read storage from
    /// @param slot The storage slot to read
    /// @return data The value stored at the slot
    function load(address target, bytes32 slot) external view returns (bytes32 data);

    /// @notice Get all logs emitted during the transaction
    /// @dev Returns logs in emission order
    /// @return logs Array of Log structs containing all emitted events
    function getLogs() external returns (Log[] memory logs);

    /// @notice Get all call inputs for a target and selector (all call types)
    /// @dev Includes CALL, STATICCALL, DELEGATECALL, and CALLCODE
    /// @param target The target contract address
    /// @param selector The function selector to filter by
    /// @return calls Array of CallInputs matching the criteria
    function getAllCallInputs(address target, bytes4 selector) external view returns (CallInputs[] memory calls);

    /// @notice Get call inputs for regular CALL opcode only
    /// @param target The target contract address
    /// @param selector The function selector to filter by
    /// @return calls Array of CallInputs from CALL opcodes
    function getCallInputs(address target, bytes4 selector) external view returns (CallInputs[] memory calls);

    /// @notice Get call inputs for STATICCALL opcode only
    /// @param target The target contract address
    /// @param selector The function selector to filter by
    /// @return calls Array of CallInputs from STATICCALL opcodes
    function getStaticCallInputs(address target, bytes4 selector) external view returns (CallInputs[] memory calls);

    /// @notice Get call inputs for DELEGATECALL opcode only
    /// @param target The target/proxy contract address
    /// @param selector The function selector to filter by
    /// @return calls Array of CallInputs from DELEGATECALL opcodes
    function getDelegateCallInputs(address target, bytes4 selector) external view returns (CallInputs[] memory calls);

    /// @notice Get call inputs for CALLCODE opcode only
    /// @param target The target contract address
    /// @param selector The function selector to filter by
    /// @return calls Array of CallInputs from CALLCODE opcodes
    function getCallCodeInputs(address target, bytes4 selector) external view returns (CallInputs[] memory calls);

    /// @notice Get all state changes for a specific storage slot
    /// @dev Returns the sequence of values the slot held during transaction execution
    /// @param contractAddress The contract whose storage to inspect
    /// @param slot The storage slot to get changes for
    /// @return stateChanges Array of values the slot held (in order of changes)
    function getStateChanges(address contractAddress, bytes32 slot)
        external
        view
        returns (bytes32[] memory stateChanges);

    /// @notice Get the assertion adopter address for the current transaction
    /// @dev The adopter is the contract that registered the assertion
    /// @return The address of the assertion adopter contract
    function getAssertionAdopter() external view returns (address);

    /// @notice Get the original transaction object that triggered the assertion
    /// @dev Returns the transaction envelope data for the assertion-triggering tx
    /// @return txObject The transaction data struct
    function getTxObject() external view returns (TxObject memory txObject);

    // ─── Scalar call-fact cheatcodes ───

    /// @notice Check if any call matching (target, selector, filter) exists
    /// @param target The target contract address
    /// @param selector The function selector to match
    /// @param filter Criteria for narrowing down which calls to consider
    /// @return found True if at least one matching call was recorded
    function anyCall(address target, bytes4 selector, CallFilter calldata filter) external view returns (bool found);

    /// @notice Count calls matching (target, selector, filter)
    /// @param target The target contract address
    /// @param selector The function selector to match
    /// @param filter Criteria for narrowing down which calls to consider
    /// @return count Number of matching calls
    function countCalls(address target, bytes4 selector, CallFilter calldata filter) external view returns (uint256 count);

    /// @notice Get the caller of a specific call by its ID
    /// @param callId The call identifier (index in the call trace)
    /// @return caller The address that initiated the call
    function callerAt(uint256 callId) external view returns (address caller);

    /// @notice Check if all matching calls were made by a specific caller
    /// @param target The target contract address
    /// @param selector The function selector to match
    /// @param allowedCaller The caller address that all calls must come from
    /// @param filter Criteria for narrowing down which calls to consider
    /// @return ok True if no calls match, or all matching calls have the allowed caller
    function allCallsBy(address target, bytes4 selector, address allowedCaller, CallFilter calldata filter) external view returns (bool ok);

    /// @notice Sum a uint256 argument across all matching calls
    /// @param target The target contract address
    /// @param selector The function selector to match
    /// @param argIndex The ABI word index of the uint256 argument (0-based)
    /// @param filter Criteria for narrowing down which calls to consider
    /// @return total The sum of the argument values across matching calls
    function sumArgUint(address target, bytes4 selector, uint256 argIndex, CallFilter calldata filter) external view returns (uint256 total);

    /// @notice Sum a uint256 argument for calls where an address argument equals `key`
    /// @param target The target contract address
    /// @param selector The function selector to match
    /// @param keyArgIndex ABI word index of the address key argument (0-based)
    /// @param key Address key to filter by
    /// @param valueArgIndex ABI word index of the uint256 value argument (0-based)
    /// @param filter Criteria for narrowing down which calls to consider
    /// @return total Sum of value arguments for matching key
    function sumCallArgUintForAddress(
        address target,
        bytes4 selector,
        uint256 keyArgIndex,
        address key,
        uint256 valueArgIndex,
        CallFilter calldata filter
    ) external view returns (uint256 total);

    /// @notice Return unique address values observed at a call argument index
    /// @param target The target contract address
    /// @param selector The function selector to match
    /// @param argIndex ABI word index of the address argument (0-based)
    /// @param filter Criteria for narrowing down which calls to consider
    /// @return keys Unique addresses in deterministic sorted order
    function uniqueCallArgAddresses(address target, bytes4 selector, uint256 argIndex, CallFilter calldata filter)
        external
        view
        returns (address[] memory keys);

    /// @notice Group and sum uint256 argument values by an address argument key
    /// @param target The target contract address
    /// @param selector The function selector to match
    /// @param keyArgIndex ABI word index of the address key argument (0-based)
    /// @param valueArgIndex ABI word index of the uint256 value argument (0-based)
    /// @param filter Criteria for narrowing down which calls to consider
    /// @return entries Grouped sums sorted by key
    function sumCallArgUintByAddress(
        address target,
        bytes4 selector,
        uint256 keyArgIndex,
        uint256 valueArgIndex,
        CallFilter calldata filter
    ) external view returns (AddressUint[] memory entries);

    /// @notice Sum uint256 event data values for logs with a specific topic key
    /// @param emitter Log emitter address
    /// @param topic0 Event signature topic
    /// @param keyTopicIndex Topic index to use as key (0..3)
    /// @param key Topic key value to match
    /// @param valueDataIndex ABI word index in log data for uint256 value (0-based)
    /// @return total Sum of matching values
    function sumEventUintForTopicKey(
        address emitter,
        bytes32 topic0,
        uint8 keyTopicIndex,
        bytes32 key,
        uint256 valueDataIndex
    ) external view returns (uint256 total);

    /// @notice Return unique topic values for logs matching emitter/topic0
    /// @param emitter Log emitter address
    /// @param topic0 Event signature topic
    /// @param topicIndex Topic index to extract unique values from (0..3)
    /// @return values Unique topic values in deterministic sorted order
    function uniqueEventTopicValues(address emitter, bytes32 topic0, uint8 topicIndex)
        external
        view
        returns (bytes32[] memory values);

    /// @notice Group and sum uint256 event data values by topic key
    /// @param emitter Log emitter address
    /// @param topic0 Event signature topic
    /// @param keyTopicIndex Topic index to use as grouping key (0..3)
    /// @param valueDataIndex ABI word index in log data for uint256 value (0-based)
    /// @return entries Grouped sums sorted by key
    function sumEventUintByTopic(address emitter, bytes32 topic0, uint8 keyTopicIndex, uint256 valueDataIndex)
        external
        view
        returns (Bytes32Uint[] memory entries);

    // ─── Storage write-policy cheatcodes ───

    /// @notice Check if a specific storage slot was written during the transaction
    /// @param target The contract address whose storage to inspect
    /// @param slot The storage slot to check
    /// @return written True if the slot was modified
    function anySlotWritten(address target, bytes32 slot) external view returns (bool written);

    /// @notice Check if all writes to a specific slot were made by a specific caller
    /// @param target The contract address whose storage to inspect
    /// @param slot The storage slot to check
    /// @param allowedCaller The caller address that all writes must come from
    /// @return ok True if no writes occurred, or all writes were by the allowed caller
    function allSlotWritesBy(address target, bytes32 slot, address allowedCaller) external view returns (bool ok);

    // ─── Call-boundary state cheatcodes ───

    /// @notice Load a storage slot at a specific call boundary
    /// @param target The address to read storage from
    /// @param slot The storage slot to read
    /// @param callId The call identifier
    /// @param point Whether to read before or after the call
    /// @return data The value stored at the slot at the specified point
    function loadAtCall(address target, bytes32 slot, uint256 callId, CallPoint point) external view returns (bytes32 data);

    /// @notice Compute the delta of a storage slot across a call
    /// @param target The address to read storage from
    /// @param slot The storage slot to read
    /// @param callId The call identifier
    /// @return delta The signed difference (post - pre)
    function slotDeltaAtCall(address target, bytes32 slot, uint256 callId) external view returns (int256 delta);

    // ─── Trigger context cheatcode ───

    /// @notice Get the context of the trigger that caused this assertion to run
    /// @return ctx The trigger context with call details
    function getTriggerContext() external view returns (TriggerContext memory ctx);

    // ─── ERC20 fact cheatcodes ───

    /// @notice Get the change in ERC20 balance for an account across the transaction
    /// @param token The ERC20 token contract address
    /// @param account The account to check balance for
    /// @return delta The signed balance change (post - pre)
    function erc20BalanceDiff(address token, address account) external view returns (int256 delta);

    /// @notice Get the change in ERC20 total supply across the transaction
    /// @param token The ERC20 token contract address
    /// @return delta The signed supply change (post - pre)
    function erc20SupplyDiff(address token) external view returns (int256 delta);

    /// @notice Get the net ERC20 token flow for an account from Transfer events
    /// @param token The ERC20 token contract address
    /// @param account The account to compute net flow for
    /// @return netFlow The signed net flow (received - sent)
    function getERC20NetFlow(address token, address account) external view returns (int256 netFlow);

    /// @notice Get the ERC20 token flow for an account within a specific call's scope
    /// @param token The ERC20 token contract address
    /// @param account The account to compute net flow for
    /// @param callId The call identifier to scope events to
    /// @return netFlow The signed net flow within the call's scope
    function getERC20FlowByCall(address token, address account, uint256 callId) external view returns (int256 netFlow);

    // ─── ERC4626 fact cheatcodes ───

    /// @notice Get the change in ERC4626 total assets across the transaction
    /// @param vault The ERC4626 vault address
    /// @return delta The signed change in totalAssets (post - pre)
    function erc4626TotalAssetsDiff(address vault) external view returns (int256 delta);

    /// @notice Get the change in ERC4626 total share supply across the transaction
    /// @param vault The ERC4626 vault address
    /// @return delta The signed change in totalSupply (post - pre)
    function erc4626TotalSupplyDiff(address vault) external view returns (int256 delta);

    /// @notice Get the change in the vault's underlying asset token balance across the transaction
    /// @dev Uses vault.asset() and ERC20(asset).balanceOf(vault) at pre/post tx state.
    /// @param vault The ERC4626 vault address
    /// @return delta The signed change in the vault's asset balance (post - pre)
    function erc4626VaultAssetBalanceDiff(address vault) external view returns (int256 delta);

    /// @notice Get the change in assets-per-share ratio in basis points across the transaction
    /// @dev Computes floor(totalAssets * 10_000 / totalSupply) at pre/post state and returns post-pre.
    /// @param vault The ERC4626 vault address
    /// @return deltaBps Signed change in assets-per-share ratio in bps
    function erc4626AssetsPerShareDiffBps(address vault) external view returns (int256 deltaBps);

    // ─── P1: State/Mapping diff cheatcodes ───

    /// @notice Get all storage slots that changed for a target address during the transaction
    /// @dev Only returns slots where the final value differs from the pre-tx value
    /// @param target The contract address to inspect
    /// @return slots Array of storage slot keys that were modified
    function getChangedSlots(address target) external view returns (bytes32[] memory slots);

    /// @notice Get the pre and post values of a storage slot across the transaction
    /// @dev If the slot was not modified, returns the loaded value (or zero) with changed=false
    /// @param target The contract address to inspect
    /// @param slot The storage slot to diff
    /// @return pre The value before the transaction
    /// @return post The value after the transaction
    /// @return changed True if the value changed
    function getSlotDiff(address target, bytes32 slot) external view returns (bytes32 pre, bytes32 post, bool changed);

    /// @notice Check if a Solidity mapping entry's storage slot was modified
    /// @dev Computes slot as keccak256(key ++ baseSlot) + fieldOffset per Solidity storage layout
    /// @param target The contract address
    /// @param baseSlot The storage slot of the mapping declaration
    /// @param key The mapping key (left-padded to bytes32)
    /// @param fieldOffset Struct field offset (0 for simple value mappings)
    /// @return changed True if the computed slot was modified
    function didMappingKeyChange(address target, bytes32 baseSlot, bytes32 key, uint256 fieldOffset) external view returns (bool changed);

    /// @notice Get pre/post values for a Solidity mapping entry's storage slot
    /// @dev Computes slot as keccak256(key ++ baseSlot) + fieldOffset per Solidity storage layout
    /// @param target The contract address
    /// @param baseSlot The storage slot of the mapping declaration
    /// @param key The mapping key (left-padded to bytes32)
    /// @param fieldOffset Struct field offset (0 for simple value mappings)
    /// @return pre The value before the transaction
    /// @return post The value after the transaction
    /// @return changed True if the value changed
    function mappingValueDiff(address target, bytes32 baseSlot, bytes32 key, uint256 fieldOffset) external view returns (bytes32 pre, bytes32 post, bool changed);

    /// @notice Check if an ERC20 token balance changed for an account
    /// @dev Based on Transfer event scanning, equivalent to erc20BalanceDiff != 0
    /// @param token The ERC20 token contract address
    /// @param account The account to check
    /// @return changed True if the net balance changed
    function didBalanceChange(address token, address account) external view returns (bool changed);

    /// @notice Get the ERC20 balance change for an account across the transaction
    /// @dev Equivalent to erc20BalanceDiff — computes net flow from Transfer events
    /// @param token The ERC20 token contract address
    /// @param account The account to check
    /// @return delta The signed balance change
    function balanceDiff(address token, address account) external view returns (int256 delta);
}
