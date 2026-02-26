// SPDX-License-Identifier: MIT
pragma solidity ^0.8.13;

/// @title ITriggerRecorder
/// @author Phylax Systems
/// @notice Precompile interface for registering assertion triggers
/// @dev Used within the `triggers()` function of assertion contracts to specify
/// when assertions should be executed. Supports call triggers, storage change triggers,
/// and balance change triggers.
///
/// NOTE: The interface is named ITriggerRecorder in the executor bindings.
/// The downstream credible-std exposes it as TriggerRecorder (without I prefix).
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
