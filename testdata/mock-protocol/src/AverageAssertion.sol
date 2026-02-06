// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";

/// @title AverageAssertion
/// @notice A representative assertion contract for benchmarking purposes.
/// @dev This assertion exercises typical precompile operations:
///      - Forking pre/post transaction state
///      - Loading storage values via PhEvm.load()
///      - Getting state changes
///      - Control flow logic and arithmetic
///      The assertion always passes (never reverts) to allow benchmarks to complete.
contract AverageAssertion is Assertion {
    /// @notice Main assertion check function triggered on any call to the adopter.
    /// @dev Performs a series of representative operations to simulate typical assertion workload.
    function check() external {
        // Get the adopter address that triggered this assertion
        address adopter = ph.getAssertionAdopter();

        // Storage slot 0 is commonly used (e.g., ERC20 balance mapping base)
        bytes32 slot0 = bytes32(uint256(0));
        // Storage slot 4 is used by MockERC20 for _balanceOf mapping
        bytes32 slot4 = bytes32(uint256(4));

        // Fork to pre-transaction state and load storage values
        ph.forkPreTx();
        bytes32 preValue0 = ph.load(adopter, slot0);
        bytes32 preValue4 = ph.load(adopter, slot4);

        // Fork to post-transaction state and load the same storage values
        ph.forkPostTx();
        bytes32 postValue0 = ph.load(adopter, slot0);
        bytes32 postValue4 = ph.load(adopter, slot4);

        // Get state changes for slot 4 (balance mapping)
        bytes32[] memory stateChanges = ph.getStateChanges(adopter, slot4);

        // Perform some arithmetic and control flow to simulate assertion logic
        uint256 pre0 = uint256(preValue0);
        uint256 post0 = uint256(postValue0);
        uint256 pre4 = uint256(preValue4);
        uint256 post4 = uint256(postValue4);

        // Calculate differences (typical in balance-checking assertions)
        uint256 diff0 = post0 >= pre0 ? post0 - pre0 : pre0 - post0;
        uint256 diff4 = post4 >= pre4 ? post4 - pre4 : pre4 - post4;

        // Control flow based on state changes
        uint256 totalChanges = stateChanges.length;
        uint256 checksum = 0;

        // Loop through state changes (exercises iteration)
        for (uint256 i = 0; i < totalChanges && i < 10; i++) {
            checksum += uint256(stateChanges[i]);
        }

        // Additional arithmetic operations
        uint256 combined = diff0 + diff4 + checksum;
        uint256 scaled = combined * 100;
        uint256 shifted = scaled >> 2;

        // Conditional checks (never revert, just observe)
        if (shifted > 0) {
            // State changed - this is expected
            checksum = shifted;
        } else {
            // No significant state change
            checksum = combined;
        }

        // Final dummy operation to ensure the computation isn't optimized away
        assembly {
            // Store result in memory (discarded, but prevents optimization)
            mstore(0x00, checksum)
        }
    }

    /// @notice Registers triggers for this assertion.
    /// @dev Triggers on any call to the adopter (AllCalls trigger type).
    function triggers() external view override {
        registerCallTrigger(this.check.selector);
    }
}
