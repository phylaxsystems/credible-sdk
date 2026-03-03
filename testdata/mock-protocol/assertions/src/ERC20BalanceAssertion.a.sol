// SPDX-License-Identifier: MIT
pragma solidity ^0.8.13;

import {Assertion} from "../../lib/credible-std/src/Assertion.sol";
import {PhEvm} from "../../lib/credible-std/src/PhEvm.sol";

interface IERC20 {
    function totalSupply() external view returns (uint256);
    function balanceOf(address account) external view returns (uint256);
}

/// @title ERC20BalanceAssertion
/// @notice Demonstrates the getMappingStateChanges precompile helpers.
///
/// Asserts that no single ERC20 transfer moves more than 10% of the total
/// supply in one transaction, by watching the `_balances` mapping directly.
///
/// Uses `Experimental` spec for access to the mapping state-changes precompile.
contract ERC20BalanceAssertion is Assertion {
    /// @dev ERC20 `_balances` mapping lives at storage slot 0
    bytes32 constant BALANCES_SLOT = bytes32(uint256(0));

    /// @dev Maximum single-transfer size as a percentage of totalSupply
    uint256 constant MAX_TRANSFER_PCT = 10;

    constructor() {
        registerAssertionSpec(AssertionSpec.Experimental);
    }

    function triggers() external view override {
        registerCallTrigger(this.assertionMaxTransfer.selector);
    }

    function assertionMaxTransfer() external {
        IERC20 token = IERC20(ph.getAssertionAdopter());

        // Get the transfer's `to` address from calldata
        PhEvm.CallInputs[] memory calls = ph.getCallInputs(
            address(token),
            IERC20.balanceOf.selector // proxy: any call that reads balances
        );

        ph.forkPostTx();
        uint256 supply = token.totalSupply();
        uint256 maxAmount = (supply * MAX_TRANSFER_PCT) / 100;

        // --- Old way (manual slot math in Solidity) ---
        // bytes32 slot = keccak256(abi.encodePacked(uint256(uint160(account)), BALANCES_SLOT));
        // uint256[] memory changes = getStateChangesUint(address(token), slot);

        // --- New way (precompile computes the slot for you) ---
        // Check every address that appeared as `from` in a transfer call
        PhEvm.CallInputs[] memory transfers = ph.getCallInputs(
            address(token),
            bytes4(keccak256("transfer(address,uint256)"))
        );

        for (uint256 i = 0; i < transfers.length; i++) {
            address sender = transfers[i].caller;

            // The precompile derives: keccak256(key ++ baseSlot) + offset
            // key = bytes32(uint256(uint160(sender))), offset = 0
            uint256[] memory balanceChanges = getMappingStateChangesUint(
                address(token),
                BALANCES_SLOT,
                bytes32(uint256(uint160(sender)))
            );

            // Walk through every intermediate balance value
            for (uint256 j = 1; j < balanceChanges.length; j++) {
                uint256 prev = balanceChanges[j - 1];
                uint256 curr = balanceChanges[j];

                // Detect decreases (sends)
                if (prev > curr) {
                    uint256 delta = prev - curr;
                    require(delta <= maxAmount, "Transfer exceeds max single-transfer size");
                }
            }
        }
    }
}
