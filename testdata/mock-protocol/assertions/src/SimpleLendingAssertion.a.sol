// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "../../lib/credible-std/src/Assertion.sol";
import {SimpleLending} from "../../src/SimpleLending.sol";
import {IPriceFeed} from "../../src/SimpleLending.sol";
import {console} from "../../lib/credible-std/lib/forge-std/src/console.sol";
import {PhEvm} from "../../lib/credible-std/src/PhEvm.sol";

contract SimpleLendingAssertion is Assertion {
    SimpleLending simpleLending;

    constructor(address simpleLending_) {
        simpleLending = SimpleLending(simpleLending_);
    }

    function triggers() external view override {
        registerCallTrigger(this.assertionIndividualPosition.selector);
        registerCallTrigger(this.assertionBorrowedInvariant.selector);
        registerCallTrigger(this.assertionEthDrain.selector);
    }

    // Verify that total borrowed tokens never exceed total collateral value
    function assertionBorrowedInvariant() external {
        ph.forkPostState();

        // Get price feeds directly from the lending contract
        IPriceFeed ethPriceFeed = simpleLending.ethPriceFeed();
        IPriceFeed tokenPriceFeed = simpleLending.tokenPriceFeed();

        uint256 ethPrice = ethPriceFeed.getPrice();
        uint256 tokenPrice = tokenPriceFeed.getPrice();

        // Rearrange calculation to avoid potential overflow
        uint256 collateralValue = (simpleLending.totalCollateral() * ethPrice) / 1e18;
        uint256 borrowedValue = (simpleLending.totalBorrowed() * tokenPrice) / 1e18;

        // Must maintain 75% collateral ratio
        require(
            collateralValue * simpleLending.COLLATERAL_RATIO() >= borrowedValue * 100,
            "Borrowed tokens exceed collateral value"
        );
    }

    // Prevent large sudden drops in total collateral
    function assertionEthDrain() external {
        uint256 MAX_WITHDRAWAL_PERCENT = 50; // 50%
        ph.forkPreState();

        uint256 preTotalCollateral = simpleLending.totalCollateral();

        ph.forkPostState();
        uint256 postTotalCollateral = simpleLending.totalCollateral();

        if (postTotalCollateral >= preTotalCollateral) {
            return;
        }

        uint256 withdrawalPercent = ((preTotalCollateral - postTotalCollateral) * 100) / preTotalCollateral;
        require(withdrawalPercent <= MAX_WITHDRAWAL_PERCENT, "Withdrawal percentage too high");
    }

    // Verify individual position maintains required collateral ratio
    function assertionIndividualPosition() external {
        ph.forkPostState();

        // Get the caller from call inputs
        PhEvm.CallInputs[] memory calls = ph.getCallInputs(address(simpleLending), simpleLending.withdraw.selector);
        if (calls.length == 0) {
            return;
        }

        for (uint256 i = 0; i < calls.length; i++) {
            address caller = calls[i].caller;

            // Get price feeds
            IPriceFeed ethPriceFeed = simpleLending.ethPriceFeed();
            IPriceFeed tokenPriceFeed = simpleLending.tokenPriceFeed();

            uint256 ethPrice = ethPriceFeed.getPrice();
            uint256 tokenPrice = tokenPriceFeed.getPrice();

            // Check the specific position that was modified
            (uint256 collateral, uint256 borrowed) = simpleLending.positions(caller);
            if (borrowed > 0) {
                // Only check positions with outstanding borrows
                uint256 collateralValue = (collateral * ethPrice) / 1e18;
                uint256 borrowedValue = (borrowed * tokenPrice) / 1e18;

                require(
                    collateralValue * simpleLending.COLLATERAL_RATIO() >= borrowedValue * 100,
                    "Individual position undercollateralized"
                );
            }
        }
    }
}
