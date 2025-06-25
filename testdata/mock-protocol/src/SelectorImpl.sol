// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";

contract SelectorImpl is Assertion {
    event RunningAssertion(uint256 count);

    function assertCount() public pure {
        uint256 count = 0;

        if (count > 1) {
            revert("Counter cannot be greater than 1");
        }
    }

    function triggers() external view override {
        registerCallTrigger(this.assertionStorage.selector);
        registerCallTrigger(this.assertionEther.selector);
        registerCallTrigger(this.assertionBoth.selector);
    }

    function assertionStorage() external pure returns (bool) {
        return true;
    }

    function assertionEther() external pure returns (bool) {
        return true;
    }

    function assertionBoth() external pure returns (bool) {
        return true;
    }
}
