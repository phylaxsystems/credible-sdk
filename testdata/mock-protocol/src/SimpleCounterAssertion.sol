// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";

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
