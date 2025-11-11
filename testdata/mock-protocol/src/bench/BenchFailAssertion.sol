// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";

contract BenchTrigger {
    event Triggered();

    function noop() external {}

    function trigger() external {
        emit Triggered();
    }
}

contract BenchFailAssertion is Assertion {
    bool public shouldFail;

    function setFail(bool value) external {
        shouldFail = value;
    }

    function assertNoFail() external view {
        if (shouldFail) {
            revert("bench fail");
        }
    }

    function triggers() external view override {
        registerCallTrigger(BenchTrigger.trigger.selector);
    }
}
