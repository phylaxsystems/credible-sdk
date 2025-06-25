// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";

contract InfiniteDeploymentAssertion is Assertion {
    constructor() {
        while (true) {}
    }

    function assertCount() public {}

    function triggers() external view override {
        registerCallTrigger(this.assertCount.selector);
    }
}
