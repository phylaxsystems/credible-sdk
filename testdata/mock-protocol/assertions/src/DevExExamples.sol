// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {MockContract} from "../../src/DevExExamples.sol";

contract DevExExamplesAssertion is Assertion {
    event log(string);
    function assertion() external {
        MockContract adopter = MockContract(ph.getAssertionAdopter());
        
        string memory example = "Example log";
        emit log(example);
        if (adopter.invalidate()) {
            revert("Some invalidation reason");
        }
    }

    function triggers() external view override {
        registerCallTrigger(this.assertion.selector);
    }
}
