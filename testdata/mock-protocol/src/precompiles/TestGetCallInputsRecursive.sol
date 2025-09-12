// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";

import {Target, TARGET} from "./Target.sol";

contract TestGetCallInputsRecursive is Assertion, Test {
    function testGetCallInputsRecursive() external view {
        PhEvm.CallInputs[] memory callInputs = ph.getCallInputs(address(TARGET), Target.recursiveCalls.selector);
        require(callInputs.length == 3, "callInputs.length != 3");

        PhEvm.CallInputs memory callInput = callInputs[0];
        require(callInput.target_address == address(TARGET), "callInput.target_address != target");
        require(callInput.input.length == 0, "callInput.input.length != 0");
        require(callInput.value == 0, "callInput.value != 0");
        require(callInput.id == 0, "callInput.id != 0");

        callInput = callInputs[1];
        require(callInput.target_address == address(TARGET), "callInput.target_address != target");
        require(callInput.input.length == 0, "callInput.input.length != 0");
        require(callInput.value == 0, "callInput.value != 0");
        require(callInput.id == 1, "callInput.id != 1");

        callInput = callInputs[2];
        require(callInput.target_address == address(TARGET), "callInput.target_address != target");
        require(callInput.input.length == 0, "callInput.input.length != 0");
        require(callInput.value == 0, "callInput.value != 0");
        require(callInput.id == 2, "callInput.id != 2");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetCallInputsRecursive.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.recursiveCalls();
    }
}
