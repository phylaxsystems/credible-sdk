// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";

import {Target, TARGET} from "./Target.sol";

contract TestGetCallInputsStatic is Assertion, Test {
    constructor() payable {}

    function testGetCallInputs() external view {
        PhEvm.CallInputs[] memory callInputs = ph.getStaticCallInputs(address(TARGET), Target.readStorage.selector);
        require(callInputs.length == 2, "callInputs.length != 2");

        PhEvm.CallInputs memory callInput = callInputs[0];
        require(callInput.target_address == address(TARGET), "callInput.target_address != target");
        require(callInput.input.length == 0, "callInput.input.length != 0");
        require(callInput.id == 1, "callInput.id != 1");

        callInput = callInputs[1];
        require(callInput.target_address == address(TARGET), "callInput.target_address != target");
        require(callInput.input.length == 0, "callInput.input.length != 0");
        require(callInput.id == 3, "callInput.id != 3");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetCallInputs.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TriggeringCall callee = new TriggeringCall();
        callee.trigger();
    }
}

contract TriggeringCall {
    function trigger() external {
        TARGET.readStorage();
        TARGET.writeStorage(2);
        TARGET.readStorage();
    }
}
