// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";

import {Target, TARGET, Reverts} from "./Target.sol";

contract TestGetCallInputsReverts is Assertion, Test {
    function testGetCallInputsReverts() external view {
        PhEvm.CallInputs[] memory callInputs = ph.getCallInputs(
            address(TARGET),
            Target.unhandledRevert.selector
        );
        require(callInputs.length == 1, "callInputs.length != 1");

        PhEvm.CallInputs memory callInput = callInputs[0];
        require(
            callInput.target_address == address(TARGET),
            "callInput.target_address != target"
        );
        require(callInput.input.length == 0, "callInput.input.length != 0");
        require(callInput.value == 0, "callInput.value != 0");
        require(callInput.id == 0, "callInput.id != 0");

        callInputs = ph.getCallInputs(
            address(TARGET.reverts()),
            ""
        );
        require(callInputs.length == 1, "callInputs.length != 1");

        callInput = callInputs[0];
        require(
            callInput.target_address == address(TARGET.reverts()),
            "callInput.target_address != target"
        );

        require(callInput.input.length == 0, "callInput.input.length != 0");
        require(callInput.value == 0, "callInput.value != 0");
        require(callInput.id == 1, "callInput.id != 1");

    }

    function triggers() external view override {
        registerCallTrigger(this.testGetCallInputsReverts.selector);
    }
}
contract TriggeringTx {
    constructor() payable {
        TARGET.unhandledRevert();
    }
}
