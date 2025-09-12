// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";

import {Target, TARGET} from "./Target.sol";

contract TestGetCallInputsAllCalls is Assertion, Test {
    constructor() payable {}

    function testGetCallInputs() external view {
        PhEvm.CallInputs[] memory callInputs = ph.getAllCallInputs(address(TARGET), Target.writeStorage.selector);
        require(callInputs.length == 2, "callInputs.length != 2");

        PhEvm.CallInputs memory callInput = callInputs[0];
        require(callInput.target_address == address(TARGET), "callInput.target_address != target");
        require(callInput.input.length == 32, "callInput.input.length != 32");
        uint256 param = abi.decode(callInput.input, (uint256));
        require(param == 1, "First writeStorage param should be 1");
        require(callInput.value == 0, "callInput.value != 0");
        require(callInput.id == 0, "callInput.id != 0");

        callInput = callInputs[1];
        require(callInput.target_address == address(TARGET), "callInput.target_address != target");
        require(callInput.input.length == 32, "callInput.input.length != 32");
        require(callInput.id == 2, "callInput.id != 2");
        param = abi.decode(callInput.input, (uint256));
        require(param == 2, "Second writeStorage param should be 2");
        require(callInput.value == 0, "callInput.value != 0");

        callInputs = ph.getAllCallInputs(address(TARGET), Target.readStorage.selector);
        require(callInputs.length == 1, "callInputs.length != 1");

        callInput = callInputs[0];
        require(callInput.target_address == address(TARGET), "callInput.target_address != target");
        require(callInput.input.length == 0, "callInput.input.length != 0");
        require(callInput.id == 1, "callInput.id != 1");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetCallInputs.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(1);
        TARGET.readStorage();
        TARGET.writeStorage(2);
    }
}
