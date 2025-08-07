// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {console} from "forge-std/console.sol";

import {Target, TARGET, ProxyImpl} from "./Target.sol";

contract TestGetCallInputsProxy is Assertion, Test {
    constructor() payable {}

    function testGetCallInputs() external view {
        PhEvm.CallInputs[] memory callInputs = ph.getCallInputs(
            address(TARGET),
            ProxyImpl.readStorageProxy.selector
        );
        require(callInputs.length == 2, "callInputs.length != 2");
        PhEvm.CallInputs memory callInput = callInputs[0];

        require(
            callInput.target_address == address(TARGET),
            "callInput.target_address != target"
        );

        require(
            callInput.bytecode_address != address(TARGET),
            "callInput.bytecode_address != target"
        );

        require(callInput.input.length == 0, "callInput.input.length != 0");
        require(callInput.value == 0, "callInput.value != 0");
        require(callInput.id == 3, "callInput.id != 3");

        callInputs = ph.getCallInputs(
            address(TARGET),
            ProxyImpl.writeStorageProxy.selector
        );
        require(callInputs.length == 2, "callInputs.length != 2");

        callInput = callInputs[0];
        require(
            callInput.target_address == address(TARGET),
            "callInput.target_address != target"
        );

        require(
            callInput.bytecode_address != address(TARGET),
            "callInput.bytecode_address != target"
        );

        require(callInput.input.length == 32, "callInput.input.length != 32");
        uint256 param = abi.decode(callInput.input, (uint256));
        require(param == 1, "First writeStorage param should be 1");
        require(callInput.value == 0, "callInput.value != 0");
        require(callInput.id == 1, "callInput.id != 1");

        callInput = callInputs[1];
        require(
            callInput.target_address == address(TARGET),
            "callInput.target_address != target"
        );
        require(callInput.input.length == 32, "callInput.input.length != 32");
        require(callInput.id == 2, "callInput.id != 2");
        param = abi.decode(callInput.input, (uint256));
        require(param == 2, "Second writeStorage param should be 2");
        require(callInput.value == 0, "callInput.value != 0");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetCallInputs.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        // Create proxy contract
        TARGET.initProxy();

        // Make calls through proxy
        ProxyImpl proxy = ProxyImpl(address(TARGET));
        require(proxy.readStorageProxy() == 1, "init value not 1");
        proxy.writeStorageProxy(1);
        proxy.writeStorageProxy(2);
        proxy.readStorageProxy();
    }
}