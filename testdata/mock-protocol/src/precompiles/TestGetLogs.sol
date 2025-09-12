// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {Target, TARGET} from "./Target.sol";

contract TestGetLogs is Assertion, Test {
    constructor() payable {}

    function testGetLogs() external {
        require(TARGET.readStorage() == 1, "val != 1");
        PhEvm.Log[] memory logs = ph.getLogs();
        require(logs.length == 2, "logs.length != 2");

        PhEvm.Log memory log = logs[0];
        require(log.emitter == address(TARGET), "log.address != target");
        require(log.topics.length == 1, "log.topics.length != 1");
        require(log.topics[0] == Target.Log.selector, "log.topics[0] != Target.Log.selector");
        require(log.data.length == 32, "log.data.length != 32");
        require(bytes32(log.data) == bytes32(uint256(1)), "log.data != 1");

        log = logs[1];
        require(log.emitter == address(TARGET), "log.address != target");
        require(log.topics.length == 1, "log.topics.length != 1");
        require(log.topics[0] == Target.Log2.selector, "log.topics[0] != Target.Log2.selector");
        require(log.data.length == 32, "log.data.length != 32");
        require(bytes32(log.data) == bytes32(uint256(1)), "log.data != 1");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetLogs.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(1);
    }
}
