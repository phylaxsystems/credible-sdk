// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {Test} from "forge-std/Test.sol";

import {Target, TARGET} from "./Target.sol";

contract TestFork is Assertion, Test {
    uint256 expectedSum = 0;
    uint256 someInitValue = 1;

    constructor() payable {}

    function testForkSwitch() external {
        //Test fork switching reads from underlying state
        require(TARGET.readStorage() == 2, "readStorage() != 2");

        ph.forkPreState();
        return;
        require(TARGET.readStorage() == 1, "readStorage() != 1");

        ph.forkPostState();
        require(TARGET.readStorage() == 2, "readStorage() != 2");

        ph.forkPreState();
        require(TARGET.readStorage() == 1, "readStorage() != 1");
    }

    function testPersistTargetContracts() external {
        require(someInitValue == 1, "someInitValue != 1");
        uint256 sum = 0;

        require(TARGET.readStorage() == 2, "val != 2");
        expectedSum += TARGET.readStorage();
        sum += TARGET.readStorage();

        ph.forkPreState();
        require(TARGET.readStorage() == 1, "readStorage != 1");
        expectedSum += TARGET.readStorage();
        sum += TARGET.readStorage();

        ph.forkPostState();
        require(TARGET.readStorage() == 2, "val != 2");
        expectedSum += TARGET.readStorage();
        sum += TARGET.readStorage();

        ph.forkPreState();
        require(TARGET.readStorage() == 1, "val != 1");
        expectedSum += TARGET.readStorage();
        sum += TARGET.readStorage();

        require(sum == expectedSum, "sum != expectedSum");
        require(sum == 6, "sum != 6");
    }

    function triggers() external view override {
        registerCallTrigger(this.testForkSwitch.selector);
        registerCallTrigger(this.testPersistTargetContracts.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(2);
    }
}
