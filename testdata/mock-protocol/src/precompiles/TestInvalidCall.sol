// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {console} from "forge-std/console.sol";
import {Target, TARGET} from "./Target.sol";

contract TestInvalidCall is Assertion, Test {
    constructor() payable {}

    function testGetStateChanges() external {
        BadInterface(address(ph)).doesNotExist();
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetStateChanges.selector);
    }
}

interface BadInterface {
    function doesNotExist() external;
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(5);
    }
}
