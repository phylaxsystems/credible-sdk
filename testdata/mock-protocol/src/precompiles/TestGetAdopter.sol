// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {console} from "forge-std/console.sol";
import {Target, TARGET} from "./Target.sol";

contract TestGetAdopter is Assertion, Test {
    constructor() payable {}

    function testGetAdopter() external view {
        require(TARGET.readStorage() == 1, "val != 1");
        address adopter = ph.getAssertionAdopter();
        require(adopter == address(TARGET), "adopter != target");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetAdopter.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(1);
    }
}
