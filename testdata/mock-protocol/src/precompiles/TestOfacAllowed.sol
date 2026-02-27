// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {TARGET} from "./Target.sol";

contract TestOfacAllowed is Assertion, Test {
    constructor() payable {}

    function testRevertIfSanctionedAllowsAddress() external view {
        ph.revertIfSanctioned(address(0x1111111111111111111111111111111111111111));
    }

    function triggers() external view override {
        registerCallTrigger(this.testRevertIfSanctionedAllowsAddress.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(1);
    }
}
