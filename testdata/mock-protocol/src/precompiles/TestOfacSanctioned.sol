// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {TARGET} from "./Target.sol";

contract TestOfacSanctioned is Assertion, Test {
    // Included in the fallback snapshot and in OFAC SDN digital asset sanctions.
    address constant SANCTIONED = 0x8576aCC5C05D6Ce88f4e49bf65BdF0C62F91353C;

    constructor() payable {}

    function testRevertIfSanctionedFails() external view {
        ph.revertIfSanctioned(SANCTIONED);
    }

    function triggers() external view override {
        registerCallTrigger(this.testRevertIfSanctionedFails.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(1);
    }
}
