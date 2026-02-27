// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {TARGET} from "./Target.sol";

/// @notice Assertion that calls getTxObject (Reshiram-only precompile).
/// Should fail when run under Legacy spec, succeed under Reshiram/Experimental.
contract TestSpecForbidTxObject is Assertion, Test {
    constructor() payable {}

    function testGetTxObjectForbidden() external view {
        // getTxObject is Reshiram-only; calling under Legacy should revert
        PhEvm.TxObject memory txObj = ph.getTxObject();
        require(txObj.from != address(0), "txObj.from should not be zero");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetTxObjectForbidden.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(1);
    }
}
