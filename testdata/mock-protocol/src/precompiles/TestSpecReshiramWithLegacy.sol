// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {TARGET} from "./Target.sol";

/// @notice Assertion that uses both Legacy and Reshiram-tier precompiles.
/// Should pass under Reshiram or Experimental spec.
contract TestSpecReshiramWithLegacy is Assertion, Test {
    constructor() payable {}

    function testReshiramAndLegacy() external {
        // Legacy precompiles
        address adopter = ph.getAssertionAdopter();
        require(adopter != address(0), "adopter should not be zero");

        ph.forkPostTx();
        PhEvm.Log[] memory logs = ph.getLogs();

        // Reshiram precompile
        PhEvm.TxObject memory txObj = ph.getTxObject();
        require(txObj.from != address(0), "txObj.from should not be zero");

        ph.forkPreTx();
    }

    function triggers() external view override {
        registerCallTrigger(this.testReshiramAndLegacy.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(1);
    }
}
