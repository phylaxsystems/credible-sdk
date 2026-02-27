// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {TARGET} from "./Target.sol";

/// @notice Assertion that uses only Legacy-tier precompiles.
/// Should pass when run under Legacy, Reshiram, or Experimental spec.
contract TestSpecLegacy is Assertion, Test {
    constructor() payable {}

    function testLegacyPrecompiles() external {
        // All of these are Legacy-tier and should work under any spec
        address adopter = ph.getAssertionAdopter();
        require(adopter != address(0), "adopter should not be zero");

        ph.forkPostTx();
        PhEvm.Log[] memory logs = ph.getLogs();
        ph.forkPreTx();
    }

    function triggers() external view override {
        registerCallTrigger(this.testLegacyPrecompiles.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(1);
    }
}
