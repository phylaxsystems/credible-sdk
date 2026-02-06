// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {TARGET} from "./Target.sol";

contract TestLoadTxObject is Assertion, Test {
    // Expected caller from test_utils.rs run_precompile_test
    address constant EXPECTED_CALLER = 0x5FDCCA53617f4d2b9134B29090C87D01058e27e9;

    constructor() payable {}

    function testGetTxObject() external view {
        PhEvm.TxObject memory txObj = ph.getTxObject();

        // Verify the transaction sender
        require(txObj.from == EXPECTED_CALLER, "txObj.from != expected caller");

        // For contract creation (TxKind::Create), to should be address(0)
        require(txObj.to == address(0), "txObj.to != address(0) for create tx");

        // Default value should be 0
        require(txObj.value == 0, "txObj.value != 0");

        // Input should contain the deployment bytecode (non-empty for contract creation)
        require(txObj.input.length > 0, "txObj.input should contain deployment bytecode");

        // Gas limit should be non-zero
        require(txObj.gas_limit > 0, "txObj.gas_limit should be > 0");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetTxObject.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(1);
    }
}
