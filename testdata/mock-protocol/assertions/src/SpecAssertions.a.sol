// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {AssertionSpec} from "credible-std/SpecRecorder.sol";
import {PhEvm} from "credible-std/PhEvm.sol";

/// @notice Assertion that registers the Legacy spec in its constructor.
contract LegacySpecAssertion is Assertion {
    constructor() {
        registerAssertionSpec(AssertionSpec.Legacy);
    }

    function triggers() external view override {
        registerCallTrigger(this.assertionLegacyPrecompiles.selector);
        registerCallTrigger(this.assertionGetTxObjectForbidden.selector);
    }

    /// @notice Uses only Legacy-tier precompiles (forkPreTx, forkPostTx, getLogs, etc.)
    function assertionLegacyPrecompiles() external {
        address adopter = ph.getAssertionAdopter();
        ph.forkPostTx();
        // Legacy precompiles should work fine
        PhEvm.Log[] memory logs = ph.getLogs();
        ph.forkPreTx();
    }

    /// @notice Attempts to call getTxObject which is Reshiram-only. Should be forbidden.
    function assertionGetTxObjectForbidden() external {
        ph.getTxObject();
    }
}

/// @notice Assertion that registers the Reshiram spec in its constructor.
contract ReshiramSpecAssertion is Assertion {
    constructor() {
        registerAssertionSpec(AssertionSpec.Reshiram);
    }

    function triggers() external view override {
        registerCallTrigger(this.assertionReshiramPrecompiles.selector);
        registerCallTrigger(this.assertionLegacyStillWorks.selector);
    }

    /// @notice Uses Reshiram-tier precompile (getTxObject).
    function assertionReshiramPrecompiles() external {
        PhEvm.TxObject memory txObj = ph.getTxObject();
        require(txObj.from != address(0), "TxObject from should not be zero");
    }

    /// @notice Verifies Legacy precompiles still work under Reshiram spec.
    function assertionLegacyStillWorks() external {
        address adopter = ph.getAssertionAdopter();
        ph.forkPostTx();
        PhEvm.Log[] memory logs = ph.getLogs();
        ph.forkPreTx();
    }
}

/// @notice Assertion that registers the Experimental spec in its constructor.
contract ExperimentalSpecAssertion is Assertion {
    constructor() {
        registerAssertionSpec(AssertionSpec.Experimental);
    }

    function triggers() external view override {
        registerCallTrigger(this.assertionExperimentalPrecompiles.selector);
    }

    /// @notice Uses both Legacy and Reshiram precompiles under Experimental spec.
    function assertionExperimentalPrecompiles() external {
        // Legacy precompiles
        address adopter = ph.getAssertionAdopter();
        ph.forkPostTx();
        PhEvm.Log[] memory logs = ph.getLogs();

        // Reshiram precompiles
        PhEvm.TxObject memory txObj = ph.getTxObject();
        require(txObj.from != address(0), "TxObject from should not be zero");

        ph.forkPreTx();
    }
}

/// @notice Assertion that does NOT register any spec (defaults to Legacy).
contract NoSpecAssertion is Assertion {
    function triggers() external view override {
        registerCallTrigger(this.assertionDefaultSpec.selector);
        registerCallTrigger(this.assertionGetTxObjectForbidden.selector);
    }

    /// @notice Uses only Legacy precompiles - should work with default spec.
    function assertionDefaultSpec() external {
        address adopter = ph.getAssertionAdopter();
        ph.forkPostTx();
        PhEvm.Log[] memory logs = ph.getLogs();
        ph.forkPreTx();
    }

    /// @notice Attempts getTxObject without registering Reshiram spec. Should be forbidden.
    function assertionGetTxObjectForbidden() external {
        ph.getTxObject();
    }
}
