// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Test} from "forge-std/Test.sol";
import {CredibleTest} from "credible-std/CredibleTest.sol";
import {
    LegacySpecAssertion,
    ReshiramSpecAssertion,
    ExperimentalSpecAssertion,
    NoSpecAssertion
} from "../src/SpecAssertions.a.sol";
import {MockContract} from "../../src/DevExExamples.sol";

contract TestSpecAssertions is CredibleTest, Test {
    MockContract public adopter;

    function setUp() public {
        adopter = new MockContract();
    }

    // ---------------------------------------------------------------
    // Legacy spec tests
    // ---------------------------------------------------------------

    /// @notice Legacy spec assertion can use Legacy-tier precompiles (forkPreTx, forkPostTx, getLogs, etc.)
    function test_legacySpec_legacyPrecompiles() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(LegacySpecAssertion).creationCode,
            fnSelector: LegacySpecAssertion.assertionLegacyPrecompiles.selector
        });
        adopter.setInvalidate(false);
    }

    /// @notice Legacy spec assertion is forbidden from calling getTxObject (Reshiram-only)
    function test_legacySpec_getTxObject_reverts() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(LegacySpecAssertion).creationCode,
            fnSelector: LegacySpecAssertion.assertionGetTxObjectForbidden.selector
        });
        vm.expectRevert();
        adopter.setInvalidate(false);
    }

    // ---------------------------------------------------------------
    // Reshiram spec tests
    // ---------------------------------------------------------------

    /// @notice Reshiram spec assertion can use getTxObject
    function test_reshiramSpec_getTxObject() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(ReshiramSpecAssertion).creationCode,
            fnSelector: ReshiramSpecAssertion.assertionReshiramPrecompiles.selector
        });
        adopter.setInvalidate(false);
    }

    /// @notice Reshiram spec assertion can still use Legacy-tier precompiles
    function test_reshiramSpec_legacyPrecompiles() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(ReshiramSpecAssertion).creationCode,
            fnSelector: ReshiramSpecAssertion.assertionLegacyStillWorks.selector
        });
        adopter.setInvalidate(false);
    }

    // ---------------------------------------------------------------
    // Experimental spec tests
    // ---------------------------------------------------------------

    /// @notice Experimental spec assertion can use all precompiles
    function test_experimentalSpec_allPrecompiles() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(ExperimentalSpecAssertion).creationCode,
            fnSelector: ExperimentalSpecAssertion.assertionExperimentalPrecompiles.selector
        });
        adopter.setInvalidate(false);
    }

    // ---------------------------------------------------------------
    // No spec (default) tests
    // ---------------------------------------------------------------

    /// @notice Assertion without explicit spec defaults to Legacy and can use Legacy precompiles
    function test_noSpec_defaultsToLegacy() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(NoSpecAssertion).creationCode,
            fnSelector: NoSpecAssertion.assertionDefaultSpec.selector
        });
        adopter.setInvalidate(false);
    }

    /// @notice Assertion without explicit spec is forbidden from calling getTxObject
    function test_noSpec_getTxObject_reverts() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(NoSpecAssertion).creationCode,
            fnSelector: NoSpecAssertion.assertionGetTxObjectForbidden.selector
        });
        vm.expectRevert();
        adopter.setInvalidate(false);
    }
}
