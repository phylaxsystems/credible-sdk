// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import {Test} from "../../lib/credible-std/lib/forge-std/src/Test.sol";
import {Credible} from "../../lib/credible-std/src/Credible.sol";
import {MockAssertion} from "../src/MockAssertion.sol";
import {MockProtocol} from "../../src/protocol.sol";

contract TestMockAssertion is Test, Credible {
    MockProtocol public protocol;

    function setUp() public {
        protocol = new MockProtocol();
    }

    function test_assertionCheckBool() public {
        MockAssertion assertion = new MockAssertion(protocol);
        assertEq(assertion.assertionCheckBool(), true);
    }
}
