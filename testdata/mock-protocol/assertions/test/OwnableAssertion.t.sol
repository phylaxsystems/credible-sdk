// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import {Credible} from "../../lib/credible-std/src/Credible.sol";
import {OwnableAssertion} from "../src/OwnableAssertion.sol";
import {Ownable} from "../../src/Ownable.sol";
import {console} from "../../lib/credible-std/lib/forge-std/src/console.sol";
import {CredibleTest} from "../../lib/credible-std/src/CredibleTest.sol";
import {Test} from "../../lib/credible-std/lib/forge-std/src/Test.sol";

contract TestOwnableAssertion is CredibleTest, Test {
    // Contract state variables
    Ownable public assertionAdopter;
    address public initialOwner = address(0xdead);
    address public newOwner = address(0xdeadbeef);

    function setUp() public {
        assertionAdopter = new Ownable();
        vm.deal(initialOwner, 1 ether);
    }

    function test_assertionOwnershipChanged() public {
        address aaAddress = address(assertionAdopter);
        vm.prank(initialOwner);
        cl.assertion(
            aaAddress,
            type(OwnableAssertion).creationCode,
            OwnableAssertion.assertionOwnershipChange.selector
        );
        vm.expectRevert("Ownership has changed");
        assertionAdopter.transferOwnership(newOwner);
    }

    function test_assertionOwnershipNotChanged() public {
        address aaAddress = address(assertionAdopter);

        vm.prank(initialOwner);
        cl.assertion(
            aaAddress,
            type(OwnableAssertion).creationCode,
            OwnableAssertion.assertionOwnershipChange.selector
        ); // assert that the ownership has not changed
        assertionAdopter.transferOwnership(initialOwner);
    }
}
