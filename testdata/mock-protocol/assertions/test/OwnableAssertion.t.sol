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
        string memory label = "Ownership has changed";

        // Associate the assertion with the protocol
        // cl will manage the correct assertion execution under the hood when the protocol is being called
        cl.addAssertion(label, aaAddress, type(OwnableAssertion).creationCode, abi.encode(assertionAdopter));

        vm.prank(initialOwner);
        vm.expectRevert("Assertions Reverted");
        cl.validate(
            label, aaAddress, 0, abi.encodePacked(assertionAdopter.transferOwnership.selector, abi.encode(newOwner))
        );
    }

    function test_assertionOwnershipNotChanged() public {
        string memory label = "Ownership has not changed";
        address aaAddress = address(assertionAdopter);

        cl.addAssertion(label, aaAddress, type(OwnableAssertion).creationCode, abi.encode(assertionAdopter));

        vm.prank(initialOwner);
        cl.validate(
            label, aaAddress, 0, abi.encodePacked(assertionAdopter.transferOwnership.selector, abi.encode(initialOwner))
        ); // assert that the ownership has not changed
    }
}
