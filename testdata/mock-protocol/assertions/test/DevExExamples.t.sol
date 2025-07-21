// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {CredibleTest} from "credible-std/CredibleTest.sol";
import {MockContract} from "../../src/DevExExamples.sol";
import {DevExExamplesAssertion} from "../src/DevExExamples.sol";
import {Test} from "forge-std/Test.sol";

contract DevExExamplesAssertionTest is CredibleTest, Test {
    MockContract adopter = new MockContract();
    function test_validate() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(DevExExamplesAssertion).creationCode,
            fnSelector: DevExExamplesAssertion.assertion.selector
        });
        adopter.setInvalidate(false);
    }

    function test_validate_state_persist() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(DevExExamplesAssertion).creationCode,
            fnSelector: DevExExamplesAssertion.assertion.selector
        });
        adopter.setState();
        require(adopter.state(), "State not persisted");
    }

    function test_invalidate() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(DevExExamplesAssertion).creationCode,
            fnSelector: DevExExamplesAssertion.assertion.selector
        });
        vm.expectRevert("Some invalidation reason");
        adopter.setInvalidate(true);
        require(!adopter.invalidate(), "State should not have persisted");

    }

    function test_no_assertions_executed() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(DevExExamplesAssertion).creationCode,
            fnSelector: DevExExamplesAssertion.assertion.selector
        });
        vm.expectRevert(
            "Expected 1 assertion to be executed, but 0 were executed."
        );
        payable(address(0xdead)).transfer(1);
        require(address(0xdead).balance == 1, "Balance not persisted");
    }
    function test_constructor_trigger_valid() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(DevExExamplesAssertion).creationCode,
            fnSelector: DevExExamplesAssertion.assertion.selector
        });
        new Trigger(adopter, false);
    }

    function test_constructor_trigger_invalid() public {
        cl.assertion({
            adopter: address(adopter),
            createData: type(DevExExamplesAssertion).creationCode,
            fnSelector: DevExExamplesAssertion.assertion.selector
        });
        vm.expectRevert("Some invalidation reason");
        new Trigger(adopter, true);
    }
}

contract Trigger {
    constructor(MockContract adopter, bool invalidate) {
        adopter.setInvalidate(invalidate);
    }
}