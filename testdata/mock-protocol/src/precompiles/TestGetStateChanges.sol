// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {Test} from "forge-std/Test.sol";
import {console} from "forge-std/console.sol";
import {TARGET} from "./Target.sol";

contract TestGetStateChanges is Assertion, Test {
    constructor() payable {}

    function testGetStateChanges() external view {
        bytes32[] memory changes = ph.getStateChanges(address(TARGET), bytes32(0));

        require(changes.length == 3, "changes.length != 3");

        require(uint256(changes[0]) == 1, "changes[0] != 1");
        require(uint256(changes[1]) == 5, "changes[1] != 5");
        require(uint256(changes[2]) == 15, "changes[2] != 15");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetStateChanges.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(5);

        // Test that state changes before reverts are not included.
        try TARGET.writeStorageAndRevert(10) {
            revert("Expected revert");
        } catch Error(string memory) {
            console.log("Caught revert as expected");
        }

        TARGET.writeStorage(15);
    }
}
