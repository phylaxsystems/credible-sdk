// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Test} from "forge-std/Test.sol";
import {TARGET} from "./Target.sol";

contract TestGetMappingStateChanges is Assertion, Test {
    /// @dev Target.mappingData is a mapping(uint256 => uint256) at storage slot 4
    bytes32 constant DATA_SLOT = bytes32(uint256(3));

    constructor() payable {}

    function testGetMappingStateChanges() external view {
        // Use the new precompile to get state changes for mappingData[42]
        // key must be left-padded to bytes32 for value-type keys
        bytes32 key = bytes32(uint256(42));

        uint256[] memory changes = getMappingStateChangesUint(
            address(TARGET),
            DATA_SLOT,
            key
        );

        // get_differences returns: [had_value_0, had_value_1, ..., present_value]
        // mappingData[42]: 0 -> 100 -> 200
        require(changes.length == 3, "changes.length != 3");
        require(changes[0] == 0, "changes[0] != 0");
        require(changes[1] == 100, "changes[1] != 100");
        require(changes[2] == 200, "changes[2] != 200");
    }

    function testGetMappingStateChangesMultipleKeys() external view {
        bytes32 key1 = bytes32(uint256(42));
        bytes32 key2 = bytes32(uint256(99));

        uint256[] memory changes1 = getMappingStateChangesUint(
            address(TARGET),
            DATA_SLOT,
            key1
        );

        uint256[] memory changes2 = getMappingStateChangesUint(
            address(TARGET),
            DATA_SLOT,
            key2
        );

        // mappingData[42]: 0 -> 100 -> 200
        require(changes1.length == 3, "key42: changes.length != 3");
        require(changes1[0] == 0, "key42: changes[0] != 0");
        require(changes1[1] == 100, "key42: changes[1] != 100");
        require(changes1[2] == 200, "key42: changes[2] != 200");

        // mappingData[99]: 0 -> 999
        require(changes2.length == 2, "key99: changes.length != 2");
        require(changes2[0] == 0, "key99: changes[0] != 0");
        require(changes2[1] == 999, "key99: changes[1] != 999");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetMappingStateChanges.selector);
        registerCallTrigger(this.testGetMappingStateChangesMultipleKeys.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        // Write to mappingData[42] twice and mappingData[99] once
        TARGET.writeMapping(42, 100);
        TARGET.writeMapping(42, 200);
        TARGET.writeMapping(99, 999);
    }
}
