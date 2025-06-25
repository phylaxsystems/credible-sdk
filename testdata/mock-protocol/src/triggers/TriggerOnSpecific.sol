// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "credible-std/Assertion.sol";

contract TriggerOnSpecific is Assertion {
    function triggers() external view override {
        registerCallTrigger(0xDEADBEEF, bytes4(keccak256("triggerCall()")));
        registerStorageChangeTrigger(0xDEADBEEF, bytes32(keccak256("triggerStorage")));
    }
}
