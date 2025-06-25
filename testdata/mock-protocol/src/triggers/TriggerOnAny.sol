// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "credible-std/Assertion.sol";

contract TriggerOnAny is Assertion {
    function triggers() external view override {
        registerCallTrigger(0xDEADBEEF);
        registerStorageChangeTrigger(0xDEADBEEF);
        registerBalanceChangeTrigger(0xDEADBEEF);
    }
}
