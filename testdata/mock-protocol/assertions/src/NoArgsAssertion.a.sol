// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";

contract NoArgsAssertion is Assertion {
    function triggers() external view virtual override {
        registerCallTrigger(this.assertionCheckBool.selector);
    }

    function assertionCheckBool() external view returns (bool) {
        return true;
    }
}
