// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Target, TARGET} from "./Target.sol";

contract TestCallFrameForkingRevert is Assertion {
    constructor() payable {}

    function callFrameForkingRevertAfterFork() external {
        PhEvm.CallInputs[] memory callInputs = ph.getCallInputs(address(TARGET), Target.incrementStorage.selector);
        for (uint256 i = 0; i < callInputs.length; i++) {
            PhEvm.CallInputs memory callInput = callInputs[i];
            ph.forkPreCall(callInput.id);
            require(false, "revert after fork");
        }
    }

    function triggers() external view override {
        registerCallTrigger(this.callFrameForkingRevertAfterFork.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.incrementStorage();
        TARGET.incrementStorage();
        TARGET.writeStorage(4);
        TARGET.incrementStorage();
    }
}
