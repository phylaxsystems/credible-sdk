// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Target, TARGET} from "./Target.sol";

contract TestCallFrameForking is Assertion {
    constructor() payable {}

    function callFrameForkingWriteStorage() external {
        PhEvm.CallInputs[] memory callInputs = ph.getCallInputs(address(TARGET), Target.writeStorage.selector);
        for (uint256 i = 0; i < callInputs.length; i++) {
            PhEvm.CallInputs memory callInput = callInputs[i];
            uint256 param = abi.decode(callInput.input, (uint256));
            ph.forkPostCall(callInput.id);

            uint256 value = TARGET.readStorage();
            require(param == value, "writeStorage param should be equal to value after call frame");
        }
    }

    function callFrameForkingIncrementStorage() external {
        PhEvm.CallInputs[] memory callInputs = ph.getCallInputs(address(TARGET), Target.incrementStorage.selector);
        for (uint256 i = 0; i < callInputs.length; i++) {
            PhEvm.CallInputs memory callInput = callInputs[i];

            ph.forkPreCall(callInput.id);
            uint256 preValue = TARGET.readStorage();

            ph.forkPostCall(callInput.id);
            uint256 postValue = TARGET.readStorage();

            require(preValue + 1 == postValue, "incrementStorage should increment value");
        }
    }

    function triggers() external view override {
        registerCallTrigger(this.callFrameForkingWriteStorage.selector);
        registerCallTrigger(this.callFrameForkingIncrementStorage.selector);
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
