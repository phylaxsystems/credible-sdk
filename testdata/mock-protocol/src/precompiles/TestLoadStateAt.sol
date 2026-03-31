// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {TARGET} from "./Target.sol";

contract TestLoadStateAt is Assertion {
    constructor() payable {}

    function testLoadStateAt() external view {
        PhEvm.ForkId memory preTx = PhEvm.ForkId({forkType: 0, callIndex: 0});
        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});

        bytes32 preValue = ph.loadStateAt(bytes32(uint256(0)), preTx);
        bytes32 postValue = ph.loadStateAt(address(TARGET), bytes32(uint256(0)), postTx);
        bytes32 missingValue = ph.loadStateAt(address(TARGET), bytes32(uint256(999)), postTx);

        require(uint256(preValue) == 1, "pre-tx snapshot should see initial value");
        require(uint256(postValue) == 5, "post-tx snapshot should see updated value");
        require(uint256(missingValue) == 0, "missing slots should read as zero");
    }

    function triggers() external view override {
        registerCallTrigger(this.testLoadStateAt.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(5);
    }
}
