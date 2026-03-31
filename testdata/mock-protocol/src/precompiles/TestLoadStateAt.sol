// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Target, TARGET} from "./Target.sol";

contract TestLoadStateAt is Assertion {
    constructor() payable {}

    function testLoadStateAtTxSnapshots() external view {
        PhEvm.ForkId memory preTx = PhEvm.ForkId({forkType: 0, callIndex: 0});
        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});

        bytes32 preValue = ph.loadStateAt(bytes32(uint256(0)), preTx);
        bytes32 postValue = ph.loadStateAt(address(TARGET), bytes32(uint256(0)), postTx);
        bytes32 missingValue = ph.loadStateAt(address(TARGET), bytes32(uint256(999)), postTx);
        bytes32 untouchedAccountValue =
            ph.loadStateAt(address(0xBEEF), bytes32(uint256(0)), postTx);

        require(uint256(preValue) == 1, "pre-tx snapshot should see initial value");
        require(uint256(postValue) == 7, "post-tx snapshot should see updated value");
        require(uint256(missingValue) == 0, "missing slots should read as zero");
        require(uint256(untouchedAccountValue) == 0, "untouched accounts should read as zero");
    }

    function testLoadStateAtCallSnapshots() external view {
        uint256 successfulNestedWriteId = _successfulNestedWriteId();

        PhEvm.ForkId memory preCall = PhEvm.ForkId({
            forkType: 2,
            callIndex: successfulNestedWriteId
        });
        PhEvm.ForkId memory postCall = PhEvm.ForkId({
            forkType: 3,
            callIndex: successfulNestedWriteId
        });

        bytes32 preValue = ph.loadStateAt(address(TARGET), bytes32(uint256(0)), preCall);
        bytes32 postValue = ph.loadStateAt(address(TARGET), bytes32(uint256(0)), postCall);

        require(uint256(preValue) == 5, "pre-call snapshot should see pre-write value");
        require(uint256(postValue) == 7, "post-call snapshot should see written value");
    }

    function testLoadStateAtIgnoresAssertionWrites() external {
        TARGET.writeStorage(999);
        require(TARGET.readStorage() == 999, "assertion write should affect current execution state");

        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});
        bytes32 snapshotValue = ph.loadStateAt(address(TARGET), bytes32(uint256(0)), postTx);

        require(
            uint256(snapshotValue) == 7,
            "snapshot reads should ignore assertion-local writes"
        );
    }

    function testLoadStateAtRejectsInvalidForkType() external view {
        PhEvm.ForkId memory invalidFork = PhEvm.ForkId({forkType: 9, callIndex: 0});
        bytes memory calldata_ = abi.encodeWithSignature(
            "loadStateAt(address,bytes32,(uint8,uint256))",
            address(TARGET),
            bytes32(uint256(0)),
            invalidFork
        );

        (bool success,) = address(ph).staticcall(calldata_);
        require(!success, "invalid forkType should revert");
    }

    function testLoadStateAtRejectsRevertedCallSnapshot() external view {
        uint256 revertedCallId = _successfulNestedWriteId() + 1;
        PhEvm.ForkId memory revertedFork = PhEvm.ForkId({
            forkType: 3,
            callIndex: revertedCallId
        });
        bytes memory calldata_ = abi.encodeWithSignature(
            "loadStateAt(address,bytes32,(uint8,uint256))",
            address(TARGET),
            bytes32(uint256(0)),
            revertedFork
        );

        (bool success,) = address(ph).staticcall(calldata_);
        require(!success, "reverted subtree snapshot should revert");
    }

    function _successfulNestedWriteId() internal view returns (uint256 successfulNestedWriteId) {
        PhEvm.CallInputs[] memory callInputs =
            ph.getCallInputs(address(TARGET), Target.writeStorage.selector);

        bool found;
        for (uint256 i = 0; i < callInputs.length; i++) {
            uint256 param = abi.decode(callInputs[i].input, (uint256));
            if (param == 7) {
                successfulNestedWriteId = callInputs[i].id;
                found = true;
                break;
            }
        }

        require(found, "expected nested writeStorage call");
    }

    function triggers() external view override {
        registerCallTrigger(this.testLoadStateAtTxSnapshots.selector);
        registerCallTrigger(this.testLoadStateAtCallSnapshots.selector);
        registerCallTrigger(this.testLoadStateAtIgnoresAssertionWrites.selector);
        registerCallTrigger(this.testLoadStateAtRejectsInvalidForkType.selector);
        registerCallTrigger(this.testLoadStateAtRejectsRevertedCallSnapshot.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        TARGET.writeStorage(5);
        CallFrameTrigger callFrameTrigger = new CallFrameTrigger();
        callFrameTrigger.trigger();
    }
}

contract CallFrameTrigger {
    function trigger() external {
        TARGET.writeStorage(7);

        try TARGET.writeStorageAndRevert(9) {
            revert("expected writeStorageAndRevert to revert");
        } catch {}
    }
}
