// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Target, TARGET} from "./Target.sol";

contract TestStaticCallAt is Assertion {
    uint64 internal constant CALL_GAS = 200_000;

    constructor() payable {}

    function testStaticCallAtTxSnapshots() external view {
        PhEvm.ForkId memory preTx = PhEvm.ForkId({forkType: 0, callIndex: 0});
        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});

        PhEvm.StaticCallResult memory preValue =
            ph.staticcallAt(address(TARGET), abi.encodeCall(Target.readStorage, ()), CALL_GAS, preTx);
        PhEvm.StaticCallResult memory postValue =
            ph.staticcallAt(address(TARGET), abi.encodeCall(Target.readStorage, ()), CALL_GAS, postTx);

        require(preValue.ok && postValue.ok, "snapshot staticcall failed");
        require(abi.decode(preValue.data, (uint256)) == 1, "pre-tx snapshot should see initial value");
        require(abi.decode(postValue.data, (uint256)) == 7, "post-tx snapshot should see updated value");
    }

    function testStaticCallAtCallSnapshots() external view {
        uint256 successfulNestedWriteId = _successfulNestedWriteId();

        PhEvm.ForkId memory preCall = PhEvm.ForkId({
            forkType: 2,
            callIndex: successfulNestedWriteId
        });
        PhEvm.ForkId memory postCall = PhEvm.ForkId({
            forkType: 3,
            callIndex: successfulNestedWriteId
        });

        PhEvm.StaticCallResult memory preValue =
            ph.staticcallAt(address(TARGET), abi.encodeCall(Target.readStorage, ()), CALL_GAS, preCall);
        PhEvm.StaticCallResult memory postValue =
            ph.staticcallAt(address(TARGET), abi.encodeCall(Target.readStorage, ()), CALL_GAS, postCall);

        require(preValue.ok && postValue.ok, "call snapshot staticcall failed");
        require(abi.decode(preValue.data, (uint256)) == 5, "pre-call snapshot should see pre-write value");
        require(abi.decode(postValue.data, (uint256)) == 7, "post-call snapshot should see written value");
    }

    function testStaticCallAtIgnoresAssertionWrites() external {
        TARGET.writeStorage(999);
        require(TARGET.readStorage() == 999, "assertion write should affect current execution state");

        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});
        PhEvm.StaticCallResult memory snapshotValue =
            ph.staticcallAt(address(TARGET), abi.encodeCall(Target.readStorage, ()), CALL_GAS, postTx);

        require(snapshotValue.ok, "post-tx snapshot staticcall failed");
        require(abi.decode(snapshotValue.data, (uint256)) == 7, "snapshot should ignore assertion-local writes");
    }

    function testStaticCallAtPreservesMsgSender() external view {
        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});
        PhEvm.StaticCallResult memory result = ph.staticcallAt(
            address(TARGET), abi.encodeCall(Target.readMsgSender, ()), CALL_GAS, postTx
        );

        require(result.ok, "msg.sender staticcall failed");
        require(abi.decode(result.data, (address)) == address(TARGET), "msg.sender should be adopter");
    }

    function testStaticCallAtEoaTarget() external view {
        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});
        PhEvm.StaticCallResult memory result =
            ph.staticcallAt(address(0xBEEF), bytes(""), CALL_GAS, postTx);

        require(result.ok, "EOA staticcall should succeed");
        require(result.data.length == 0, "EOA staticcall should return empty bytes");
    }

    function testStaticCallAtRevertData() external view {
        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});
        PhEvm.StaticCallResult memory result = ph.staticcallAt(
            address(TARGET), abi.encodeCall(Target.revertWithMessage, ()), CALL_GAS, postTx
        );

        require(!result.ok, "reverting staticcall should report failure");
        require(result.data.length >= 4, "reverting staticcall should return revert bytes");
        require(_selector(result.data) == bytes4(keccak256("Error(string)")), "unexpected revert selector");
    }

    function testStaticCallAtStaticWriteFails() external view {
        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});
        PhEvm.StaticCallResult memory result = ph.staticcallAt(
            address(TARGET), abi.encodeCall(Target.writeStorage, (123)), CALL_GAS, postTx
        );

        require(!result.ok, "state-changing staticcall should fail");
        require(result.data.length == 0, "state-changing staticcall should not return revert data");
    }

    function testStaticCallAtNestedOog() external view {
        PhEvm.ForkId memory postTx = PhEvm.ForkId({forkType: 1, callIndex: 0});
        PhEvm.StaticCallResult memory result = ph.staticcallAt(
            address(TARGET),
            abi.encodeCall(Target.consumeGas, (1_000_000)),
            20_000,
            postTx
        );

        require(!result.ok, "nested OOG should report failure");
        require(result.data.length == 0, "nested OOG should not return data");
    }

    function testStaticCallAtRejectsInvalidForkType() external view {
        PhEvm.ForkId memory invalidFork = PhEvm.ForkId({forkType: 9, callIndex: 0});
        bytes memory calldata_ = abi.encodeWithSignature(
            "staticcallAt(address,bytes,uint64,(uint8,uint256))",
            address(TARGET),
            abi.encodeCall(Target.readStorage, ()),
            CALL_GAS,
            invalidFork
        );

        (bool success,) = address(ph).staticcall(calldata_);
        require(!success, "invalid forkType should revert");
    }

    function testStaticCallAtRejectsRevertedCallSnapshot() external view {
        uint256 revertedCallId = _successfulNestedWriteId() + 1;
        PhEvm.ForkId memory revertedFork = PhEvm.ForkId({
            forkType: 3,
            callIndex: revertedCallId
        });
        bytes memory calldata_ = abi.encodeWithSignature(
            "staticcallAt(address,bytes,uint64,(uint8,uint256))",
            address(TARGET),
            abi.encodeCall(Target.readStorage, ()),
            CALL_GAS,
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

    function _selector(bytes memory data) internal pure returns (bytes4 selector) {
        if (data.length < 4) {
            return bytes4(0);
        }

        assembly {
            selector := mload(add(data, 32))
        }
    }

    function triggers() external view override {
        registerCallTrigger(this.testStaticCallAtTxSnapshots.selector);
        registerCallTrigger(this.testStaticCallAtCallSnapshots.selector);
        registerCallTrigger(this.testStaticCallAtIgnoresAssertionWrites.selector);
        registerCallTrigger(this.testStaticCallAtPreservesMsgSender.selector);
        registerCallTrigger(this.testStaticCallAtEoaTarget.selector);
        registerCallTrigger(this.testStaticCallAtRevertData.selector);
        registerCallTrigger(this.testStaticCallAtStaticWriteFails.selector);
        registerCallTrigger(this.testStaticCallAtNestedOog.selector);
        registerCallTrigger(this.testStaticCallAtRejectsInvalidForkType.selector);
        registerCallTrigger(this.testStaticCallAtRejectsRevertedCallSnapshot.selector);
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
