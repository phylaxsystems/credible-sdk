// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {Target, TARGET} from "./Target.sol";

contract TestGetLogsQuery is Assertion {
    constructor() payable {}

    function testGetLogsQueryPostTxAllLogs() external view {
        PhEvm.Log[] memory logs = ph.getLogsQuery(_query(address(0), bytes32(0)), _postTxFork());

        require(logs.length == 6, "post-tx query should return all committed logs");
        require(logs[0].emitter == address(TARGET), "first log emitter mismatch");
        require(logs[0].topics[0] == Target.Log.selector, "first log signature mismatch");
        require(abi.decode(logs[0].data, (uint256)) == 5, "first log value mismatch");
        require(logs[5].topics[0] == Target.Log.selector, "last log signature mismatch");
        require(abi.decode(logs[5].data, (uint256)) == 7, "last log value mismatch");
    }

    function testGetLogsQueryFiltersByEmitter() external view {
        PhEvm.Log[] memory logs =
            ph.getLogsQuery(_query(address(TARGET), bytes32(0)), _postTxFork());

        require(logs.length == 4, "emitter query should keep only target logs");
        for (uint256 i = 0; i < logs.length; i++) {
            require(logs[i].emitter == address(TARGET), "unexpected emitter");
        }
    }

    function testGetLogsQueryFiltersBySignature() external view {
        PhEvm.Log[] memory logs =
            ph.getLogsQuery(_query(address(0), Target.Log.selector), _postTxFork());

        require(logs.length == 4, "signature query should keep matching topic0 logs");
        for (uint256 i = 0; i < logs.length; i++) {
            require(logs[i].topics.length == 1, "unexpected topics length");
            require(logs[i].topics[0] == Target.Log.selector, "unexpected topic0");
        }
    }

    function testGetLogsQueryCombinesEmitterAndSignature() external view {
        PhEvm.Log[] memory logs =
            ph.getLogsQuery(_query(address(TARGET), Target.Log.selector), _postTxFork());

        require(logs.length == 2, "combined query should AND filters");
        require(abi.decode(logs[0].data, (uint256)) == 5, "first combined log mismatch");
        require(abi.decode(logs[1].data, (uint256)) == 7, "second combined log mismatch");
    }

    function testGetLogsQueryPreTxEmpty() external view {
        PhEvm.Log[] memory logs = ph.getLogsQuery(_query(address(0), bytes32(0)), _preTxFork());
        require(logs.length == 0, "pre-tx query should be empty");
    }

    function testGetLogsQueryCallSnapshots() external view {
        uint256 successfulNestedWriteId = _successfulNestedWriteId();

        PhEvm.Log[] memory preCallLogs = ph.getLogsQuery(
            _query(address(0), bytes32(0)),
            PhEvm.ForkId({forkType: 2, callIndex: successfulNestedWriteId})
        );
        PhEvm.Log[] memory postCallLogs = ph.getLogsQuery(
            _query(address(0), bytes32(0)),
            PhEvm.ForkId({forkType: 3, callIndex: successfulNestedWriteId})
        );

        require(preCallLogs.length == 3, "pre-call query should stop before nested write logs");
        require(postCallLogs.length == 5, "post-call query should include nested write logs");
        require(abi.decode(postCallLogs[4].data, (uint256)) == 7, "post-call trailing log mismatch");
    }

    function testGetLogsQueryRejectsInvalidForkType() external view {
        bytes memory calldata_ = abi.encodeWithSignature(
            "getLogsQuery((address,bytes32),(uint8,uint256))",
            _query(address(0), bytes32(0)),
            PhEvm.ForkId({forkType: 9, callIndex: 0})
        );

        (bool success,) = address(ph).staticcall(calldata_);
        require(!success, "invalid forkType should revert");
    }

    function testGetLogsQueryRejectsRevertedCallSnapshot() external view {
        // `+ 2` lands on the inner `writeStorage(9)` call inside `writeStorageAndRevert(9)`.
        uint256 revertedCallId = _successfulNestedWriteId() + 2;
        bytes memory calldata_ = abi.encodeWithSignature(
            "getLogsQuery((address,bytes32),(uint8,uint256))",
            _query(address(0), bytes32(0)),
            PhEvm.ForkId({forkType: 3, callIndex: revertedCallId})
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

    function _query(address emitter, bytes32 signature)
        internal
        pure
        returns (PhEvm.LogQuery memory)
    {
        return PhEvm.LogQuery({emitter: emitter, signature: signature});
    }

    function _preTxFork() internal pure returns (PhEvm.ForkId memory) {
        return PhEvm.ForkId({forkType: 0, callIndex: 0});
    }

    function _postTxFork() internal pure returns (PhEvm.ForkId memory) {
        return PhEvm.ForkId({forkType: 1, callIndex: 0});
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetLogsQueryPostTxAllLogs.selector);
        registerCallTrigger(this.testGetLogsQueryFiltersByEmitter.selector);
        registerCallTrigger(this.testGetLogsQueryFiltersBySignature.selector);
        registerCallTrigger(this.testGetLogsQueryCombinesEmitterAndSignature.selector);
        registerCallTrigger(this.testGetLogsQueryPreTxEmpty.selector);
        registerCallTrigger(this.testGetLogsQueryCallSnapshots.selector);
        registerCallTrigger(this.testGetLogsQueryRejectsInvalidForkType.selector);
        registerCallTrigger(this.testGetLogsQueryRejectsRevertedCallSnapshot.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        OtherEmitter other = new OtherEmitter();
        TARGET.writeStorage(5);
        other.emitLog(5);

        CallFrameTrigger callFrameTrigger = new CallFrameTrigger(other);
        callFrameTrigger.trigger();
    }
}

contract CallFrameTrigger {
    OtherEmitter internal immutable other;

    constructor(OtherEmitter other_) {
        other = other_;
    }

    function trigger() external {
        TARGET.writeStorage(7);

        try TARGET.writeStorageAndRevert(9) {
            revert("expected writeStorageAndRevert to revert");
        } catch {}

        other.emitLog(7);
    }
}

contract OtherEmitter {
    event Log(uint256 value);

    function emitLog(uint256 value) external {
        emit Log(value);
    }
}
