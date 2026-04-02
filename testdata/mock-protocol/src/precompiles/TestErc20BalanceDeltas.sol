// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

import {Assertion} from "credible-std/Assertion.sol";
import {PhEvm} from "credible-std/PhEvm.sol";
import {TARGET} from "./Target.sol";

contract TestErc20BalanceDeltas is Assertion {
    address internal constant ALICE = address(0xA11CE);
    address internal constant BOB = address(0xB0B);
    address internal constant CAROL = address(0xCA801);

    // `run_precompile_test_with_spec` deploys `TriggeringTx` from a fixed caller at nonce 1.
    // These are the first two contracts it creates in its constructor, so the addresses are stable.
    MockErc20TransferEmitter internal constant TOKEN_A =
        MockErc20TransferEmitter(0x293d5D48B9139c497F12Db849df5aaB60eC60e9d);
    MockErc20TransferEmitter internal constant TOKEN_B =
        MockErc20TransferEmitter(0x3efB87b08a79Fb121Cd406c92940Dc37319b6dc3);

    constructor() payable {}

    function testGetErc20Transfers() external view {
        PhEvm.Erc20TransferData[] memory transfers =
            ph.getErc20Transfers(address(TOKEN_A), _postTxFork());

        require(transfers.length == 4, "token A transfer count mismatch");
        _assertTransfer(transfers[0], address(TOKEN_A), ALICE, BOB, 10);
        _assertTransfer(transfers[1], address(TOKEN_A), ALICE, BOB, 7);
        _assertTransfer(transfers[2], address(TOKEN_A), BOB, CAROL, 3);
        _assertTransfer(transfers[3], address(TOKEN_A), ALICE, ALICE, 0);
    }

    function testGetErc20TransfersForTokens() external view {
        address[] memory tokens = new address[](2);
        tokens[0] = address(TOKEN_B);
        tokens[1] = address(TOKEN_A);

        PhEvm.Erc20TransferData[] memory transfers =
            ph.getErc20TransfersForTokens(tokens, _postTxFork());

        require(transfers.length == 6, "combined transfer count mismatch");
        _assertTransfer(transfers[0], address(TOKEN_A), ALICE, BOB, 10);
        _assertTransfer(transfers[1], address(TOKEN_B), BOB, ALICE, 4);
        _assertTransfer(transfers[2], address(TOKEN_A), ALICE, BOB, 7);
        _assertTransfer(transfers[3], address(TOKEN_A), BOB, CAROL, 3);
        _assertTransfer(transfers[4], address(TOKEN_A), ALICE, ALICE, 0);
        _assertTransfer(transfers[5], address(TOKEN_B), BOB, ALICE, 6);
    }

    function testGetErc20TransfersForEmptyTokenList() external view {
        address[] memory tokens = new address[](0);
        PhEvm.Erc20TransferData[] memory transfers =
            ph.getErc20TransfersForTokens(tokens, _postTxFork());
        require(transfers.length == 0, "empty token list should return no transfers");
    }

    function testChangedErc20BalanceDeltasAlias() external view {
        PhEvm.Erc20TransferData[] memory transfers =
            ph.getErc20Transfers(address(TOKEN_A), _postTxFork());
        PhEvm.Erc20TransferData[] memory deltas =
            ph.changedErc20BalanceDeltas(address(TOKEN_A), _postTxFork());

        require(transfers.length == deltas.length, "alias length mismatch");
        for (uint256 i = 0; i < transfers.length; i++) {
            _assertTransfer(
                deltas[i], transfers[i].token_addr, transfers[i].from, transfers[i].to, transfers[i].value
            );
        }
    }

    function testReduceErc20BalanceDeltas() external view {
        PhEvm.Erc20TransferData[] memory deltas =
            ph.reduceErc20BalanceDeltas(address(TOKEN_A), _postTxFork());

        require(deltas.length == 3, "reduced delta count mismatch");
        _assertTransfer(deltas[0], address(TOKEN_A), ALICE, BOB, 17);
        _assertTransfer(deltas[1], address(TOKEN_A), BOB, CAROL, 3);
        _assertTransfer(deltas[2], address(TOKEN_A), ALICE, ALICE, 0);
    }

    function _postTxFork() internal pure returns (PhEvm.ForkId memory) {
        return PhEvm.ForkId({forkType: 1, callIndex: 0});
    }

    function _assertTransfer(
        PhEvm.Erc20TransferData memory transferData,
        address token,
        address from,
        address to,
        uint256 value
    ) internal pure {
        require(transferData.token_addr == token, "token mismatch");
        require(transferData.from == from, "from mismatch");
        require(transferData.to == to, "to mismatch");
        require(transferData.value == value, "value mismatch");
    }

    function triggers() external view override {
        registerCallTrigger(this.testGetErc20Transfers.selector);
        registerCallTrigger(this.testGetErc20TransfersForTokens.selector);
        registerCallTrigger(this.testGetErc20TransfersForEmptyTokenList.selector);
        registerCallTrigger(this.testChangedErc20BalanceDeltasAlias.selector);
        registerCallTrigger(this.testReduceErc20BalanceDeltas.selector);
    }
}

contract TriggeringTx {
    constructor() payable {
        MockErc20TransferEmitter tokenA = new MockErc20TransferEmitter();
        MockErc20TransferEmitter tokenB = new MockErc20TransferEmitter();

        tokenA.emitTransfer(address(0xA11CE), address(0xB0B), 10);
        tokenB.emitTransfer(address(0xB0B), address(0xA11CE), 4);
        tokenA.emitTransfer(address(0xA11CE), address(0xB0B), 7);
        tokenA.emitTransfer(address(0xB0B), address(0xCA801), 3);
        tokenA.emitTransfer(address(0xA11CE), address(0xA11CE), 0);
        tokenB.emitTransfer(address(0xB0B), address(0xA11CE), 6);

        TARGET.readStorage();
    }
}

contract MockErc20TransferEmitter {
    event Transfer(address indexed from, address indexed to, uint256 value);

    function emitTransfer(address from, address to, uint256 value) external {
        emit Transfer(from, to, value);
    }
}
