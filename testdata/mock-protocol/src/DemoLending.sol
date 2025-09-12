// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import {Credible} from "credible-std/Credible.sol";
import {Test} from "forge-std/Test.sol";

DemoLending constant TARGET = DemoLending(address(0x118DD24a3b0D02F90D8896E242D3838B4D37c181));

contract DemoLending {
    mapping(address => uint256) public balances;
    mapping(address => uint256) public borrowed;

    event Deposited(address indexed user, uint256 amount);
    event Withdrawn(address indexed user, uint256 amount);
    event Borrowed(address indexed user, uint256 amount);
    event Repaid(address indexed user, uint256 amount);

    function deposit() public payable {
        require(msg.value > 0, "Must deposit some ETH");
        balances[msg.sender] = balances[msg.sender] + msg.value;
        emit Deposited(msg.sender, msg.value);
    }

    function withdraw(uint256 _amount) public {
        // Vulnerability: No change to balances
        (bool sent,) = msg.sender.call{value: _amount}("");
        require(sent, "Failed to send ETH");

        emit Withdrawn(msg.sender, _amount);
    }

    function getDeposit() public view returns (uint256) {
        return balances[msg.sender];
    }

    function getDebt() public view returns (uint256) {
        return borrowed[msg.sender];
    }
}

contract NormalTx {
    constructor() payable {
        TARGET.deposit{value: msg.value}();
    }
}

contract TriggeringTx {
    constructor() payable {
        uint256 value = msg.value;
        TARGET.deposit{value: value}();
        TARGET.withdraw(value + 1 ether);
    }
}

contract DemoLendingAssertion is Credible, Test {
    function testWithdraw() public {
        uint256 balanceNow = address(0x4545454545454545454545454545454545454545).balance;

        ph.forkPreTx();
        uint256 borrowBefore = TARGET.getDebt();
        uint256 balanceBefore = address(0x4545454545454545454545454545454545454545).balance;

        require(borrowBefore <= balanceBefore + balanceNow, "Withdraw: More than debt");
    }

    // Gas wasting fn
    function doStuff() public pure {
        for (uint256 i = 0; i < 256; i++) {
            keccak256(abi.encodePacked(i));
        }
    }

    function fnSelectors() external pure returns (bytes4[] memory selectors) {
        selectors = new bytes4[](2);
        selectors[0] = this.testWithdraw.selector;
        selectors[1] = this.doStuff.selector;
    }
}
