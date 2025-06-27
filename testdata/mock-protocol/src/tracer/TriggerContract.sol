// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

contract Bs {
    event Foo();
    function foo() external {
        emit Foo();
    }
 }

contract TriggerContract {
    uint256 public slot0;
    uint256 public slot1;

    event Write();

    function trigger() external {
        _write();
        transfer(msg.sender);
    }

    function _write() internal {
        slot0 = 1;
        slot1 = 2;
        emit Write();
    }

    function transfer(address to) internal {
        (bool success, ) = to.call{value: 1}("");
        require(success, "Transfer failed");
    }
}

