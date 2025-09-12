// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

contract MockContract {
    bool public invalidate;
    bool public state;

    function setInvalidate(bool _invalidate) external {
        invalidate = _invalidate;
    }

    function setState() external {
        state = true;
    }

    function returnsValue() external pure returns (bool) {
        return true;
    }
}
