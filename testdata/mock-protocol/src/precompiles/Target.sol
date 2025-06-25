// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

Target constant TARGET = Target(address(0x118DD24a3b0D02F90D8896E242D3838B4D37c181));

contract Target {
    event Log(uint256 value);
    event Log2(uint256 value);

    uint256 value;

    constructor() payable {
        value = 1;
    }

    function readStorage() external view returns (uint256) {
        return value;
    }

    function writeStorage(uint256 value_) public {
        value = value_;
        emit Log(value);
        emit Log2(value);
    }

    function writeStorageAndRevert(uint256 value_) external {
        writeStorage(value_);
        revert("revert from Target");
    }
}
