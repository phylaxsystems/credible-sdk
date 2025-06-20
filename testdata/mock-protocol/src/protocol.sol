// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

contract MockProtocol {
    bool public example;

    constructor() {
        example = true;
    }

    function checkBool() public view returns (bool) {
        return example;
    }
}
