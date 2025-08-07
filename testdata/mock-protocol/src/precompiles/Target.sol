// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

Target constant TARGET = Target(
    address(0x118DD24a3b0D02F90D8896E242D3838B4D37c181)
);

contract Target {
    event Log(uint256 value);
    event Log2(uint256 value);

    uint256 value;
    Reverts public reverts;

    address public impl;

    constructor() payable {
        value = 1;
    }

    function initProxy() external {
        impl = address(new ProxyImpl());
    }

    function readStorage() external view returns (uint256) {
        return value;
    }

    function incrementStorage() public {
        uint256 _value = value + 1;
        writeStorage(_value);
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

    function recursiveCalls() public {
        value++;
        while (value != 4) {
            (bool success, ) = address(this).call(
                abi.encodeWithSelector(this.recursiveCalls.selector)
            );
            (success);
        }
    }
    function unhandledRevert() external {
        reverts = new Reverts();
        (bool success, ) = address(reverts).call("");
        (success);
    }
    fallback(bytes calldata) external returns (bytes memory data) {
        if (impl != address(0)) {
            (bool success, bytes memory retData) = impl.delegatecall(msg.data);
            if (!success) {
                revert("delegatecall failed");
            } else {
                return retData;
            }
        }
    }
}

contract ProxyImpl {
    event Log(uint256 value);
    event Log2(uint256 value);

    uint256 value;

    function readStorageProxy() external view returns (uint256) {
        return value;
    }

    function writeStorageProxy(uint256 value_) public {
        value = value_;
        emit Log(value);
        emit Log2(value);
    }
}

contract Reverts {
    fallback() external {
        revert("revert");
    }
}
