// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.28;

contract RegistrationMock {
    // Events to index
    event AssertionAdded(address contractAddress, bytes32 assertionId, uint256 activeAtBlock);

    event AssertionRemoved(address contractAddress, bytes32 assertionId, uint256 activeAtBlock);

    uint64 public constant TIMELOCK = 64;

    function addAssertion(address contractAddress, bytes32 assertionId) public {
        emit AssertionAdded(contractAddress, assertionId, block.number + TIMELOCK);
    }

    function removeAssertion(address contractAddress, bytes32 assertionId) public {
        emit AssertionRemoved(contractAddress, assertionId, block.number + TIMELOCK);
    }
}
