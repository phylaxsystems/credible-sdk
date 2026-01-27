// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/// @notice Minimal contract that accepts ETH transfers.
/// Used for benchmarking EOA transfers to assertion adopter addresses.
contract PayableReceiver {
    receive() external payable {}
}
