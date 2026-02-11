// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract BeaconRootTiming {
    address constant BEACON_ROOTS = 0x000F3df6D732807Ef1319fB7B8bB8522d0Beac02;
    uint256 constant HISTORY_BUFFER_LENGTH = 8191;

    event Result(
        uint256 blockNumber,
        uint256 blockTimestamp,
        bool success,
        bytes32 root
    );
    event ResultLast(
        uint256 blockNumber,
        uint256 blockTimestamp,
        uint256 queriedTimestamp,
        bool success,
        bytes32 root
    );

    function check() external {
        (bool success, bytes memory data) = BEACON_ROOTS.staticcall(
            abi.encode(block.timestamp)
        );

        bytes32 root = success && data.length == 32
            ? abi.decode(data, (bytes32))
            : bytes32(0);

        emit Result(block.number, block.timestamp, success, root);
    }

    function check_last() external {
        if (block.timestamp < (HISTORY_BUFFER_LENGTH - 1)) {
            emit ResultLast(
                block.number,
                block.timestamp,
                0,
                false,
                bytes32(0)
            );
            return;
        }

        uint256 targetTimestamp = block.timestamp - (HISTORY_BUFFER_LENGTH - 1);
        (bool success, bytes memory data) = BEACON_ROOTS.staticcall(
            abi.encode(targetTimestamp)
        );

        bytes32 root = success && data.length == 32
            ? abi.decode(data, (bytes32))
            : bytes32(0);

        emit ResultLast(
            block.number,
            block.timestamp,
            targetTimestamp,
            success,
            root
        );
    }
}
