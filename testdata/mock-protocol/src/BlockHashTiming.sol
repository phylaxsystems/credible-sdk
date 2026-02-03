// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract BlockHashTiming {
    // EIP-2935 History Storage Contract
    address constant HISTORY_STORAGE = 0x0000F90827F1C53a10cb7A02335B175320002935;
    uint256 constant HISTORY_SERVE_WINDOW = 8191;

    event Result(
        uint256 blockNumber,
        uint256 queriedBlockNumber,
        bool success,
        bytes32 returnedHash,
        bytes32 expectedHash
    );

    function check() external {
        // Query for parent block (block.number - 1)
        // Per EIP-2935, system call at start of block N writes hash of block N-1
        uint256 parentBlock = block.number - 1;

        (bool success, bytes memory data) = HISTORY_STORAGE.staticcall(
            abi.encode(parentBlock)
        );

        bytes32 returnedHash = success && data.length == 32
            ? abi.decode(data, (bytes32))
            : bytes32(0);

        // Get expected hash via BLOCKHASH opcode (only works for last 256 blocks)
        bytes32 expectedHash = blockhash(parentBlock);

        emit Result(
            block.number,
            parentBlock,
            success,
            returnedHash,
            expectedHash
        );
    }

    function check_last() external {
        if (block.number <= HISTORY_SERVE_WINDOW) {
            emit Result(
                block.number,
                0,
                false,
                bytes32(0),
                bytes32(0)
            );
            return;
        }

        uint256 targetBlock = block.number - HISTORY_SERVE_WINDOW;

        (bool success, bytes memory data) = HISTORY_STORAGE.staticcall(
            abi.encode(targetBlock)
        );

        bytes32 returnedHash = success && data.length == 32
            ? abi.decode(data, (bytes32))
            : bytes32(0);

        bytes32 expectedHash = blockhash(targetBlock);

        emit Result(
            block.number,
            targetBlock,
            success,
            returnedHash,
            expectedHash
        );
    }

    function checkSpecificBlock(uint256 targetBlock) external {
        (bool success, bytes memory data) = HISTORY_STORAGE.staticcall(
            abi.encode(targetBlock)
        );

        bytes32 returnedHash = success && data.length == 32
            ? abi.decode(data, (bytes32))
            : bytes32(0);

        bytes32 expectedHash = blockhash(targetBlock);

        emit Result(
            block.number,
            targetBlock,
            success,
            returnedHash,
            expectedHash
        );
    }
}
