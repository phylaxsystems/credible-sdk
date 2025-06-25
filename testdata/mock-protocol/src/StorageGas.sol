// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

contract SSTOREGas {
    fallback() external {
        assembly {
            sstore(0x0, 0x1)
        }
    }

    // This function exists so the sstore in the fallback function is not optimized out
    function get() external view returns (uint256 val) {
        assembly {
            val := sload(0x0)
        }
    }
}

contract SLOADGas {
    fallback() external {
        assembly {
            log1(sload(0x0), 0, 0)
        }
    }

    // This function exists so the sload in the fallback function is not optimized out
    function set(uint256 val) external {
        assembly {
            sstore(0x0, val)
        }
    }
}
