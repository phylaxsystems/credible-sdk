// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

contract SelfDestructable {
    uint256 public value;

    constructor() payable {
        value = 42;
    }

    function setValue(uint256 _value) external {
        value = _value;
    }

    function destroy(address payable beneficiary) external {
        selfdestruct(beneficiary);
    }

    receive() external payable {}
}

/// @title Orchestrator
/// @notice Executes the full test scenario in a single transaction:
///         1. Create Contract
///         2. Write state
///         3. Call selfdestruct
///         4. Send funds to the address
contract Orchestrator {
    // Events for debugging/tracing
    event Step1_ContractCreated(address indexed contractAddr, uint256 balance);
    event Step2_StateWritten(address indexed contractAddr, uint256 newValue);
    event Step3_SelfDestructed(address indexed contractAddr, address indexed beneficiary);
    event Step4_FundsSent(address indexed contractAddr, uint256 amount, bool success);
    event FinalState(address indexed contractAddr, uint256 balance, uint256 codeSize);

    address public createdContract;
    address public constant BENEFICIARY = address(0xBEEF);

    constructor() payable {}

    /// @notice Execute the full scenario in one transaction
    /// @return The address of the created (and destroyed) contract
    function executeScenario() external payable returns (address) {
        // STEP 1: Create contract with 1 ETH
        SelfDestructable newContract = new SelfDestructable{value: 1 ether}();
        createdContract = address(newContract);
        emit Step1_ContractCreated(createdContract, createdContract.balance);

        // STEP 2: Write state
        newContract.setValue(100);
        emit Step2_StateWritten(createdContract, 100);

        // STEP 3: Call selfdestruct
        // This sends the 1 ETH to BENEFICIARY
        newContract.destroy(payable(BENEFICIARY));
        emit Step3_SelfDestructed(createdContract, BENEFICIARY);

        // STEP 4: Send funds to the now-destroyed address
        // Post-Cancun (EIP-6780): Contract created in same TX is fully destroyed
        // So this address should now be an EOA-like empty account
        (bool success, ) = createdContract.call{value: 0.5 ether}("");
        emit Step4_FundsSent(createdContract, 0.5 ether, success);

        // Log final state
        uint256 codeSize;
        address target = createdContract;
        assembly {
            codeSize := extcodesize(target)
        }
        emit FinalState(createdContract, createdContract.balance, codeSize);

        return createdContract;
    }

    receive() external payable {}
}

/// @title PreExistingContract
/// @notice Deploy this in a SEPARATE transaction, then call selfdestruct in another
///         to test the Cancun behavior (code should persist)
contract PreExistingContract {
    uint256 public value;

    constructor() payable {
        value = 42;
    }

    function setValue(uint256 _value) external {
        value = _value;
    }

    function destroy(address payable beneficiary) external {
        selfdestruct(beneficiary);
    }

    receive() external payable {}
}

/// @title TestHelper
/// @notice Helper contract to interact with pre-existing contracts
contract TestHelper {
    event PreExistingSelfDestructed(
        address indexed target,
        address indexed beneficiary,
        uint256 balanceBefore,
        uint256 beneficiaryBalanceAfter
    );

    event StateAfterDestruct(
        address indexed target,
        uint256 balance,
        uint256 codeSize,
        bytes32 codeHash
    );

    /// @notice Call selfdestruct on a pre-existing contract and log the results
    function destroyPreExisting(
        address target,
        address payable beneficiary
    ) external {
        uint256 balanceBefore = target.balance;

        // Call destroy
        PreExistingContract(payable(target)).destroy(beneficiary);

        // Log state after
        uint256 codeSize;
        bytes32 codeHash;
        assembly {
            codeSize := extcodesize(target)
            codeHash := extcodehash(target)
        }

        emit PreExistingSelfDestructed(
            target,
            beneficiary,
            balanceBefore,
            beneficiary.balance
        );

        emit StateAfterDestruct(target, target.balance, codeSize, codeHash);
    }

    /// @notice Send funds to an address and check state
    function sendFundsAndCheck(address target) external payable {
        (bool success, ) = target.call{value: msg.value}("");
        require(success, "Transfer failed");

        uint256 codeSize;
        bytes32 codeHash;
        assembly {
            codeSize := extcodesize(target)
            codeHash := extcodehash(target)
        }

        emit StateAfterDestruct(target, target.balance, codeSize, codeHash);
    }
}