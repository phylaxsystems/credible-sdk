// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title SimpleLending
/// @notice A basic lending protocol that allows users to borrow tokens against ETH collateral
/// @dev This is a simplified implementation for demonstration purposes
contract SimpleLending {
    IERC20 public immutable borrowToken;

    // Price feed interfaces for getting asset prices
    IPriceFeed public immutable ethPriceFeed;
    IPriceFeed public immutable tokenPriceFeed;

    /// @notice Collateral ratio required (75% - can only borrow up to 75% of collateral value)
    uint256 public constant COLLATERAL_RATIO = 75;

    /// @notice Structure to track user's lending position
    /// @param collateralAmount Amount of ETH deposited as collateral (in wei)
    /// @param borrowedAmount Amount of tokens borrowed (in token's smallest unit)
    struct Position {
        uint256 collateralAmount;
        uint256 borrowedAmount;
    }

    /// @notice Mapping of user addresses to their lending positions
    mapping(address => Position) public positions;

    /// @notice Total ETH collateral in the contract (in wei)
    uint256 public totalCollateral;
    /// @notice Total tokens borrowed from the contract
    uint256 public totalBorrowed;

    /// @notice Initializes the lending contract
    /// @param _borrowToken Address of the ERC20 token that can be borrowed
    /// @param _ethPriceFeed Address of the ETH price feed contract
    /// @param _tokenPriceFeed Address of the token price feed contract
    constructor(address _borrowToken, address _ethPriceFeed, address _tokenPriceFeed) {
        borrowToken = IERC20(_borrowToken);
        ethPriceFeed = IPriceFeed(_ethPriceFeed);
        tokenPriceFeed = IPriceFeed(_tokenPriceFeed);
    }

    /// @notice Allows users to deposit ETH as collateral
    /// @dev Emits no events currently (consider adding them)
    function deposit() external payable {
        require(msg.value > 0, "Must deposit ETH");

        positions[msg.sender].collateralAmount += msg.value;
        totalCollateral += msg.value;
    }

    /// @notice Allows users to borrow tokens against their ETH collateral
    /// @param amount The amount of tokens to borrow
    /// @dev WARNING: Price calculation doesn't consider decimals properly
    function borrow(uint256 amount) external {
        require(amount > 0, "Must borrow non-zero amount");

        // Get current prices from oracles
        uint256 ethPrice = ethPriceFeed.getPrice();
        uint256 tokenPrice = tokenPriceFeed.getPrice();

        Position storage position = positions[msg.sender];

        // Calculate USD values for collateral and borrow amounts
        uint256 collateralValue = position.collateralAmount * ethPrice;
        uint256 newBorrowValue = (position.borrowedAmount + amount) * tokenPrice;

        // Ensure new borrow amount maintains required collateral ratio
        require(collateralValue * COLLATERAL_RATIO >= newBorrowValue * 100, "Would exceed collateral ratio");

        position.borrowedAmount += amount;
        totalBorrowed += amount;

        require(borrowToken.transfer(msg.sender, amount), "Token transfer failed");
    }

    /// @notice Allows users to repay their borrowed tokens
    /// @param amount The amount of tokens to repay
    function repay(uint256 amount) external {
        require(amount > 0, "Must repay non-zero amount");
        Position storage position = positions[msg.sender];
        require(position.borrowedAmount >= amount, "Cannot repay more than borrowed");

        require(borrowToken.transferFrom(msg.sender, address(this), amount), "Token transfer failed");

        position.borrowedAmount -= amount;
        totalBorrowed -= amount;
    }

    /*//////////////////////////////////////////////////////////////
                   THIS PART HAS BUGS, LET'S FIX THEM
    //////////////////////////////////////////////////////////////*/

    /// @notice Allows users to withdraw their ETH collateral
    /// @param amount The amount of ETH to withdraw (in wei)
    /// @dev WARNING: Missing check for maintaining sufficient collateral ratio after withdrawal
    function withdraw(uint256 amount) external {
        require(amount > 0, "Must withdraw non-zero amount");
        Position storage position = positions[msg.sender];
        require(position.collateralAmount >= amount, "Insufficient collateral");

        // Bug: No check if remaining collateral would be sufficient for current borrow
        position.collateralAmount -= amount;
        totalCollateral -= amount;

        (bool success,) = msg.sender.call{value: amount}("");
        require(success, "ETH transfer failed");
    }

    /// @notice A deliberately vulnerable withdrawal function used for testing assertions
    /// @dev WARNING: This function is intentionally unsafe and should not be used in production
    /// @dev It allows any caller to withdraw any amount of ETH without checks
    /// @dev Known vulnerabilities:
    ///      - No validation of caller's collateral balance
    ///      - No validation of borrowed token ratio
    ///      - Allows draining protocol's entire ETH balance
    /// @param amount The amount of ETH to withdraw (in wei)
    function buggyWithdraw(uint256 amount) external {
        // No checks, just transfer ETH
        payable(msg.sender).transfer(amount);
        // Update state to simulate proper withdrawal
        totalCollateral -= amount;
    }

    /*//////////////////////////////////////////////////////////////
                   BUGGY PART OVER
    //////////////////////////////////////////////////////////////*/
}

/// @title IPriceFeed
/// @notice Interface for price feed oracles
interface IPriceFeed {
    /// @notice Gets the current price of an asset
    /// @return The price with 18 decimals of precision
    function getPrice() external view returns (uint256);
    function setPrice(uint256 price) external;
}

/// @notice Mock price feed for ETH/USD
contract MockPriceFeed is IPriceFeed {
    uint256 public price;

    function getPrice() public view returns (uint256) {
        return price;
    }

    function setPrice(uint256 _price) external {
        price = _price;
    }
}

/// @notice Mock price feed for Token/USD
contract MockTokenPriceFeed is IPriceFeed {
    uint256 public price;

    function getPrice() public view returns (uint256) {
        return price;
    }

    function setPrice(uint256 _price) external {
        price = _price;
    }
}
