// SPDX-License-Identifier: UNLICENSED
pragma solidity 0.8.28;

import {SimpleLendingAssertion} from "../src/SimpleLendingAssertion.a.sol";
import {SimpleLending, MockPriceFeed, MockTokenPriceFeed, IERC20} from "../../src/SimpleLending.sol";
import {Test} from "../../lib/credible-std/lib/forge-std/src/Test.sol";
import {CredibleTest} from "../../lib/credible-std/src/CredibleTest.sol";
import {ERC20} from "../../lib/openzeppelin-contracts/contracts/token/ERC20/ERC20.sol";
import {console} from "../../lib/credible-std/lib/forge-std/src/console.sol";

contract MockERC20 is ERC20 {
    uint8 private _decimals;

    constructor(string memory name_, string memory symbol_, uint8 decimals_) ERC20(name_, symbol_) {
        _decimals = decimals_;
    }

    function mint(address to, uint256 amount) public {
        _mint(to, amount);
    }

    function decimals() public view override returns (uint8) {
        return _decimals;
    }
}

contract TestSimpleLendingAssertion is CredibleTest, Test {
    SimpleLending public assertionAdopter;
    SimpleLendingAssertion public assertion;
    MockPriceFeed public ethPriceFeed;
    MockTokenPriceFeed public tokenPriceFeed;
    IERC20 public borrowToken;

    address testUser = address(0xBEEF);
    uint256 constant INITIAL_ETH_PRICE = 2000e18; // $2000 per ETH
    uint256 constant INITIAL_TOKEN_PRICE = 1e18; // $1 per token

    function setUp() public {
        // Deploy mock ERC20 token
        MockERC20 token = new MockERC20("Test Token", "TEST", 18);
        borrowToken = IERC20(address(token));

        // Deploy price feeds
        ethPriceFeed = new MockPriceFeed();
        tokenPriceFeed = new MockTokenPriceFeed();

        // Set initial prices
        ethPriceFeed.setPrice(INITIAL_ETH_PRICE);
        tokenPriceFeed.setPrice(INITIAL_TOKEN_PRICE);

        // Deploy lending protocol
        assertionAdopter = new SimpleLending(address(borrowToken), address(ethPriceFeed), address(tokenPriceFeed));

        // Mint tokens to lending protocol for borrowing
        token.mint(address(assertionAdopter), 1_000_000e18);

        // Deploy assertion
        assertion = new SimpleLendingAssertion(address(assertionAdopter));

        // Setup test user
        vm.deal(testUser, 10 ether);
    }

    function testAssertionCatchesUnsafeWithdrawal() public {
        // User deposits 1 ETH as collateral
        vm.startPrank(testUser);
        assertionAdopter.deposit{value: 1 ether}();

        // User borrows 1500 USDC (75% of collateral value at $2000/ETH)
        assertionAdopter.borrow(1500e18); // Max borrow based on 75% collateral ratio

        // Register the assertion
        cl.addAssertion(
            "collateralBalance",
            address(assertionAdopter),
            type(SimpleLendingAssertion).creationCode,
            abi.encode(address(assertionAdopter))
        );

        // Try to withdraw 0.5 ETH - this should fail the assertion
        // because remaining collateral wouldn't cover the loan
        vm.expectRevert("Assertions Reverted");
        cl.validate(
            "collateralBalance",
            address(assertionAdopter),
            0,
            abi.encodeWithSelector(assertionAdopter.withdraw.selector, 0.5 ether)
        );
        vm.stopPrank();
    }

    function testAssertionAllowsSafeWithdrawal() public {
        // User deposits 1 ETH as collateral
        vm.startPrank(testUser);
        assertionAdopter.deposit{value: 1 ether}();

        // User borrows only 500 USDC (25% of collateral value)
        assertionAdopter.borrow(500e18);

        // Register the assertion
        cl.addAssertion(
            "borrowedInvariant",
            address(assertionAdopter),
            type(SimpleLendingAssertion).creationCode,
            abi.encode(address(assertionAdopter))
        );

        // Try to withdraw 0.5 ETH - this should succeed
        // because remaining collateral still covers the loan
        cl.validate(
            "borrowedInvariant",
            address(assertionAdopter),
            0,
            abi.encodeWithSelector(assertionAdopter.withdraw.selector, 0.5 ether)
        );

        vm.stopPrank();
    }

    function testAssertionCatchesProtocolDrain() public {
        // Setup multiple users depositing ETH
        address[] memory users = new address[](5);
        for (uint256 i = 0; i < 5; i++) {
            users[i] = address(uint160(0xbeef + i));
            vm.deal(users[i], 2000 ether);

            vm.prank(users[i]);
            assertionAdopter.deposit{value: 2000 ether}();
        }

        // Total protocol collateral is now 10000 ETH
        assertEq(assertionAdopter.totalCollateral(), 10000 ether);

        // Register the assertion
        cl.addAssertion(
            "ethDrain",
            address(assertionAdopter),
            type(SimpleLendingAssertion).creationCode,
            abi.encode(address(assertionAdopter))
        );

        // Create attacker EOA
        address attacker = address(0xdead);

        // Try to drain using the buggy function
        vm.prank(attacker);
        vm.expectRevert("Assertions Reverted");
        cl.validate(
            "ethDrain",
            address(assertionAdopter),
            0,
            abi.encodeWithSelector(assertionAdopter.buggyWithdraw.selector, 8000 ether)
        );
    }

    function testAssertionAllowsNormalWithdrawals() public {
        // Setup multiple users depositing ETH
        address[] memory users = new address[](5);
        for (uint256 i = 0; i < 5; i++) {
            users[i] = address(uint160(0xbeef + i));
            vm.deal(users[i], 2000 ether);

            vm.prank(users[i]);
            assertionAdopter.deposit{value: 2000 ether}();
        }

        // Register the assertion
        cl.addAssertion(
            "ethDrain",
            address(assertionAdopter),
            type(SimpleLendingAssertion).creationCode,
            abi.encode(address(assertionAdopter))
        );

        // Normal withdrawal of 40% from one user should work
        vm.prank(users[0]);
        cl.validate(
            "ethDrain",
            address(assertionAdopter),
            0,
            abi.encodeWithSelector(assertionAdopter.withdraw.selector, 800 ether)
        );
    }
}
