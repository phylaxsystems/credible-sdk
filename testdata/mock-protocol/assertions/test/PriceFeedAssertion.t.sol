// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import {PriceFeedAssertion} from "../src/PriceFeedAssertion.a.sol";
import {IPriceFeed} from "../../src/SimpleLending.sol";
import {MockTokenPriceFeed} from "../../src/SimpleLending.sol";
import {Test} from "../../lib/credible-std/lib/forge-std/src/Test.sol";
import {CredibleTest} from "../../lib/credible-std/src/CredibleTest.sol";

contract TestPriceFeedAssertion is CredibleTest, Test {
    BatchTokenPriceUpdates public batchTokenPriceUpdates;
    MockTokenPriceFeed public assertionAdopter;

    function setUp() public {
        assertionAdopter = new MockTokenPriceFeed();
        vm.deal(address(0xdeadbeef), 1 ether);
    }

    function testBatchPriceUpdates() public {
        BatchTokenPriceUpdates updater = new BatchTokenPriceUpdates(address(assertionAdopter));

        vm.prank(address(0xdeadbeef));
        // Set initial token price
        assertionAdopter.setPrice(1 ether);

        cl.addAssertion(
            "PriceFeedAssertion",
            address(assertionAdopter),
            type(PriceFeedAssertion).creationCode,
            abi.encode(address(assertionAdopter))
        );

        vm.prank(address(0xdeadbeef));
        vm.expectRevert("Assertions Reverted");
        // Execute batch price updates in validate
        cl.validate("PriceFeedAssertion", address(updater), 0, new bytes(0));
    }

    function testAllowsSafePriceUpdate() public {
        vm.prank(address(0xdeadbeef));
        // Set initial token price
        assertionAdopter.setPrice(1 ether);

        cl.addAssertion(
            "PriceFeedAssertion",
            address(assertionAdopter),
            type(PriceFeedAssertion).creationCode,
            abi.encode(address(assertionAdopter))
        );

        // Update price within allowed range (5% increase)
        vm.prank(address(0xdeadbeef));
        cl.validate(
            "PriceFeedAssertion",
            address(assertionAdopter),
            0,
            abi.encodeWithSelector(MockTokenPriceFeed.setPrice.selector, 1.05 ether)
        );
    }
}

contract BatchTokenPriceUpdates {
    IPriceFeed public tokenPriceFeed;

    constructor(address tokenPriceFeed_) {
        tokenPriceFeed = IPriceFeed(tokenPriceFeed_);
    }

    fallback() external {
        uint256 originalPrice = tokenPriceFeed.getPrice();

        TempTokenPriceUpdater updater = new TempTokenPriceUpdater(address(tokenPriceFeed));

        // Perform 10 token price updates (using realistic token/USD prices)
        updater.setPrice(0.95 ether); // $0.95
        updater.setPrice(1.05 ether); // $1.05
        updater.setPrice(0.9 ether); // $0.90
        updater.setPrice(1.1 ether); // $1.10
        updater.setPrice(0.85 ether); // $0.85 -- price deviates too much, should trigger assertion
        updater.setPrice(1.15 ether); // $1.15
        updater.setPrice(0.9 ether); // $0.90
        updater.setPrice(originalPrice); // Return to original price
    }
}

contract TempTokenPriceUpdater {
    IPriceFeed public tokenPriceFeed;

    constructor(address tokenPriceFeed_) {
        tokenPriceFeed = IPriceFeed(tokenPriceFeed_);
    }

    function setPrice(uint256 newPrice) external {
        tokenPriceFeed.setPrice(newPrice);
    }
}
