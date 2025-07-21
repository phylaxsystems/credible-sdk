// SPDX-License-Identifier: MIT
pragma solidity ^0.8.13;

import {PriceFeedAssertion} from "../src/PriceFeedAssertion.a.sol";
import {IPriceFeed} from "../../src/SimpleLending.sol";
import {MockTokenPriceFeed} from "../../src/SimpleLending.sol";
import {Test} from "../../lib/credible-std/lib/forge-std/src/Test.sol";
import {CredibleTest} from "../../lib/credible-std/src/CredibleTest.sol";

contract TestPriceFeedAssertion is CredibleTest, Test {
    MockTokenPriceFeed public assertionAdopter;

    function setUp() public {
        assertionAdopter = new MockTokenPriceFeed();
        vm.deal(address(0xdeadbeef), 1 ether);
        // Set initial token price
        assertionAdopter.setPrice(1 ether);
    }

    function testValidPriceUpdate() public {
        cl.assertion({
            label: "PriceFeedAssertion",
            adopter: address(assertionAdopter),
            createData: type(PriceFeedAssertion).creationCode,
            fnSelector: PriceFeedAssertion.assertionPriceDeviation.selector
        });

        // Update price within allowed range (5% increase)
        vm.prank(address(0xdeadbeef));
        assertionAdopter.setPrice(1.05 ether);
    }

    function testInvalidPriceUpdate() public {
        cl.assertion({
            label: "PriceFeedAssertion",
            adopter: address(assertionAdopter),
            createData: type(PriceFeedAssertion).creationCode,
            fnSelector: PriceFeedAssertion.assertionPriceDeviation.selector
        });

        vm.prank(address(0xdeadbeef));
        vm.expectRevert("Price deviation exceeds 10% threshold");
        // Update price beyond allowed range (15% decrease)
        assertionAdopter.setPrice(0.85 ether);
    }

    function testBoundaryPriceUpdate() public {
        cl.assertion({
            label: "PriceFeedAssertion",
            adopter: address(assertionAdopter),
            createData: type(PriceFeedAssertion).creationCode,
            fnSelector: PriceFeedAssertion.assertionPriceDeviation.selector
        });

        // Test exactly at boundary (10% increase)
        vm.prank(address(0xdeadbeef));
        assertionAdopter.setPrice(1.1 ether);
    }

    function testBoundaryNegativePriceUpdate() public {
        cl.assertion({
            label: "PriceFeedAssertion",
            adopter: address(assertionAdopter),
            createData: type(PriceFeedAssertion).creationCode,
            fnSelector: PriceFeedAssertion.assertionPriceDeviation.selector
        });

        // Test exactly at boundary (10% decrease)
        vm.prank(address(0xdeadbeef));
        assertionAdopter.setPrice(0.9 ether);
    }

    function testBatchPriceUpdates() public {
        cl.assertion({
            label: "PriceFeedAssertion",
            adopter: address(assertionAdopter),
            createData: type(PriceFeedAssertion).creationCode,
            fnSelector: PriceFeedAssertion.assertionPriceDeviation.selector
        });

        vm.expectRevert("Price deviation exceeds 10% threshold");
        // Execute batch price updates which includes a price that exceeds threshold
        new BatchTokenPriceUpdates(address(assertionAdopter));
    }
}

contract BatchTokenPriceUpdates {
    IPriceFeed public tokenPriceFeed;

    constructor(address tokenPriceFeed_) {
        tokenPriceFeed = IPriceFeed(tokenPriceFeed_);
        uint256 originalPrice = tokenPriceFeed.getPrice();

        TempTokenPriceUpdater updater = new TempTokenPriceUpdater(
            address(tokenPriceFeed)
        );

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
