// SPDX-License-Identifier: MIT
pragma solidity =0.7.6;

import "uniswap-v3-core/contracts/UniswapV3Factory.sol";
import "uniswap-v3-core/contracts/interfaces/IERC20Minimal.sol";
import "uniswap-v3-core/contracts/interfaces/IUniswapV3Pool.sol";
import "uniswap-v3-core/contracts/interfaces/callback/IUniswapV3MintCallback.sol";
import "uniswap-v3-core/contracts/interfaces/callback/IUniswapV3SwapCallback.sol";

contract UniV3Bench is IUniswapV3MintCallback, IUniswapV3SwapCallback {
    function deployPoolAndMint(
        address factory,
        address tokenA,
        address tokenB,
        uint24 fee,
        uint160 sqrtPriceX96,
        int24 tickLower,
        int24 tickUpper,
        uint128 liquidity
    ) external returns (address pool) {
        pool = UniswapV3Factory(factory).createPool(tokenA, tokenB, fee);

        IUniswapV3Pool(pool).initialize(sqrtPriceX96);

        (address token0, address token1) = _sortTokens(tokenA, tokenB);
        bytes memory data = abi.encode(token0, token1);

        IUniswapV3Pool(pool).mint(address(this), tickLower, tickUpper, liquidity, data);
    }

    function swap(
        address factory,
        address tokenA,
        address tokenB,
        uint24 fee,
        bool zeroForOne,
        int256 amountSpecified,
        uint160 sqrtPriceLimitX96
    ) external returns (int256 amount0, int256 amount1) {
        address pool = UniswapV3Factory(factory).getPool(tokenA, tokenB, fee);
        require(pool != address(0), "POOL_NOT_DEPLOYED");

        (address token0, address token1) = _sortTokens(tokenA, tokenB);
        bytes memory data = abi.encode(token0, token1);

        (amount0, amount1) = IUniswapV3Pool(pool).swap(
            msg.sender,
            zeroForOne,
            amountSpecified,
            sqrtPriceLimitX96,
            data
        );
    }

    function uniswapV3MintCallback(
        uint256 amount0Owed,
        uint256 amount1Owed,
        bytes calldata data
    ) external override {
        (address token0, address token1) = abi.decode(data, (address, address));

        if (amount0Owed > 0) IERC20Minimal(token0).transfer(msg.sender, amount0Owed);
        if (amount1Owed > 0) IERC20Minimal(token1).transfer(msg.sender, amount1Owed);
    }

    function uniswapV3SwapCallback(
        int256 amount0Delta,
        int256 amount1Delta,
        bytes calldata data
    ) external override {
        (address token0, address token1) = abi.decode(data, (address, address));

        if (amount0Delta > 0) IERC20Minimal(token0).transfer(msg.sender, uint256(amount0Delta));
        if (amount1Delta > 0) IERC20Minimal(token1).transfer(msg.sender, uint256(amount1Delta));
    }

    function _sortTokens(
        address tokenA,
        address tokenB
    ) internal pure returns (address token0, address token1) {
        (token0, token1) = tokenA < tokenB ? (tokenA, tokenB) : (tokenB, tokenA);
    }
}

