"""Hyperliquid exchange subclass"""

import asyncio
import logging
from copy import deepcopy
from datetime import datetime
from typing import Any

from freqtrade.constants import BuySell
from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exceptions import ExchangeError, OperationalException, RetryableOrderError, TemporaryError

from freqtrade.enums.runmode import NON_UTIL_MODES
from freqtrade.exceptions import ConfigurationError, ExchangeError, OperationalException
from freqtrade.exchange import Exchange
from freqtrade.exchange.exchange_types import CcxtBalances, CcxtOrder, CcxtPosition, FtHas
from freqtrade.util.datetime_helpers import dt_from_ts


logger = logging.getLogger(__name__)


class Hyperliquid(Exchange):
    """Hyperliquid exchange class.
    Contains adjustments needed for Freqtrade to work with this exchange.
    """

    _ft_has: FtHas = {
        "ohlcv_has_history": False,
        "l2_limit_range": [20],
        "trades_has_history": False,
        "tickers_have_bid_ask": False,
        "stoploss_on_exchange": False,
        "exchange_has_overrides": {"fetchTrades": False},
        "marketOrderRequiresPrice": True,
        "download_data_parallel_quick": False,
        "ws_enabled": True,
    }
    _ft_has_futures: FtHas = {
        "stoploss_on_exchange": True,
        "stoploss_order_types": {"limit": "limit"},
        "stoploss_blocks_assets": False,
        "stop_price_prop": "stopPrice",
        "funding_fee_candle_limit": 500,
        "uses_leverage_tiers": False,
        "mark_ohlcv_price": "futures",
    }

    _supported_trading_mode_margin_pairs: list[tuple[TradingMode, MarginMode]] = [
        (TradingMode.SPOT, MarginMode.NONE),
        (TradingMode.FUTURES, MarginMode.ISOLATED),
        (TradingMode.FUTURES, MarginMode.CROSS),
    ]

    @property
    def _ccxt_config(self) -> dict:
        # ccxt Hyperliquid defaults to swap
        config = {}
        if self.trading_mode == TradingMode.SPOT:
            config.update({"options": {"defaultType": "spot"}})
        config.update(super()._ccxt_config)
        return config

    def _get_configured_hip3_dexes(self) -> list[str]:
        """Get list of configured HIP-3 DEXes."""
        return self._config.get("exchange", {}).get("hip3_dexes", [])

    def validate_config(self, config: dict) -> None:
        """Validate HIP-3 configuration at bot startup."""
        super().validate_config(config)
        configured = self._get_configured_hip3_dexes()
        if not configured or not self.markets:
            return
        if self.trading_mode != TradingMode.FUTURES:
            if configured:
                raise ConfigurationError(
                    "HIP-3 DEXes are only supported in FUTURES trading mode. "
                    "Please update your configuration!"
                )
            return
        if configured and self.margin_mode != MarginMode.ISOLATED:
            raise ConfigurationError(
                "HIP-3 DEXes require 'isolated' margin mode. "
                f"Current margin mode: '{self.margin_mode.value}'. "
                "Please update your configuration!"
            )

        available = {
            m.get("info", {}).get("dex")
            for m in self.get_markets(
                quote_currencies=[self._config["stake_currency"]],
                tradable_only=True,
                active_only=True,
            ).values()
            if m.get("info", {}).get("hip3")
        }
        available.discard(None)

        invalid = set(configured) - available
        if invalid:
            raise ConfigurationError(
                f"Invalid HIP-3 DEXes configured: {sorted(invalid)}. "
                f"Available DEXes matching your stake currency ({self._config['stake_currency']}): "
                f"{sorted(available)}. "
                f"Check your 'hip3_dexes' configuration!"
            )

    def market_is_tradable(self, market: dict[str, Any]) -> bool:
        """Check if market is tradable, including HIP-3 markets."""
        parent_check = super().market_is_tradable(market)

        market_info = market.get("info", {})
        if market_info.get("hip3") and self._config["runmode"] in NON_UTIL_MODES:
            configured = self._get_configured_hip3_dexes()
            if not configured:
                return False

            market_dex = market_info.get("dex")
            return parent_check and market_dex in configured

        return parent_check

    def get_balances(self, params: dict | None = None) -> CcxtBalances:
        """Fetch balances from default DEX and HIP-3 DEXes needed by tradable pairs.
        This override is not absolutely necessary and is only there for correct used / total values
        which are however not used by Freqtrade in futures mode at the moment.
        """
        balances = super().get_balances()
        dexes = self._get_configured_hip3_dexes()
        for dex in dexes:
            try:
                dex_balance = super().get_balances(params={"dex": dex})

                for currency, amount_info in dex_balance.items():
                    if currency in ["info", "free", "used", "total", "datetime", "timestamp"]:
                        continue

                    if currency not in balances:
                        balances[currency] = amount_info
                    else:
                        balances[currency]["free"] += amount_info["free"]
                        balances[currency]["used"] += amount_info["used"]
                        balances[currency]["total"] += amount_info["total"]

            except Exception as e:
                logger.error(f"Could not fetch balance for HIP-3 DEX '{dex}': {e}")

        if dexes:
            self._log_exchange_response("fetch_balance", balances, add_info="combined")
        return balances

    def fetch_positions(
        self, pair: str | None = None, params: dict | None = None
    ) -> list[CcxtPosition]:
        """Fetch positions from default DEX and HIP-3 DEXes needed by tradable pairs."""
        positions = super().fetch_positions(pair)
        dexes = self._get_configured_hip3_dexes()
        for dex in dexes:
            try:
                positions.extend(super().fetch_positions(pair, params={"dex": dex}))
            except Exception as e:
                logger.error(f"Could not fetch positions from HIP-3 DEX '{dex}': {e}")
        if dexes:
            self._log_exchange_response("fetch_positions", positions, add_info="combined")
        return positions

    def get_max_leverage(self, pair: str, stake_amount: float | None) -> float:
        # There are no leverage tiers
        if self.trading_mode == TradingMode.FUTURES:
            return self.markets[pair]["limits"]["leverage"]["max"]
        else:
            return 1.0

    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
        if self.trading_mode != TradingMode.SPOT:
            # Hyperliquid expects leverage to be an int
            leverage = int(leverage)
            # Hyperliquid needs the parameter leverage.
            # Don't use _set_leverage(), as this sets margin back to cross
            self.set_margin_mode(pair, self.margin_mode, params={"leverage": leverage})

    def dry_run_liquidation_price(
        self,
        pair: str,
        open_rate: float,  # Entry price of position
        is_short: bool,
        amount: float,
        stake_amount: float,
        leverage: float,
        wallet_balance: float,  # Or margin balance
        open_trades: list,
    ) -> float | None:
        """
        Optimized
        Docs: https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations
        Below can be done in fewer lines of code, but like this it matches the documentation.

        Tested with 196 unique ccxt fetch_positions() position outputs
        - Only first output per position where pnl=0.0
        - Compare against returned liquidation price
        Positions: 197 Average deviation: 0.00028980% Max deviation: 0.01309453%
        Positions info:
        {'leverage': {1.0: 23, 2.0: 155, 3.0: 8, 4.0: 7, 5.0: 4},
        'side': {'long': 133, 'short': 64},
        'symbol': {'BTC/USDC:USDC': 81,
                   'DOGE/USDC:USDC': 20,
                   'ETH/USDC:USDC': 53,
                   'SOL/USDC:USDC': 43}}
        """
        # Defining/renaming variables to match the documentation
        position_size = amount
        price = open_rate
        position_value = price * position_size
        max_leverage = self.markets[pair]["limits"]["leverage"]["max"]

        # Docs: The maintenance margin is half of the initial margin at max leverage,
        #       which varies from 3-50x. In other words, the maintenance margin is between 1%
        #       (for 50x max leverage assets) and 16.7% (for 3x max leverage assets)
        #       depending on the asset
        # The key thing here is 'Half of the initial margin at max leverage'.
        # A bit ambiguous, but this interpretation leads to accurate results:
        #       1. Start from the position value
        #       2. Assume max leverage, calculate the initial margin by dividing the position value
        #          by the max leverage
        #       3. Divide this by 2
        maintenance_margin_required = position_value / max_leverage / 2

        if self.margin_mode == MarginMode.ISOLATED:
            # Docs: margin_available (isolated) = isolated_margin - maintenance_margin_required
            margin_available = stake_amount - maintenance_margin_required
        elif self.margin_mode == MarginMode.CROSS:
            # Docs: margin_available (cross) = account_value - maintenance_margin_required
            margin_available = wallet_balance - maintenance_margin_required
        else:
            raise OperationalException("Unsupported margin mode for liquidation price calculation")

        # Docs: The maintenance margin is half of the initial margin at max leverage
        # The docs don't explicitly specify maintenance leverage, but this works.
        # Double because of the statement 'half of the initial margin at max leverage'
        maintenance_leverage = max_leverage * 2

        # Docs: l = 1 / MAINTENANCE_LEVERAGE (Using 'll' to comply with PEP8: E741)
        ll = 1 / maintenance_leverage

        # Docs: side = 1 for long and -1 for short
        side = -1 if is_short else 1

        # Docs: liq_price = price - side * margin_available / position_size / (1 - l * side)
        liq_price = price - side * margin_available / position_size / (1 - ll * side)

        if self.trading_mode == TradingMode.FUTURES:
            return liq_price
        else:
            raise OperationalException(
                "Freqtrade only supports isolated futures for leverage trading"
            )

    def get_funding_fees(
        self, pair: str, amount: float, is_short: bool, open_date: datetime
    ) -> float:
        """
        Fetch funding fees, either from the exchange (live) or calculates them
        based on funding rate/mark price history
        :param pair: The quote/base pair of the trade
        :param is_short: trade direction
        :param amount: Trade amount
        :param open_date: Open date of the trade
        :return: funding fee since open_date
        :raises: ExchangeError if something goes wrong.
        """
        # Hyperliquid does not have fetchFundingHistory
        if self.trading_mode == TradingMode.FUTURES:
            try:
                return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
            except ExchangeError:
                logger.warning(f"Could not update funding fees for {pair}.")
        return 0.0

    def _adjust_hyperliquid_order(
        self,
        order: dict,
    ) -> dict:
        """
        Adjusts order response for Hyperliquid with enhanced verification
        :param order: Order response from Hyperliquid
        :return: Adjusted order response
        """
        order_id = order.get("id", "unknown")
        symbol = order.get("symbol", "unknown")
        status = order.get("status", "unknown")
        filled = order.get("filled", 0)
        amount = order.get("amount", 0)
        
        logger.info(f"Adjusting Hyperliquid order {order_id} for {symbol}: "
                   f"status={status}, filled={filled}, amount={amount}")
        
        # Enhanced verification for unfilled or problematic orders
        if status == "closed" and filled == 0:
            logger.error(f"Order {order_id} closed but not filled - potential execution issue")
            # This might indicate a problem with the order execution
            # Consider raising an exception or marking the order as failed
            order["status"] = "failed"
            order["reason"] = "Order closed without execution"
            
        elif status == "canceled" and filled > 0:
            logger.warning(f"Order {order_id} was partially filled before cancellation: "
                          f"filled={filled}, original_amount={amount}")
            
        elif status in ("canceled", "closed") and filled > 0 and order["average"] is None:
            # Hyperliquid does not fill the average price in the order response
            # Fetch trades to calculate the average price to have the actual price
            # the order was executed at
            logger.info(f"Calculating average price for order {order_id} from trades")
            try:
                trades = self.get_trades_for_order(
                    order["id"], order["symbol"], since=dt_from_ts(order["timestamp"])
                )

                if trades:
                    total_amount = sum(t["amount"] for t in trades)
                    if total_amount > 0:
                        weighted_sum = sum(t["price"] * t["amount"] for t in trades)
                        order["average"] = weighted_sum / total_amount
                        logger.info(f"Calculated average price: {order['average']} for order {order_id}")
                    else:
                        logger.warning(f"No trade amount found for order {order_id}")
                else:
                    logger.warning(f"No trades found for executed order {order_id}")
                    # If order is closed/filled but no trades found, this might be an issue
                    if filled > 0:
                        logger.error(f"Order {order_id} shows as filled but no trades found - data inconsistency")
                        
            except Exception as ex:
                logger.error(f"Error fetching trades for order {order_id}: {ex}")
                # Don't fail the order adjustment, but log the error
                
        elif status == "open" and filled > 0:
            logger.info(f"Order {order_id} is partially filled: {filled}/{amount}")
            
        # Additional validation
        if filled > amount:
            logger.warning(f"Order {order_id} filled amount ({filled}) exceeds original amount ({amount})")
            
        return order

    def _verify_order_execution_sync(self, order_id: str, pair: str, max_retries: int = 5) -> dict:
        """
        Synchronous wrapper for order execution verification
        :param order_id: Order ID to verify
        :param pair: Trading pair
        :param max_retries: Maximum number of retries
        :return: Verified order data
        :raises: RetryableOrderError if verification fails
        """
        # Create a new event loop for this thread if needed
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        return loop.run_until_complete(self._verify_order_execution(order_id, pair, max_retries))

    async def _verify_order_execution(self, order_id: str, pair: str, max_retries: int = 5) -> dict:
        """
        Verify that an order has been executed properly on Hyperliquid
        :param order_id: Order ID to verify
        :param pair: Trading pair
        :param max_retries: Maximum number of retries
        :return: Verified order data
        :raises: RetryableOrderError if verification fails
        """
        for attempt in range(max_retries):
            try:
                order = self.fetch_order(order_id, pair)
                
                # Log order status for debugging
                logger.info(f"Order {order_id} verification attempt {attempt + 1}: "
                          f"status={order.get('status')}, filled={order.get('filled')}, "
                          f"remaining={order.get('remaining')}")
                
                # Check if order is properly filled
                if order.get('status') == 'closed' and order.get('filled', 0) > 0:
                    logger.info(f"Order {order_id} successfully verified as filled")
                    return order
                elif order.get('status') == 'canceled':
                    logger.warning(f"Order {order_id} was canceled")
                    raise RetryableOrderError(f"Order {order_id} was canceled on Hyperliquid")
                elif order.get('status') == 'open' and attempt < max_retries - 1:
                    # Wait a bit longer for Hyperliquid to process the order
                    wait_time = (attempt + 1) * 2
                    logger.info(f"Order {order_id} still open, waiting {wait_time}s before retry")
                    await asyncio.sleep(wait_time)
                    continue
                elif order.get('filled', 0) == 0 and order.get('status') == 'closed':
                    logger.error(f"Order {order_id} closed but not filled - this indicates a problem")
                    raise RetryableOrderError(f"Order {order_id} closed but not filled on Hyperliquid")
                
            except TemporaryError as ex:
                logger.warning(f"Temporary error verifying order {order_id}: {ex}")
                if attempt < max_retries - 1:
                    await asyncio.sleep((attempt + 1) * 2)
                    continue
                else:
                    raise RetryableOrderError(f"Failed to verify order {order_id} after {max_retries} attempts: {ex}")
            except Exception as ex:
                logger.error(f"Error verifying order {order_id}: {ex}")
                if attempt < max_retries - 1:
                    await asyncio.sleep((attempt + 1) * 2)
                    continue
                else:
                    raise RetryableOrderError(f"Failed to verify order {order_id} after {max_retries} attempts: {ex}")
        
        # If we get here, verification failed
        raise RetryableOrderError(f"Order {order_id} verification failed after {max_retries} attempts")

    def create_order(self, pair: str, ordertype: str, side: str, amount: float,
                    price: float | None = None, params: dict | None = None) -> CcxtOrder:
        """
        Create an order on Hyperliquid with enhanced verification
        :param pair: Trading pair
        :param ordertype: Order type (market, limit, etc.)
        :param side: Order side (buy, sell)
        :param amount: Order amount
        :param price: Order price (for limit orders)
        :param params: Additional parameters
        :return: Created order with verification
        """
        try:
            # Create the order
            logger.info(f"Creating {ordertype} {side} order for {pair} with amount {amount}")
            order = super().create_order(pair, ordertype, side, amount, price, params)
            
            # Log initial order creation
            logger.info(f"Order created successfully: {order.get('id')} with status: {order.get('status')}")
            
            # For market orders, verify execution immediately
            if ordertype == 'market':
                logger.info(f"Market order detected, verifying execution for order {order.get('id')}")
                try:
                    verified_order = self._verify_order_execution_sync(order.get('id'), pair)
                    return verified_order
                except RetryableOrderError as ex:
                    logger.error(f"Market order verification failed: {ex}")
                    # If verification fails, return the original order but mark it as needing attention
                    order["verification_failed"] = True
                    order["verification_error"] = str(ex)
                    return order
            
            return order
            
        except TemporaryError as ex:
            logger.error(f"Temporary error creating order on Hyperliquid: {ex}")
            if "429" in str(ex):
                logger.warning("Hyperliquid rate limit exceeded, will retry with backoff")
            raise TemporaryError(f"Hyperliquid temporary error: {ex}")
        except ExchangeError as ex:
            logger.error(f"Exchange error creating order on Hyperliquid: {ex}")
            if "insufficient" in str(ex).lower():
                raise ExchangeError(f"Insufficient balance on Hyperliquid: {ex}")
            elif "rate" in str(ex).lower() or "limit" in str(ex).lower():
                raise TemporaryError(f"Hyperliquid rate limiting: {ex}")
            else:
                raise ExchangeError(f"Failed to create order on Hyperliquid: {ex}")
        except Exception as ex:
            logger.error(f"Unexpected error creating order on Hyperliquid: {ex}")
            raise ExchangeError(f"Unexpected error creating order on Hyperliquid: {ex}")

    def fetch_order(self, order_id: str, pair: str, params: dict | None = None) -> CcxtOrder:
        """
        Fetch order with enhanced error handling for Hyperliquid
        :param order_id: Order ID to fetch
        :param pair: Trading pair
        :param params: Additional parameters
        :return: Order data
        """
        try:
            logger.info(f"Fetching order {order_id} for {pair}")
            order = super().fetch_order(order_id, pair, params)
            
            if not order:
                logger.warning(f"No order data returned for order {order_id}")
                raise ExchangeError(f"No order data returned for order {order_id}")
                
            order = self._adjust_hyperliquid_order(order)
            self._log_exchange_response("fetch_order2", order)
            
            logger.info(f"Successfully fetched order {order_id}: status={order.get('status')}")
            return order
            
        except TemporaryError as ex:
            logger.error(f"Temporary error fetching order {order_id}: {ex}")
            if "429" in str(ex):
                logger.warning(f"Hyperliquid rate limit hit while fetching order {order_id}")
            raise TemporaryError(f"Failed to fetch order {order_id}: {ex}")
        except ExchangeError as ex:
            logger.error(f"Exchange error fetching order {order_id}: {ex}")
            raise ExchangeError(f"Failed to fetch order {order_id}: {ex}")
        except Exception as ex:
            logger.error(f"Unexpected error fetching order {order_id}: {ex}")
            raise ExchangeError(f"Unexpected error fetching order {order_id}: {ex}")

    def fetch_orders(
        self, pair: str, since: datetime, params: dict | None = None
    ) -> list[CcxtOrder]:
        """
        Fetch orders with enhanced error handling for Hyperliquid
        :param pair: Trading pair
        :param since: Start time
        :param params: Additional parameters
        :return: List of orders
        """
        try:
            logger.info(f"Fetching orders for {pair} since {since}")
            orders = super().fetch_orders(pair, since, params)
            
            if not isinstance(orders, list):
                logger.error(f"Expected list of orders, got {type(orders)}")
                raise ExchangeError(f"Invalid response format when fetching orders for {pair}")
                
            logger.info(f"Fetched {len(orders)} orders for {pair}")
            
            for idx, order in enumerate(deepcopy(orders)):
                order2 = self._adjust_hyperliquid_order(order)
                orders[idx] = order2

            self._log_exchange_response("fetch_orders2", orders)
            logger.info(f"Successfully processed {len(orders)} orders for {pair}")
            return orders
            
        except TemporaryError as ex:
            logger.error(f"Temporary error fetching orders for {pair}: {ex}")
            if "429" in str(ex):
                logger.warning(f"Hyperliquid rate limit hit while fetching orders for {pair}")
            raise TemporaryError(f"Failed to fetch orders for {pair}: {ex}")
        except ExchangeError as ex:
            logger.error(f"Exchange error fetching orders for {pair}: {ex}")
            raise ExchangeError(f"Failed to fetch orders for {pair}: {ex}")
        except Exception as ex:
            logger.error(f"Unexpected error fetching orders for {pair}: {ex}")
            raise ExchangeError(f"Unexpected error fetching orders for {pair}: {ex}")
    def get_valid_pair_combination(self, curr_1: str, curr_2: str) -> Generator[str, None, None]:
        """
        Get valid pair combination of curr_1 and curr_2 by trying both combinations.
        """
        yielded = False
        for pair in (
            f"{curr_1}/{curr_2}",
            f"{curr_2}/{curr_1}",
        ):
            if pair in self.markets and self.markets[pair].get("active"):
                yielded = True
                yield pair
        if not yielded:
            raise ValueError(f"Could not combine {curr_1} and {curr_2} to get a valid pair.")

