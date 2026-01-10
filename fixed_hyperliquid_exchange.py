"""Hyperliquid exchange subclass with strict 10s rate limiting"""

import asyncio
import logging
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from freqtrade.constants import BuySell
from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exceptions import (
    ExchangeError, 
    OperationalException, 
    RetryableOrderError, 
    TemporaryError
)
from freqtrade.exchange import Exchange
from freqtrade.exchange.exchange_types import CcxtBalances, CcxtOrder, CcxtPosition, FtHas
from freqtrade.util.datetime_helpers import dt_from_ts

logger = logging.getLogger(__name__)


class HyperliquidRateLimiter:
    """
    CRITICAL: Enforces Hyperliquid's 10-second rate limit strictly.
    When rate limited, Hyperliquid allows exactly 1 request per 10 seconds.
    """
    
    def __init__(self, min_request_gap_seconds: float = 10.0):
        """
        Initialize rate limiter
        :param min_request_gap_seconds: Minimum seconds between requests (default 10s)
        """
        self.min_gap = min_request_gap_seconds
        self._last_request_time: Optional[datetime] = None
        self._lock = threading.Lock()
        self._request_count = 0
        
        logger.info(f"🚦 Hyperliquid Rate Limiter initialized: {self.min_gap}s minimum gap between requests")
    
    def wait_if_needed(self) -> float:
        """
        Wait if necessary to maintain minimum gap between requests.
        Returns the wait time in seconds.
        """
        with self._lock:
            now = datetime.now()
            
            if self._last_request_time is None:
                # First request ever
                self._last_request_time = now
                self._request_count += 1
                logger.debug(f"Request #{self._request_count}: First request, no wait")
                return 0.0
            
            # Calculate time since last request
            elapsed = (now - self._last_request_time).total_seconds()
            
            if elapsed < self.min_gap:
                # Need to wait
                wait_time = self.min_gap - elapsed
                logger.info(f"⏱️ Rate limit: waiting {wait_time:.2f}s (last request was {elapsed:.2f}s ago)")
                time.sleep(wait_time)
                
                # Update timestamp after waiting
                self._last_request_time = datetime.now()
                self._request_count += 1
                
                return wait_time
            else:
                # Enough time has passed
                self._last_request_time = now
                self._request_count += 1
                logger.debug(f"Request #{self._request_count}: {elapsed:.2f}s elapsed >= {self.min_gap}s, no wait needed")
                return 0.0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics"""
        with self._lock:
            return {
                "total_requests": self._request_count,
                "min_gap_seconds": self.min_gap,
                "last_request": self._last_request_time.isoformat() if self._last_request_time else None
            }


class Hyperliquid(Exchange):
    """
    Hyperliquid exchange class with strict rate limiting.
    CRITICAL: All API calls go through rate limiter to enforce 10s gaps.
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

    def __init__(self, config: dict, *, exchange_config: dict, **kwargs):
        """Initialize Hyperliquid exchange with strict rate limiting"""
        super().__init__(config, exchange_config=exchange_config, **kwargs)
        
        # Initialize rate limiter with 10s minimum gap
        # Adding 2s buffer for safety (12s total)
        min_gap = exchange_config.get("request_min_gap_seconds", 12.0)
        self._rate_limiter = HyperliquidRateLimiter(min_request_gap_seconds=min_gap)
        
        # Track API calls
        self._api_call_counter = 0
        
        logger.info("=" * 80)
        logger.info("HYPERLIQUID EXCHANGE INITIALIZED WITH STRICT RATE LIMITING")
        logger.info(f"• Minimum gap between requests: {min_gap}s")
        logger.info("• All API calls will wait if needed to maintain gap")
        logger.info("=" * 80)

    @property
    def _ccxt_config(self) -> dict:
        """CCXT configuration"""
        config = {}
        if self.trading_mode == TradingMode.SPOT:
            config.update({"options": {"defaultType": "spot"}})
        config.update(super()._ccxt_config)
        return config

    def _wait_for_rate_limit(self, operation: str) -> None:
        """
        CRITICAL: Wait if needed before making an API call.
        This is called before EVERY API operation.
        """
        wait_time = self._rate_limiter.wait_if_needed()
        if wait_time > 0:
            logger.info(f"✋ Rate limit enforced for {operation}: waited {wait_time:.2f}s")
        
        self._api_call_counter += 1

    def get_balances(self, params: dict | None = None) -> CcxtBalances:
        """Fetch balances with rate limiting"""
        self._wait_for_rate_limit("get_balances")
        logger.info(f"📊 API Call #{self._api_call_counter}: get_balances")
        return super().get_balances(params)

    def fetch_positions(
        self, pair: str | None = None, params: dict | None = None
    ) -> list[CcxtPosition]:
        """Fetch positions with rate limiting"""
        self._wait_for_rate_limit("fetch_positions")
        logger.info(f"📊 API Call #{self._api_call_counter}: fetch_positions (pair={pair})")
        return super().fetch_positions(pair, params)

    def get_max_leverage(self, pair: str, stake_amount: float | None) -> float:
        """Get max leverage (no API call)"""
        if self.trading_mode == TradingMode.FUTURES:
            return self.markets[pair]["limits"]["leverage"]["max"]
        else:
            return 1.0

    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
        """Prepare leverage (with API call)"""
        if self.trading_mode != TradingMode.SPOT:
            leverage = int(leverage)
            # This makes an API call - apply rate limiting
            self._wait_for_rate_limit("set_margin_mode")
            self.set_margin_mode(pair, self.margin_mode, params={"leverage": leverage})

    def create_order(
        self,
        *,
        pair: str,
        ordertype: str,
        side: str,
        amount: float,
        rate: float,
        leverage: float,
        time_in_force: str = "GTC",
        reduceOnly: bool = False,
        initial_order: bool = True,
    ) -> CcxtOrder:
        """
        Create order with strict rate limiting and retry logic.
        CRITICAL: This enforces 10s gaps and retries on rate limit errors.
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # CRITICAL: Wait for rate limit before attempting
                self._wait_for_rate_limit("create_order")
                
                logger.info(f"📤 API Call #{self._api_call_counter}: create_order "
                          f"(pair={pair}, side={side}, amount={amount:.4f}, type={ordertype}, attempt={attempt+1}/{max_retries})")
                
                # Create the order
                order = super().create_order(
                    pair=pair,
                    ordertype=ordertype,
                    side=side,
                    amount=amount,
                    rate=rate,
                    leverage=leverage,
                    time_in_force=time_in_force,
                    reduceOnly=reduceOnly,
                    initial_order=initial_order,
                )
                
                logger.info(f"✅ Order created successfully: {order.get('id')} (status: {order.get('status')})")
                return order
                
            except (TemporaryError, ExchangeError) as ex:
                error_msg = str(ex).lower()
                
                # Check if it's a rate limit error
                if "rate limit" in error_msg or "too many" in error_msg or "429" in error_msg:
                    if attempt < max_retries - 1:
                        # Rate limit hit - wait 10s and retry
                        logger.warning(f"⚠️ Rate limit error on attempt {attempt+1}/{max_retries}: {ex}")
                        logger.info(f"⏱️ Waiting 10s before retry...")
                        time.sleep(10)
                        continue
                    else:
                        # Max retries reached
                        logger.error(f"❌ Rate limit persisted after {max_retries} attempts")
                        raise TemporaryError(f"Rate limit exceeded after {max_retries} attempts: {ex}")
                else:
                    # Not a rate limit error - don't retry
                    logger.error(f"❌ Order creation failed with non-rate-limit error: {ex}")
                    raise
        
        # Should never reach here
        raise ExchangeError(f"Failed to create order after {max_retries} attempts")

    def fetch_order(self, order_id: str, pair: str, params: dict | None = None) -> CcxtOrder:
        """Fetch order with rate limiting"""
        self._wait_for_rate_limit("fetch_order")
        logger.info(f"📊 API Call #{self._api_call_counter}: fetch_order (id={order_id}, pair={pair})")
        
        try:
            order = super().fetch_order(order_id, pair, params)
            order = self._adjust_hyperliquid_order(order)
            return order
        except Exception as ex:
            logger.error(f"Error fetching order {order_id}: {ex}")
            raise

    def fetch_orders(
        self, pair: str, since: datetime, params: dict | None = None
    ) -> list[CcxtOrder]:
        """Fetch orders with rate limiting"""
        self._wait_for_rate_limit("fetch_orders")
        logger.info(f"📊 API Call #{self._api_call_counter}: fetch_orders (pair={pair})")
        
        try:
            orders = super().fetch_orders(pair, since, params)
            adjusted_orders = []
            for order in orders:
                adjusted_orders.append(self._adjust_hyperliquid_order(order))
            return adjusted_orders
        except Exception as ex:
            logger.error(f"Error fetching orders for {pair}: {ex}")
            raise

    def _adjust_hyperliquid_order(self, order: dict) -> dict:
        """
        Adjust order response for Hyperliquid quirks.
        This does NOT make API calls.
        """
        order_id = order.get("id", "unknown")
        symbol = order.get("symbol", "unknown")
        status = order.get("status", "unknown")
        filled = order.get("filled", 0)
        amount = order.get("amount", 0)
        
        # Log order state
        logger.debug(f"Adjusting order {order_id} for {symbol}: status={status}, filled={filled}/{amount}")
        
        # Handle Hyperliquid's average price calculation
        if status in ("canceled", "closed") and filled > 0 and order.get("average") is None:
            try:
                # Need to fetch trades to calculate average price
                logger.info(f"Calculating average price for order {order_id}")
                
                # CRITICAL: This makes an API call - apply rate limiting
                self._wait_for_rate_limit("get_trades_for_order")
                
                trades = self.get_trades_for_order(
                    order["id"], 
                    order["symbol"], 
                    since=dt_from_ts(order["timestamp"])
                )
                
                if trades:
                    total_amount = sum(t["amount"] for t in trades)
                    if total_amount > 0:
                        weighted_sum = sum(t["price"] * t["amount"] for t in trades)
                        order["average"] = weighted_sum / total_amount
                        logger.info(f"Calculated average price: {order['average']:.2f}")
                else:
                    logger.warning(f"No trades found for order {order_id}")
            
            except Exception as ex:
                logger.error(f"Error calculating average price for order {order_id}: {ex}")
        
        return order

    def get_funding_fees(
        self, pair: str, amount: float, is_short: bool, open_date: datetime
    ) -> float:
        """
        Fetch funding fees.
        CRITICAL: This makes API calls - apply rate limiting.
        """
        if self.trading_mode == TradingMode.FUTURES:
            try:
                self._wait_for_rate_limit("get_funding_fees")
                return self._fetch_and_calculate_funding_fees(pair, amount, is_short, open_date)
            except ExchangeError:
                logger.warning(f"Could not update funding fees for {pair}")
        return 0.0

    def dry_run_liquidation_price(
        self,
        pair: str,
        open_rate: float,
        is_short: bool,
        amount: float,
        stake_amount: float,
        leverage: float,
        wallet_balance: float,
        open_trades: list,
    ) -> float | None:
        """
        Calculate liquidation price (no API call).
        Formula from Hyperliquid docs.
        """
        position_size = amount
        price = open_rate
        position_value = price * position_size
        max_leverage = self.markets[pair]["limits"]["leverage"]["max"]
        
        maintenance_margin_required = position_value / max_leverage / 2
        
        if self.margin_mode == MarginMode.ISOLATED:
            margin_available = stake_amount - maintenance_margin_required
        elif self.margin_mode == MarginMode.CROSS:
            margin_available = wallet_balance - maintenance_margin_required
        else:
            raise OperationalException("Unsupported margin mode")
        
        maintenance_leverage = max_leverage * 2
        ll = 1 / maintenance_leverage
        side = -1 if is_short else 1
        
        liq_price = price - side * margin_available / position_size / (1 - ll * side)
        
        if self.trading_mode == TradingMode.FUTURES:
            return liq_price
        else:
            raise OperationalException("Only futures supported for leverage trading")

    def get_rate_limiter_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics for monitoring"""
        return self._rate_limiter.get_stats()
