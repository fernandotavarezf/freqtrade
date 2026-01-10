"""Hyperliquid exchange subclass"""

import asyncio
import logging
import re
import threading
import time
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import OrderedDict

from freqtrade.constants import BuySell
from freqtrade.enums import MarginMode, TradingMode
from freqtrade.exceptions import ExchangeError, OperationalException, RetryableOrderError, TemporaryError

from freqtrade.enums.runmode import NON_UTIL_MODES
from freqtrade.exceptions import ConfigurationError, ExchangeError, OperationalException, RetryableOrderError, TemporaryError
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


class TTLCache:
    """Simple TTL cache implementation for HIP-3 DEX data"""
    
    def __init__(self, ttl_seconds: int = 30, max_size: int = 100):
        self.ttl = timedelta(seconds=ttl_seconds)
        self.max_size = max_size
        self._cache: OrderedDict[str, tuple[Any, datetime]] = OrderedDict()
        self._lock = asyncio.Lock()
    
    def _is_expired(self, timestamp: datetime) -> bool:
        """Check if cached data is expired"""
        return datetime.now() - timestamp > self.ttl
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired"""
        async with self._lock:
            if key in self._cache:
                value, timestamp = self._cache[key]
                if not self._is_expired(timestamp):
                    # Move to end (LRU)
                    self._cache.move_to_end(key)
                    return value
                else:
                    # Remove expired entry
                    del self._cache[key]
            return None
    
    async def set(self, key: str, value: Any) -> None:
        """Set value in cache with current timestamp"""
        async with self._lock:
            # Remove oldest entries if cache is full
            while len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)
            
            self._cache[key] = (value, datetime.now())
            self._cache.move_to_end(key)
    
    async def clear(self) -> None:
        """Clear all cached data"""
        async with self._lock:
            self._cache.clear()
    
    async def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate cache entries matching a pattern"""
        async with self._lock:
            keys_to_remove = [k for k in self._cache.keys() if pattern in k]
            for key in keys_to_remove:
                del self._cache[key]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_entries = len(self._cache)
        expired_entries = sum(1 for _, timestamp in self._cache.values() if self._is_expired(timestamp))
        
        return {
            "total_entries": total_entries,
            "expired_entries": expired_entries,
            "valid_entries": total_entries - expired_entries,
            "max_size": self.max_size,
            "ttl_seconds": self.ttl.total_seconds()
        }


class HyperliquidRateLimitInfo:
    """Parse and store Hyperliquid rate limit information from error messages"""
    
    def __init__(self, error_msg: str):
        self.error_msg = error_msg
        self.requests_sent = None
        self.requests_allowed = None
        self.volume_traded = None
        self.is_rate_limit_error = False
        self._parse()
    
    def _parse(self):
        """Parse Hyperliquid rate limit error message"""
        try:
            # Pattern: "Too many cumulative requests sent (20629 > 16414) for cumulative volume traded $6415.83."
            # Use negative lookahead to exclude trailing period from capture
            pattern = r"Too many cumulative requests sent \((\d+)\s*>\s*(\d+)\)\s*for cumulative volume traded \$?([\d,]+\.?\d*)"
            match = re.search(pattern, self.error_msg, re.IGNORECASE)
            
            if match:
                self.is_rate_limit_error = True
                self.requests_sent = int(match.group(1))
                self.requests_allowed = int(match.group(2))
                self.volume_traded = float(match.group(3).replace(',', ''))
        except (ValueError, AttributeError, IndexError) as e:
            # If parsing fails, still mark as rate limit error if keywords present
            # but set basic defaults to prevent crashes
            logger.warning(f"Failed to parse Hyperliquid rate limit details: {e}. Raw message: {self.error_msg}")
            if "too many cumulative requests" in self.error_msg.lower():
                self.is_rate_limit_error = True
            
    def get_info_str(self) -> str:
        """Get formatted info string for logging"""
        if not self.is_rate_limit_error:
            return ""
        
        requests_over = self.requests_sent - self.requests_allowed
        usdc_needed = requests_over  # 1 USDC traded frees 1 request
        
        return (f"Rate limit exceeded: {self.requests_sent} requests sent > {self.requests_allowed} allowed. "
                f"Volume traded: ${self.volume_traded:.2f}. "
                f"Need ~${usdc_needed:.2f} more volume in taker orders to free up requests.")


class HyperliquidRateLimitManager:
    """Manages rate limit state and recovery for Hyperliquid"""
    
    def __init__(self):
        self.is_rate_limited = False
        self.rate_limit_info: Optional[HyperliquidRateLimitInfo] = None
        self.recovery_time: Optional[datetime] = None
        self.lock = threading.Lock()
    
    def enter_rate_limit_mode(self, error_info: HyperliquidRateLimitInfo) -> None:
        """Enter rate limited state"""
        with self.lock:
            self.is_rate_limited = True
            self.rate_limit_info = error_info
            self.recovery_time = datetime.now()
            logger.info(f"⚠️ Entered rate limit mode at {self.recovery_time}")
    
    def check_recovery_status(self) -> bool:
        """Check if we've recovered from rate limits"""
        with self.lock:
            if not self.is_rate_limited:
                return True
            # Auto-recovery logic can be added here if needed
            return False
    
    def exit_rate_limit_mode(self) -> None:
        """Clear rate limited state"""
        with self.lock:
            self.is_rate_limited = False
            self.rate_limit_info = None
            self.recovery_time = None
            logger.info("✅ Exited rate limit mode")


class HyperliquidVolumeTracker:
    """Tracks trading volume for rate limit recovery calculations"""
    
    def __init__(self):
        self.cumulative_volume = 0.0
        self.last_update = datetime.now()
        self.lock = threading.Lock()
    
    def record_trade_volume(self, volume_usdc: float) -> None:
        """Track successful trade volume"""
        with self.lock:
            self.cumulative_volume += volume_usdc
            self.last_update = datetime.now()
    
    def get_total_volume(self) -> float:
        """Return cumulative volume"""
        with self.lock:
            return self.cumulative_volume


class HyperliquidOrderQueue:
    """Queues orders during rate limit recovery"""
    
    def __init__(self, max_size: int = 50, timeout_seconds: int = 300):
        self.queue: List[Dict[str, Any]] = []
        self.max_size = max_size
        self.timeout_seconds = timeout_seconds
        self.lock = threading.Lock()
    
    def enqueue_order(self, order_params: Dict[str, Any]) -> bool:
        """Add order to queue if space available"""
        with self.lock:
            if len(self.queue) >= self.max_size:
                logger.error(f"❌ Order queue is full ({self.max_size} orders). Cannot queue more orders.")
                return False
            
            order_params['queued_at'] = datetime.now()
            self.queue.append(order_params)
            return True
    
    def get_queued_count(self) -> int:
        """Return queue size"""
        with self.lock:
            return len(self.queue)
    
    def clear_queue(self) -> None:
        """Empty the queue"""
        with self.lock:
            self.queue.clear()


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

    def __init__(self, config: dict, *, exchange_config: dict, **kwargs):
        """Initialize Hyperliquid exchange with caching support and strict rate limiting"""
        super().__init__(config, exchange_config=exchange_config, **kwargs)

        # Initialize TTL caches for HIP-3 DEX data
        cache_ttl = exchange_config.get("dex_cache_ttl", 30)
        self._dex_balance_cache = TTLCache(ttl_seconds=cache_ttl, max_size=50)
        self._dex_position_cache = TTLCache(ttl_seconds=cache_ttl, max_size=100)
        self._verification_threshold = exchange_config.get("verification_threshold", 0.01)

        # Initialize rate limit recovery components
        self._rate_limit_manager = HyperliquidRateLimitManager()
        self._volume_tracker = HyperliquidVolumeTracker()
        self._order_queue = HyperliquidOrderQueue()

        # Initialize the dedicated rate limiter with 10s minimum gap (12s with buffer)
        min_gap = exchange_config.get("request_min_gap_seconds", 12.0)
        self._rate_limiter = HyperliquidRateLimiter(min_request_gap_seconds=min_gap)

        # Initialize request timing tracker for 10-second spacing enforcement
        self._last_request_time: Optional[datetime] = None
        self._request_lock = threading.Lock()

        # Initialize API call tracking counters
        self._api_call_counters = {
            'get_balances': 0,
            'fetch_positions': 0,
            'create_order': 0,
            'fetch_order': 0,
            'fetch_orders': 0,
            'get_perp_account_status': 0,
            'set_margin_mode': 0,
            'total_wallet_calls': 0,
        }
        self._api_call_start_times = {}

        logger.info(f"Initialized Hyperliquid with DEX caching (TTL: {cache_ttl}s, "
                    f"verification threshold: {self._verification_threshold}) and rate limit recovery system")
        logger.info(f"🚦 Rate limiter active: {min_gap}s minimum gap between requests")

    def _track_api_call(self, call_type: str, details: str = "") -> None:
        """Track API call for bottleneck analysis"""
        import time
        
        # Handle unknown call types gracefully
        if call_type not in self._api_call_counters:
            logger.warning(f"Unknown API call type: {call_type}, adding to counters")
            self._api_call_counters[call_type] = 0
        
        self._api_call_counters[call_type] += 1
        if call_type in ['get_balances', 'fetch_positions', 'get_perp_account_status']:
            self._api_call_counters['total_wallet_calls'] += 1

        # Log the call with timing info
        call_count = self._api_call_counters[call_type]
        total_wallet = self._api_call_counters['total_wallet_calls']
        logger.info(f"🔍 API CALL [{call_type}]: #{call_count} | Wallet calls: {total_wallet} | {details}")

        # Warn about excessive calls
        if call_type == 'get_perp_account_status' and call_count > 10:
            logger.warning(f"⚠️ HIGH API USAGE: {call_count} GET_PERP_ACCOUNT_STATUS calls - potential bottleneck")
        elif self._api_call_counters['total_wallet_calls'] > 50:
            logger.warning(f"⚠️ EXCESSIVE WALLET CALLS: {total_wallet} total - check for polling loops")

    def get_api_call_stats(self) -> Dict[str, int]:
        """Get API call statistics for monitoring"""
        return self._api_call_counters.copy()

    def _wait_for_rate_limit(self, operation: str) -> None:
        """
        CRITICAL: Wait if needed before making an API call.
        This is called before EVERY API operation.
        """
        wait_time = self._rate_limiter.wait_if_needed()
        if wait_time > 0:
            logger.info(f"✋ Rate limit enforced for {operation}: waited {wait_time:.2f}s")
        
        # Also track the API call
        self._track_api_call(operation)

    def get_rate_limiter_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics for monitoring"""
        return self._rate_limiter.get_stats()

    def _adjust_hyperliquid_order(self, order: dict) -> dict:
        """
        Adjusts order response for Hyperliquid with enhanced verification and rate limiting.
        This method includes API calls for trade fetching, so it applies rate limiting.
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
                # CRITICAL: This makes an API call - apply rate limiting
                self._wait_for_rate_limit("get_trades_for_order")
                
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

    def _reload_markets(self):
        # Skip market reload to speed up startup and avoid API calls
        logger.debug("Skipping market reload for performance")

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
        # Use the new rate limiter system
        self._wait_for_rate_limit("get_balances")
        balances = super().get_balances()
        dexes = self._get_configured_hip3_dexes()
        
        for dex in dexes:
            try:
                # Check cache first
                cache_key = f"balance_{dex}"
                cached_balance = asyncio.run(self._dex_balance_cache.get(cache_key))
                
                if cached_balance is not None:
                    logger.info(f"Using cached balance for HIP-3 DEX '{dex}'")
                    dex_balance = cached_balance
                else:
                    logger.info(f"Fetching fresh balance for HIP-3 DEX '{dex}'")
                    dex_balance = super().get_balances(params={"dex": dex})
                    # Cache the result
                    asyncio.run(self._dex_balance_cache.set(cache_key, dex_balance))

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
            # Log cache stats
            cache_stats = self._dex_balance_cache.get_stats()
            logger.info(f"DEX balance cache stats: {cache_stats}")
        return balances

    def fetch_positions(
        self, pair: str | None = None, params: dict | None = None
    ) -> list[CcxtPosition]:
        """Fetch positions from default DEX and HIP-3 DEXes needed by tradable pairs."""
        # Use the new rate limiter system
        self._wait_for_rate_limit("fetch_positions")
        positions = super().fetch_positions(pair)
        dexes = self._get_configured_hip3_dexes()
        
        for dex in dexes:
            try:
                # Check cache first
                cache_key = f"positions_{dex}_{pair or 'all'}"
                cached_positions = asyncio.run(self._dex_position_cache.get(cache_key))
                
                if cached_positions is not None:
                    logger.info(f"Using cached positions for HIP-3 DEX '{dex}' pair '{pair or 'all'}'")
                    dex_positions = cached_positions
                else:
                    logger.info(f"Fetching fresh positions for HIP-3 DEX '{dex}' pair '{pair or 'all'}'")
                    dex_positions = super().fetch_positions(pair, params={"dex": dex})
                    # Cache the result
                    asyncio.run(self._dex_position_cache.set(cache_key, dex_positions))
                
                positions.extend(dex_positions)
                
            except Exception as e:
                logger.error(f"Could not fetch positions from HIP-3 DEX '{dex}': {e}")
        
        if dexes:
            self._log_exchange_response("fetch_positions", positions, add_info="combined")
            # Log cache stats
            cache_stats = self._dex_position_cache.get_stats()
            logger.info(f"DEX position cache stats: {cache_stats}")
        return positions

    def get_max_leverage(self, pair: str, stake_amount: float | None) -> float:
        # There are no leverage tiers
        if self.trading_mode == TradingMode.FUTURES:
            return self.markets[pair]["limits"]["leverage"]["max"]
        else:
            return 1.0

    def _lev_prep(self, pair: str, leverage: float, side: BuySell, accept_fail: bool = False):
        if self.trading_mode != TradingMode.SPOT:
            leverage = int(leverage)
            # This makes an API call - apply rate limiting
            self._wait_for_rate_limit("set_margin_mode")
            # self.set_margin_mode(pair, self.margin_mode, params={"leverage": leverage})

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

    def _should_verify_order(self, order: dict, pair: str) -> bool:
        """
        Determine if order verification should be performed based on smart criteria
        :param order: Order data
        :param pair: Trading pair
        :return: True if verification should be performed
        """
        # Skip verification for small orders based on threshold
        amount = order.get('amount', 0)
        filled = order.get('filled', 0)
        
        if amount > 0 and (filled / amount) < self._verification_threshold:
            logger.info(f"Skipping verification for order {order.get('id')} - "
                       f"filled ratio {filled/amount:.3f} < threshold {self._verification_threshold}")
            return False
        
        # Skip verification during high market volatility (could be detected via recent price changes)
        # For now, we'll use a simple time-based approach
        order_time = order.get('timestamp')
        if order_time:
            order_age = time.time() - (order_time / 1000)  # Convert ms to seconds
            if order_age > 300:  # Skip verification for orders older than 5 minutes
                logger.info(f"Skipping verification for order {order.get('id')} - "
                           f"order age {order_age:.1f}s > 300s threshold")
                return False
        
        return True

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

    def _is_hyperliquid_rate_limit_error(self, error_msg: str) -> bool:
        """
        Check if error message is a Hyperliquid rate limit error
        :param error_msg: Error message to check
        :return: True if rate limit error, False otherwise
        """
        rate_limit_keywords = [
            "too many cumulative requests",
            "rate limit",
            "429",
            "requests sent",
            "volume traded"
        ]
        error_lower = str(error_msg).lower()
        is_rate_limit = any(keyword in error_lower for keyword in rate_limit_keywords)
        if is_rate_limit:
            logger.debug(f"Detected rate limit error in message: {error_msg[:200]}...")
        return is_rate_limit
    
    def _parse_hyperliquid_rate_limit_error(self, error_msg: str) -> HyperliquidRateLimitInfo:
        """
        Parse Hyperliquid rate limit error to extract useful information
        :param error_msg: The error message to parse
        :return: HyperliquidRateLimitInfo object with parsed data
        """
        return HyperliquidRateLimitInfo(str(error_msg))
    
    def _get_retry_config(self) -> dict:
        """
        Get retry configuration from exchange config or use defaults
        :return: Dictionary with retry configuration
        """
        exchange_config = self._config.get("exchange", {})
        
        return {
            "max_retries": exchange_config.get("hyperliquid_max_retries", 3),  # Reduced retries
            "initial_delay": exchange_config.get("hyperliquid_retry_delay", 5.0),  # Increased initial delay
            "backoff_factor": exchange_config.get("hyperliquid_backoff_factor", 2.0),  # Reduced backoff
            "max_delay": exchange_config.get("hyperliquid_max_retry_delay", 20.0),  # Reduced max delay
            "request_delay": exchange_config.get("hyperliquid_request_delay", 1.0),  # Increased request delay
            "rate_limit_delay": exchange_config.get("hyperliquid_rate_limit_delay", 15.0),  # Increased rate limit delay
        }
    
    def _calculate_retry_delay(self, attempt: int, retry_config: dict, is_rate_limited: bool = False) -> float:
        """
        Calculate exponential backoff delay for retry
        :param attempt: Current retry attempt (0-based)
        :param retry_config: Retry configuration dictionary
        :param is_rate_limited: Whether this retry is due to rate limiting
        :return: Delay in seconds
        """
        if is_rate_limited:
            # When rate limited, Hyperliquid only allows 1 request per 10 seconds
            # Use configured rate_limit_delay (default 12s = 10s minimum + 2s buffer)
            rate_limit_delay = retry_config.get("rate_limit_delay", 12.0)
            logger.info(f"Rate limit detected - enforcing {rate_limit_delay}s delay (Hyperliquid allows 1 request per 10s when rate limited)")
            return rate_limit_delay
        
        # Normal exponential backoff for non-rate-limit errors
        delay = retry_config["initial_delay"] * (retry_config["backoff_factor"] ** attempt)
        return min(delay, retry_config["max_delay"])
    
    def _enforce_rate_limit_spacing(self) -> None:
        """
        Ensure minimum 10-second gap between ALL requests to prevent request accumulation.
        According to Hyperliquid docs: When rate limited, the system automatically
        allows one request every 10 seconds. We enforce this spacing always to avoid
        initial bursts that could lead to rate limit violations.
        
        This method now integrates with the dedicated HyperliquidRateLimiter for
        consistent rate limiting across all API calls.
        """
        # Use the dedicated rate limiter for consistent enforcement
        self._rate_limiter.wait_if_needed()
        
        # Also maintain the existing spacing logic for compatibility
        with self._request_lock:
            current_time = datetime.now()
            if self._last_request_time:
                elapsed = (current_time - self._last_request_time).total_seconds()
                logger.debug(f"Rate limit spacing check: elapsed {elapsed:.1f}s since last request")
                if elapsed < 10:
                    wait_time = 10 - elapsed
                    logger.info(f"⏱️ Enforcing 10s rate limit spacing - waiting {wait_time:.1f}s (last request was {elapsed:.1f}s ago)")
                    time.sleep(wait_time)
            else:
                logger.debug("Rate limit spacing: first request, no wait needed")
            self._last_request_time = current_time
    
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
        Create an order on Hyperliquid with rate limit recovery system and strict rate limiting
        :param pair: Trading pair
        :param ordertype: Order type (market, limit, etc.)
        :param side: Order side (buy, sell)
        :param amount: Order amount
        :param rate: Order price (for limit orders)
        :param leverage: Leverage to use for the order
        :param time_in_force: Time in force (GTC, IOC, FOK)
        :param reduceOnly: Whether this is a reduce-only order
        :param initial_order: Whether this is an initial order
        :return: Created order with verification
        """
        # Use the new rate limiter system for tracking
        self._wait_for_rate_limit("create_order")

        # Direct retry approach for rate limits - no queue system
        retry_config = self._get_retry_config()
        max_retries = 3  # Direct retries for rate limits

        for attempt in range(max_retries):
            try:
                # Enforce 10-second minimum gap between requests (critical for rate limit compliance)
                # This also calls the rate limiter for additional enforcement
                self._enforce_rate_limit_spacing()

                # Create the order using base class implementation
                logger.info(f"Creating {ordertype} {side} order for {pair} with amount {amount} (attempt {attempt + 1}/{max_retries})")

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

                # Log successful order creation
                logger.info(f"Order created successfully: {order.get('id')} with status: {order.get('status')}")

                # Add volume tracking on success for market orders
                if ordertype == 'market' and order.get('status') in ('closed', 'filled'):
                    try:
                        # Calculate order volume (amount * price)
                        filled = order.get('filled', 0)
                        avg_price = order.get('average') or order.get('price') or rate

                        if filled > 0 and avg_price > 0:
                            volume_usdc = filled * avg_price
                            self._volume_tracker.record_trade_volume(volume_usdc)
                            total_volume = self._volume_tracker.get_total_volume()
                            logger.info(f"💰 Volume tracked: ${volume_usdc:.2f} USDC (Total: ${total_volume:.2f})")
                    except Exception as vol_ex:
                        logger.warning(f"Failed to track volume: {vol_ex}")

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
                        # Update last request time after waiting to prevent immediate retry spacing issues
                        self._last_request_time = datetime.now()
                        continue
                    else:
                        # Max retries reached
                        logger.error(f"❌ Rate limit persisted after {max_retries} attempts")
                        raise TemporaryError(f"Rate limit exceeded after {max_retries} attempts: {ex}")
                else:
                    # Not a rate limit error - don't retry
                    logger.error(f"❌ Order creation failed with non-rate-limit error: {ex}")
                    raise

        # Should not reach here
        raise ExchangeError(f"Failed to create order after {max_retries} attempts")

    def fetch_order(self, order_id: str, pair: str, params: dict | None = None) -> CcxtOrder:
        """
        Fetch order with enhanced error handling and rate limit retry for Hyperliquid
        :param order_id: Order ID to fetch
        :param pair: Trading pair
        :param params: Additional parameters
        :return: Order data
        """
        # Use the new rate limiter system for tracking
        self._wait_for_rate_limit("fetch_order")

        retry_config = self._get_retry_config()
        max_retries = retry_config["max_retries"]

        last_error = None

        for attempt in range(max_retries + 1):
            try:
                # Enforce 10-second minimum gap before each attempt (including retries)
                self._enforce_rate_limit_spacing()
                
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt}/{max_retries} for fetching order {order_id}")
                else:
                    logger.info(f"Fetching order {order_id} for {pair}")
                
                order = super().fetch_order(order_id, pair, params)
                
                if not order:
                    logger.warning(f"No order data returned for order {order_id}")
                    raise ExchangeError(f"No order data returned for order {order_id}")
                    
                order = self._adjust_hyperliquid_order(order)
                self._log_exchange_response("fetch_order2", order)
                
                logger.info(f"Successfully fetched order {order_id}: status={order.get('status')}")
                return order
                
            except (TemporaryError, ExchangeError) as ex:
                last_error = ex
                error_msg = str(ex).lower()
                
                # Check if it's a rate limit error
                if "rate limit" in error_msg or "too many" in error_msg or "429" in error_msg:
                    if attempt < max_retries:
                        # Rate limit error - wait 10s and retry
                        logger.warning(f"⚠️ Rate limit error on attempt {attempt+1}/{max_retries}: {ex}")
                        logger.info(f"⏱️ Waiting 10s before retry...")
                        time.sleep(10)
                        # Update last request time after waiting
                        self._last_request_time = datetime.now()
                        continue
                    else:
                        # Max retries reached
                        logger.error(f"❌ Rate limit persisted after {max_retries} attempts")
                        raise TemporaryError(f"Rate limit exceeded after {max_retries} attempts: {ex}")
                else:
                    # Not a rate limit error - don't retry
                    logger.error(f"❌ Order fetch failed with non-rate-limit error: {ex}")
                    raise
                
            except Exception as ex:
                logger.error(f"Unexpected error fetching order {order_id}: {ex}")
                raise ExchangeError(f"Unexpected error fetching order {order_id}: {ex}")
        
        if last_error:
            raise last_error
        raise ExchangeError(f"Failed to fetch order {order_id} after retries")

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
        # Use the new rate limiter system for tracking
        self._wait_for_rate_limit("fetch_orders")

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

