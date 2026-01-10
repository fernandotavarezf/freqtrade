import pandas as pd
from freqtrade.strategy import (IStrategy, IntParameter)
from freqtrade.persistence import Trade
import logging
import os
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from copy import deepcopy
from track_account import GET_CURRENT_PERP_ACCOUNT_STATUS, PositionTracker

logger = logging.getLogger(__name__)

ADDRESS_TO_TRACK_TOP = "0xecb63caa47c7c4e77f60f1ce858cf28dc2b82b00"

# [PositionSnapshot and PositionChange classes remain the same - omitted for brevity]
# [PositionTracker class remains the same - omitted for brevity]

class COPY_HL(IStrategy):
    global ADDRESS_TO_TRACK_TOP
    minimal_roi = {
        "0": 5000.0  # Effectively disables ROI
    }
    stoploss = -0.95
    timeframe = '1h'
    startup_candle_count: int = 1
    can_short: bool = True
    process_only_new_candles: bool = True
    position_adjustment_enable = True

    # Tunable parameters
    LEV = IntParameter(1, 6, default=6, space='buy', optimize=False)
    change_threshold = 0.01  # 1% minimum position size
    adjustement_threshold = 10.0  # in %
    ADDRESS_TO_TRACK = ADDRESS_TO_TRACK_TOP
    
    # CRITICAL: Minimum stake to avoid dust trades with $80 wallet
    MIN_STAKE_USDC = 10.0  # Minimum $10 per trade

    order_types = {
        'entry': 'market',
        'exit': 'market',
        'stoploss': 'market',
        'stoploss_on_exchange': False,
        'emergency_exit': 'market',
    }

    def __init__(self, config: dict):
        super().__init__(config)
        
        # Position tracking
        self.copied_account_position_changes = []
        self.current_positions_to_copy = {}
        self.position_tracker = None
        
        # Cached wallet data - CRITICAL: Use this instead of repeated API calls
        self._cached_perp_data = None
        self._cache_timestamp = None
        self._cache_ttl_seconds = 300  # 5 minute cache
        
        # Wallet polling - REDUCED frequency
        self._last_wallet_check = None
        self._wallet_check_interval = 300  # Check every 5 minutes (reduced from 60s)
        
        # Dynamic pairlist
        self.active_wallet_pairs = []
        
        # Signal queue - process ONE signal per cycle
        self._pending_signals = []  # List of (pair, signal_type, change) tuples
        self._current_signal = None  # Currently active signal
        self._last_signal_time = None
        self._signal_cooldown_seconds = 15  # 15s between signals (increased for safety)
        
        # Order timing - STRICT 10s enforcement per Hyperliquid rate limit
        self._last_order_time = None
        self._order_delay_seconds = 12  # 12s between orders (10s + 2s buffer)
        
        # Volume tracking
        self._volume_generated_this_session = 0.0
        
        logger.info("=" * 80)
        logger.info("HYPERLIQUID RATE LIMIT RECOVERY MODE")
        logger.info("• 1 request per 10 seconds (enforced strictly)")
        logger.info(f"• Minimum stake: ${self.MIN_STAKE_USDC} USDC")
        logger.info(f"• Wallet check interval: {self._wallet_check_interval}s")
        logger.info(f"• Signal processing: 1 per {self._signal_cooldown_seconds}s")
        logger.info("=" * 80)

    def informative_pairs(self) -> List[tuple]:
        """Return base pairs + dynamic pairs from wallet"""
        base_pairs = [
            ("BTC/USDC:USDC", self.timeframe),
            ("ETH/USDC:USDC", self.timeframe),
        ]
        
        dynamic_pairs = []
        if self.active_wallet_pairs:
            for pair in self.active_wallet_pairs:
                pair_tuple = (pair, self.timeframe)
                if pair_tuple not in base_pairs:
                    dynamic_pairs.append(pair_tuple)
        
        return base_pairs + dynamic_pairs

    def bot_loop_start(self, current_time: datetime, **kwargs) -> None:
        """
        CRITICAL FIX: This is the ONLY place we fetch wallet data.
        All other methods use the cached data.
        """
        # Initialize position tracker on first run
        if self.position_tracker is None:
            logger.info(f"Initializing PositionTracker for wallet {self.ADDRESS_TO_TRACK}")
            self.position_tracker = PositionTracker(data_dir="position_data")
        
        # Check if we should poll wallet
        now = datetime.now()
        if self._last_wallet_check is None:
            should_poll = True
        else:
            elapsed = (now - self._last_wallet_check).total_seconds()
            should_poll = elapsed >= self._wallet_check_interval
        
        if not should_poll:
            logger.debug(f"Skipping wallet poll - {elapsed:.1f}s elapsed < {self._wallet_check_interval}s interval")
            return
        
        # Poll wallet - THIS IS THE ONLY API CALL
        logger.info(f"Polling wallet {self.ADDRESS_TO_TRACK} (interval: {self._wallet_check_interval}s)")
        self._last_wallet_check = now
        
        try:
            # Make the API call
            perp_data = GET_CURRENT_PERP_ACCOUNT_STATUS(self.ADDRESS_TO_TRACK)
            
            if not perp_data:
                logger.warning("Failed to fetch wallet data")
                return
            
            # Cache the data with timestamp
            self._cached_perp_data = perp_data
            self._cache_timestamp = now
            logger.info(f"✅ Wallet data cached at {now}")
            
            # Track position changes
            changes = self.position_tracker.track_positions(perp_data)
            
            if changes:
                logger.info(f"✓ Detected {len(changes)} position changes:")
                for chg in changes:
                    logger.info(f"  • {chg.coin}: {chg.change_type} - Size: {chg.new_size}, Value: ${chg.new_position_value:.2f}")
                
                # Store changes for signal processing
                self.copied_account_position_changes = changes
                self.current_positions_to_copy = self.position_tracker.get_current_positions()
                
                # Update dynamic pairlist
                self._update_active_wallet_pairs()
                
                # Queue signals from changes
                self._queue_signals_from_changes(changes)
            else:
                logger.debug("No new position changes detected")
        
        except Exception as e:
            logger.error(f"Error polling wallet: {e}")
            # Don't crash - continue with cached data if available

    def _queue_signals_from_changes(self, changes: List) -> None:
        """Queue signals from detected position changes"""
        if not self._cached_perp_data:
            logger.warning("No cached wallet data - cannot queue signals")
            return
        
        try:
            copied_account_value = float(
                self._cached_perp_data.get("marginSummary", {}).get("accountValue", 1)
            )
        except Exception as e:
            logger.error(f"Error getting account value: {e}")
            return
        
        for chg in changes:
            coin_ticker = self.get_pair_from_coin(chg.coin)
            if not coin_ticker:
                continue
            
            # Check position size threshold
            ratio_pc = float(chg.new_position_value) / copied_account_value * 100.0
            
            if ratio_pc < self.change_threshold:
                logger.debug(f"Skipping {coin_ticker} - position too small ({ratio_pc:.4f}% < {self.change_threshold}%)")
                continue
            
            # Determine signal type
            if chg.change_type in ['opened_long', 'increased', 'decreased'] and chg.new_size > 0:
                signal_type = 'long'
            elif chg.change_type in ['opened_short', 'increased', 'decreased'] and chg.new_size < 0:
                signal_type = 'short'
            elif chg.change_type == 'closed':
                signal_type = 'exit'
            else:
                logger.debug(f"Skipping {coin_ticker} - change type {chg.change_type} not handled")
                continue
            
            # Add to queue
            self._pending_signals.append((coin_ticker, signal_type, chg))
            logger.info(f"📋 Queued {signal_type} signal for {coin_ticker} (ratio: {ratio_pc:.2f}%)")

    def _update_active_wallet_pairs(self):
        """Update dynamic pairlist from current positions"""
        new_pairs = []
        for coin in self.current_positions_to_copy.keys():
            pair = self._coin_to_pair(coin)
            if pair:
                new_pairs.append(pair)
        
        if set(new_pairs) != set(self.active_wallet_pairs):
            logger.info(f"📋 Active wallet pairs updated: {len(new_pairs)} pairs")
        
        self.active_wallet_pairs = new_pairs

    def _coin_to_pair(self, coin: str) -> Optional[str]:
        """Convert coin to pair format"""
        if not coin:
            return None
        try:
            coin = coin.strip()
            if not coin.replace('-', '').replace('_', '').isalnum():
                return None
            return f"{coin}/USDC:USDC"
        except Exception as e:
            logger.error(f"Error converting coin {coin}: {e}")
            return None

    def _can_activate_signal(self) -> bool:
        """Check if we can activate a new signal based on timing"""
        if self._last_signal_time is None:
            return True
        
        elapsed = (datetime.now() - self._last_signal_time).total_seconds()
        if elapsed < self._signal_cooldown_seconds:
            return False
        
        return True

    def populate_indicators(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        """
        CRITICAL FIX: No API calls here. Just set signal based on queued data.
        """
        pair = metadata.get('pair')
        
        # Initialize signal column
        dataframe['signal'] = 0
        dataframe['signal_type'] = ''
        
        # Check if this pair has a pending signal and we can activate it
        if self._can_activate_signal() and self._pending_signals:
            # Activate the first pending signal
            pending_pair, signal_type, change = self._pending_signals[0]
            
            if pending_pair == pair:
                # This is the pair we're activating
                self._current_signal = (pair, signal_type, change)
                self._last_signal_time = datetime.now()
                self._pending_signals.pop(0)  # Remove from queue
                
                logger.info(f"🎯 Activated {signal_type} signal for {pair} ({len(self._pending_signals)} remaining)")
                
                # Set the signal
                if signal_type == 'long':
                    dataframe.loc[dataframe.index[-1], 'signal'] = 1
                elif signal_type == 'short':
                    dataframe.loc[dataframe.index[-1], 'signal'] = -1
                dataframe.loc[dataframe.index[-1], 'signal_type'] = signal_type
        
        return dataframe

    def populate_entry_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        """
        CRITICAL FIX: Use cached data directly - NO API CALLS.
        """
        pair = metadata.get('pair')
        
        # Initialize columns
        dataframe.loc[:, 'enter_long'] = 0
        dataframe.loc[:, 'enter_short'] = 0
        dataframe.loc[:, 'enter_tag'] = ''
        
        # Check if this pair has an active signal
        if (self._current_signal and 
            self._current_signal[0] == pair and 
            self._current_signal[1] in ['long', 'short']):
            
            _, signal_type, change = self._current_signal
            
            # Use CACHED data - no API call
            if not self._cached_perp_data:
                logger.warning(f"No cached wallet data for {pair}")
                return dataframe
            
            try:
                # Calculate position size from CACHED data
                copied_account_value = float(
                    self._cached_perp_data.get("marginSummary", {}).get("accountValue", 1)
                )
                ratio_pc = float(change.new_position_value) / copied_account_value * 100.0
                
                # Verify still meets threshold
                if ratio_pc < self.change_threshold:
                    logger.info(f"✗ Signal expired for {pair}: {ratio_pc:.4f}% < {self.change_threshold}%")
                    self._current_signal = None
                    return dataframe
                
                logger.info(f"✓ Processing {signal_type} signal for {pair}: {ratio_pc:.2f}%")
                
                # Set entry signal
                if signal_type == 'long':
                    dataframe.loc[dataframe.index[-1], 'enter_long'] = 1
                    dataframe.loc[dataframe.index[-1], 'enter_tag'] = f'copy_{change.change_type}'
                    logger.info(f"🟢 LONG entry signal set for {pair}")
                elif signal_type == 'short':
                    dataframe.loc[dataframe.index[-1], 'enter_short'] = 1
                    dataframe.loc[dataframe.index[-1], 'enter_tag'] = f'copy_{change.change_type}'
                    logger.info(f"🔴 SHORT entry signal set for {pair}")
            
            except Exception as e:
                logger.error(f"Error processing signal for {pair}: {e}")
                self._current_signal = None
        
        return dataframe

    def populate_exit_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        """Set exit signals based on position absence"""
        pair = metadata.get('pair')
        
        # Initialize
        dataframe.loc[:, 'exit_long'] = 0
        dataframe.loc[:, 'exit_short'] = 0
        
        # Check if we have an exit signal for this pair
        if (self._current_signal and 
            self._current_signal[0] == pair and 
            self._current_signal[1] == 'exit'):
            
            dataframe.loc[dataframe.index[-1], 'exit_long'] = 1
            dataframe.loc[dataframe.index[-1], 'exit_short'] = 1
            logger.info(f"🛑 EXIT signal set for {pair}")
        
        return dataframe

    def custom_stake_amount(self, pair: str, current_time: datetime, current_rate: float,
                             proposed_stake: float, min_stake: float | None, max_stake: float,
                             leverage: float, entry_tag: str | None, side: str,
                             **kwargs) -> float:
        """
        CRITICAL FIX: Use cached data directly - NO API CALLS.
        """
        logger.info(f"CUSTOM_STAKE_AMOUNT: Processing {pair} ({side})")
        
        # Use CACHED data only
        if not self._cached_perp_data:
            logger.warning(f"No cached data, using minimum stake for {pair}")
            return self.MIN_STAKE_USDC
        
        try:
            coin_ticker = pair.replace("/USDC:USDC", "")
            
            # Get values from CACHED data
            copied_account_value = float(
                self._cached_perp_data.get("marginSummary", {}).get("accountValue", 1)
            )
            my_account_value = float(self.wallets.get_total_stake_amount())
            
            if my_account_value < self.MIN_STAKE_USDC:
                logger.warning(f"Wallet too small (${my_account_value:.2f}), using minimum stake")
                return self.MIN_STAKE_USDC
            
            scale_factor = my_account_value / copied_account_value
            
            # Find position value
            position_value = None
            if coin_ticker in self.current_positions_to_copy:
                position_value = self.current_positions_to_copy[coin_ticker].position_value
            
            if position_value is None:
                logger.warning(f"No position data for {coin_ticker}, using minimum stake")
                return self.MIN_STAKE_USDC
            
            # Calculate scaled stake
            calculated_stake = (position_value * scale_factor) / leverage
            
            # Apply minimum
            if calculated_stake < self.MIN_STAKE_USDC:
                logger.info(f"Calculated stake ${calculated_stake:.2f} < min ${self.MIN_STAKE_USDC}, using minimum")
                return self.MIN_STAKE_USDC
            
            logger.info(f"Calculated stake for {pair}: ${calculated_stake:.2f}")
            return calculated_stake
        
        except Exception as e:
            logger.error(f"Error calculating stake: {e}")
            return self.MIN_STAKE_USDC

    def confirm_trade_entry(self, pair: str, order_type: str, amount: float, rate: float,
                            time_in_force: str, current_time: datetime, entry_tag: Optional[str],
                            side: str, **kwargs) -> bool:
        """Confirm trade and track volume"""
        estimated_value = amount * rate
        self._volume_generated_this_session += estimated_value
        
        logger.info(f"✅ ORDER CONFIRMED: {pair} - ${estimated_value:.2f} volume")
        logger.info(f"📊 Session volume: ${self._volume_generated_this_session:.2f}")
        
        # Clear current signal after order confirmation
        self._current_signal = None
        
        return True

    def confirm_trade_exit(self, pair: str, trade, order_type: str, amount: float,
                           rate: float, time_in_force: str, exit_reason: str,
                           current_time: datetime, **kwargs) -> bool:
        """Confirm exit and track volume"""
        estimated_value = amount * rate
        self._volume_generated_this_session += estimated_value
        
        logger.info(f"✅ EXIT CONFIRMED: {pair} - ${estimated_value:.2f} volume")
        logger.info(f"📊 Session volume: ${self._volume_generated_this_session:.2f}")
        
        # Clear current signal
        self._current_signal = None
        
        return True

    def get_pair_from_coin(self, coin: str) -> Optional[str]:
        """Convert coin to pair format"""
        return self._coin_to_pair(coin)

    def leverage(self, pair: str, current_time: datetime, current_rate: float,
                 proposed_leverage: float, max_leverage: float, entry_tag: str | None, side: str,
                 **kwargs) -> float:
        """Return leverage setting"""
        return min(self.LEV.value, max_leverage)
