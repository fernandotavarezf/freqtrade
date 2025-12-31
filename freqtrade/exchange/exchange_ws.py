import asyncio
import logging
import time
from copy import deepcopy
from functools import partial
from threading import Thread

import ccxt

from freqtrade.constants import Config, PairWithTimeframe
from freqtrade.enums.candletype import CandleType
from freqtrade.exceptions import TemporaryError
from freqtrade.exchange.common import retrier
from freqtrade.exchange.exchange import timeframe_to_seconds
from freqtrade.exchange.exchange_types import OHLCVResponse
from freqtrade.util import dt_ts, format_ms_time, format_ms_time_det


logger = logging.getLogger(__name__)


class ExchangeWS:
    def __init__(self, config: Config, ccxt_object: ccxt.Exchange) -> None:
        self.config = config
        self._ccxt_object = ccxt_object
        self._background_tasks: set[asyncio.Task] = set()

        self._klines_watching: set[PairWithTimeframe] = set()
        self._klines_scheduled: set[PairWithTimeframe] = set()
        self.klines_last_refresh: dict[PairWithTimeframe, float] = {}
        self.klines_last_request: dict[PairWithTimeframe, float] = {}
        self._thread = Thread(name="ccxt_ws", target=self._start_forever)
        self._thread.start()
        self.__cleanup_called = False

    def _start_forever(self) -> None:
        self._loop = asyncio.new_event_loop()
        try:
            self._loop.run_forever()
        finally:
            if self._loop.is_running():
                self._loop.stop()

    def cleanup(self) -> None:
        logger.debug("Cleanup called - stopping")
        self._klines_watching.clear()
        for task in self._background_tasks:
            task.cancel()
        if hasattr(self, "_loop") and not self._loop.is_closed():
            self.reset_connections()

            self._loop.call_soon_threadsafe(self._loop.stop)
            time.sleep(0.1)
            if not self._loop.is_closed():
                self._loop.close()

        self._thread.join()
        logger.debug("Stopped")

    def reset_connections(self) -> None:
        """
        Reset all connections - avoids "connection-reset" errors that happen after ~9 days
        """
        if hasattr(self, "_loop") and not self._loop.is_closed():
            logger.info("Resetting WS connections.")
            asyncio.run_coroutine_threadsafe(self._cleanup_async(), loop=self._loop)
            while not self.__cleanup_called:
                time.sleep(0.1)
        self.__cleanup_called = False

    async def _cleanup_async(self) -> None:
        try:
            await self._ccxt_object.close()
            # Clear the cache.
            # Not doing this will cause problems on startup with dynamic pairlists
            self._ccxt_object.ohlcvs.clear()
        except Exception:
            logger.exception("Exception in _cleanup_async")
        finally:
            self.__cleanup_called = True

    def _pop_history(self, paircomb: PairWithTimeframe) -> None:
        """
        Remove history for a pair/timeframe combination from ccxt cache
        """
        self._ccxt_object.ohlcvs.get(paircomb[0], {}).pop(paircomb[1], None)
        self.klines_last_refresh.pop(paircomb, None)

    @retrier(retries=3)
    def ohlcvs(self, pair: str, timeframe: str) -> list[list]:
        """
        Returns a copy of the klines for a pair/timeframe combination
        Note: this will only contain the data received from the websocket
            so the data will build up over time.
        """
        try:
            return deepcopy(self._ccxt_object.ohlcvs.get(pair, {}).get(timeframe, []))
        except RuntimeError as e:
            # Capture runtime errors and retry
            # TemporaryError does not cause backoff - so we're essentially retrying immediately
            raise TemporaryError(f"Error deepcopying: {e}") from e

    def cleanup_expired(self) -> None:
        """
        Remove pairs from watchlist if they've not been requested within
        the last timeframe (+ offset)
        """
        changed = False
        for p in list(self._klines_watching):
            _, timeframe, _ = p
            timeframe_s = timeframe_to_seconds(timeframe)
            last_refresh = self.klines_last_request.get(p, 0)
            if last_refresh > 0 and (dt_ts() - last_refresh) > ((timeframe_s + 20) * 1000):
                logger.info(f"Removing {p} from websocket watchlist.")
                self._klines_watching.discard(p)
                # Pop history to avoid getting stale data
                self._pop_history(p)
                changed = True
        if changed:
            logger.info(f"Removal done: new watch list ({len(self._klines_watching)})")

    async def _schedule_while_true(self) -> None:
        # For the ones we should be watching
        for p in self._klines_watching:
            # Check if they're already scheduled
            if p not in self._klines_scheduled:
                self._klines_scheduled.add(p)
                pair, timeframe, candle_type = p
                task = asyncio.create_task(
                    self._continuously_async_watch_ohlcv(pair, timeframe, candle_type)
                )
                self._background_tasks.add(task)
                task.add_done_callback(
                    partial(
                        self._continuous_stopped,
                        pair=pair,
                        timeframe=timeframe,
                        candle_type=candle_type,
                    )
                )

    async def _unwatch_ohlcv(self, pair: str, timeframe: str, candle_type: CandleType) -> None:
        try:
            await self._ccxt_object.un_watch_ohlcv_for_symbols([[pair, timeframe]])
        except ccxt.NotSupported as e:
            logger.debug("un_watch_ohlcv_for_symbols not supported: %s", e)
            pass
        except Exception:
            logger.exception("Exception in _unwatch_ohlcv")

    def _continuous_stopped(
        self, task: asyncio.Task, pair: str, timeframe: str, candle_type: CandleType
    ):
        self._background_tasks.discard(task)
        result = "done"
        if task.cancelled():
            result = "cancelled"
        else:
            if (result1 := task.result()) is not None:
                result = str(result1)

        logger.info(f"{pair}, {timeframe}, {candle_type} - Task finished - {result}")
        asyncio.run_coroutine_threadsafe(
            self._unwatch_ohlcv(pair, timeframe, candle_type), loop=self._loop
        )

        self._klines_scheduled.discard((pair, timeframe, candle_type))
        self._pop_history((pair, timeframe, candle_type))

    async def _continuously_async_watch_ohlcv(
        self, pair: str, timeframe: str, candle_type: CandleType
    ) -> None:
        retry_count = 0
        max_retries = 3
        retry_delay = 5  # seconds
        
        try:
            while (pair, timeframe, candle_type) in self._klines_watching:
                start = dt_ts()
                try:
                    data = await self._ccxt_object.watch_ohlcv(pair, timeframe)
                    self.klines_last_refresh[(pair, timeframe, candle_type)] = dt_ts()
                    logger.debug(
                        f"watch done {pair}, {timeframe}, data {len(data)} "
                        f"in {(dt_ts() - start) / 1000:.3f}s"
                    )
                    # Reset retry count on successful watch
                    retry_count = 0
                    
                except ccxt.UnsubscribeError as e:
                    logger.warning(
                        f"WebSocket unsubscribed by exchange for {pair}, {timeframe}: {e}. "
                        f"Retry attempt {retry_count + 1}/{max_retries}"
                    )
                    retry_count += 1
                    if retry_count <= max_retries:
                        logger.info(f"Waiting {retry_delay} seconds before retrying WebSocket for {pair}, {timeframe}")
                        await asyncio.sleep(retry_delay)
                        # Try to resubscribe by removing and re-adding to watchlist
                        self._klines_watching.discard((pair, timeframe, candle_type))
                        await asyncio.sleep(1)
                        self._klines_watching.add((pair, timeframe, candle_type))
                        continue
                    else:
                        logger.error(
                            f"Max retries exceeded for WebSocket {pair}, {timeframe}. "
                            "Falling back to REST API."
                        )
                        break
                        
                except ccxt.ExchangeClosedByUser:
                    logger.debug(f"Exchange connection closed by user for {pair}, {timeframe}")
                    break
                    
                except ccxt.NetworkError as e:
                    logger.warning(
                        f"Network error in WebSocket for {pair}, {timeframe}: {e}. "
                        f"Retry attempt {retry_count + 1}/{max_retries}"
                    )
                    retry_count += 1
                    if retry_count <= max_retries:
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        logger.error(
                            f"Max network error retries exceeded for WebSocket {pair}, {timeframe}. "
                            "Falling back to REST API."
                        )
                        break
                        
                except ccxt.BaseError as e:
                    logger.exception(
                        f"Exception in continuously_async_watch_ohlcv for {pair}, {timeframe}: {e}"
                    )
                    break
                    
                except Exception as e:
                    logger.exception(
                        f"Unexpected exception in continuously_async_watch_ohlcv for {pair}, {timeframe}: {e}"
                    )
                    break
                    
        except asyncio.CancelledError:
            logger.debug(f"WebSocket task cancelled for {pair}, {timeframe}")
            raise
        finally:
            self._klines_watching.discard((pair, timeframe, candle_type))
            logger.debug(f"WebSocket monitoring ended for {pair}, {timeframe}")

    def schedule_ohlcv(self, pair: str, timeframe: str, candle_type: CandleType) -> None:
        """
        Schedule a pair/timeframe combination to be watched
        """
        self._klines_watching.add((pair, timeframe, candle_type))
        self.klines_last_request[(pair, timeframe, candle_type)] = dt_ts()
        # asyncio.run_coroutine_threadsafe(self.schedule_schedule(), loop=self._loop)
        asyncio.run_coroutine_threadsafe(self._schedule_while_true(), loop=self._loop)
        self.cleanup_expired()

    async def get_ohlcv(
        self,
        pair: str,
        timeframe: str,
        candle_type: CandleType,
        candle_ts: int,
    ) -> OHLCVResponse:
        """
        Returns cached klines from ccxt's "watch" cache.
        :param candle_ts: timestamp of the end-time of the candle we expect.
        """
        # Deepcopy the response - as it might be modified in the background as new messages arrive
        candles = self.ohlcvs(pair, timeframe)
        refresh_date = self.klines_last_refresh[(pair, timeframe, candle_type)]
        received_ts = candles[-1][0] if candles else 0
        drop_hint = received_ts >= candle_ts
        if received_ts > refresh_date:
            logger.warning(
                f"{pair}, {timeframe} - Candle date > last refresh "
                f"({format_ms_time(received_ts)} > {format_ms_time_det(refresh_date)}). "
                "This usually suggests a problem with time synchronization."
            )
        logger.debug(
            f"watch result for {pair}, {timeframe} with length {len(candles)}, "
            f"r_ts={format_ms_time(received_ts)}, "
            f"lref={format_ms_time_det(refresh_date)}, "
            f"candle_ts={format_ms_time(candle_ts)}, {drop_hint=}"
        )
        return pair, timeframe, candle_type, candles, drop_hint
