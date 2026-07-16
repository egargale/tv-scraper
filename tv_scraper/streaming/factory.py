"""Thread-safe streamer factory — fresh instance per call.

The SDK's ``CandleStreamer`` and ``ForecastStreamer`` open a fresh WebSocket on
every call and are **not thread-safe**: ``self.ws`` is shared mutable state that
concurrent ``connect()`` calls overwrite while a sibling thread is mid-``recv``.

``StreamerFactory`` guarantees safety with two invariants:

1. **Fresh instance per call** — every ``get_candles`` / ``get_forecast`` builds
   a new streamer, so ``self.ws`` is never shared across calls.
2. **Always closes** — a ``finally`` block calls ``streamer.close()`` even when
   an exception escapes before ``receive_packets()`` runs (the SDK only closes
   inside that generator's ``finally``).

Optional ``max_concurrency`` bounds in-flight WebSockets (== file descriptors)
via a ``threading.BoundedSemaphore`` held across build→fetch→close.
"""

from __future__ import annotations

import threading
from collections.abc import Generator
from typing import Any

from tv_scraper.core.validation_data import (
    EXCHANGE_LITERAL,
    TIMEFRAME_LITERAL,
)
from tv_scraper.streaming.candle_streamer import CandleStreamer
from tv_scraper.streaming.forecast_streamer import ForecastStreamer
from tv_scraper.streaming.utils import fetch_available_indicators


class StreamerFactory:
    """Thread-safe factory creating a fresh streamer instance per call.

    Args:
        export: Export format — ``"json"`` or ``"csv"``.
            If ``None`` (default), results are not exported.
        cookie: TradingView session cookies for session authentication.
        max_concurrency: Upper bound on concurrent in-flight calls.
            ``None`` (default) means unbounded; an ``int`` caps in-flight
            WebSocket connections across every caller.

    Example::

        factory = StreamerFactory(max_concurrency=4)
        result = factory.get_candles(exchange="BINANCE", symbol="BTCUSDT",
                                     timeframe="1h", numb_candles=100)
    """

    def __init__(
        self,
        export: str | None = None,
        cookie: str | None = None,
        max_concurrency: int | None = None,
    ) -> None:
        self._export = export
        self._cookie = cookie
        self._semaphore: threading.BoundedSemaphore | None
        if max_concurrency is not None:
            self._semaphore = threading.BoundedSemaphore(max_concurrency)
        else:
            self._semaphore = None

    # -- candles -----------------------------------------------------------

    def get_candles(
        self,
        exchange: EXCHANGE_LITERAL,
        symbol: str,
        timeframe: TIMEFRAME_LITERAL = "1m",
        numb_candles: int = 10,
        indicators: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Fetch OHLCV candles — thread-safe, fresh connection per call.

        Args:
            exchange: Exchange name (e.g. ``"BINANCE"``).
            symbol: Symbol name (e.g. ``"BTCUSDT"``).
            timeframe: Candle timeframe (e.g. ``"1m"``, ``"1h"``, ``"1d"``).
            numb_candles: Number of candles to retrieve (1-5000).
            indicators: Optional list of ``(script_id, script_version)`` tuples.

        Returns:
            Standardized response envelope with
            ``{"status", "data": {"ohlcv": [...], "indicators": {...}},
              "metadata", "warnings", "error"}``.
        """
        acquired = self._acquire()
        streamer = CandleStreamer(export=self._export, cookie=self._cookie)
        try:
            return streamer.get_candles(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                numb_candles=numb_candles,
                indicators=indicators,
            )
        finally:
            streamer.close()
            if acquired:
                self._release()

    # -- forecast ----------------------------------------------------------

    def get_forecast(
        self,
        exchange: EXCHANGE_LITERAL,
        symbol: str,
    ) -> dict[str, Any]:
        """Fetch forecast data — thread-safe, fresh connection per call.

        Args:
            exchange: Exchange name (e.g. ``"NYSE"``).
            symbol: Symbol name (e.g. ``"AAPL"``).

        Returns:
            Standardized response envelope with
            ``{"status", "data", "metadata", "warnings", "error"}``.
        """
        acquired = self._acquire()
        streamer = ForecastStreamer(export=self._export, cookie=self._cookie)
        try:
            return streamer.get_forecast(exchange=exchange, symbol=symbol)
        finally:
            streamer.close()
            if acquired:
                self._release()

    # -- realtime ----------------------------------------------------------

    def stream_realtime_price(
        self,
        exchange: EXCHANGE_LITERAL,
        symbol: str,
        indicators: list[tuple[str, str]] | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Yield realtime price updates — holds a permit for the generator's lifetime.

        Args:
            exchange: Exchange name.
            symbol: Symbol name.
            indicators: Optional list of ``(script_id, script_version)`` tuples.

        Yields:
            Normalised price update dicts.
        """
        acquired = self._acquire()
        streamer = CandleStreamer(export=self._export, cookie=self._cookie)
        try:
            yield from streamer.stream_realtime_price(
                exchange=exchange,
                symbol=symbol,
                indicators=indicators,
            )
        finally:
            streamer.close()
            if acquired:
                self._release()

    # -- indicators --------------------------------------------------------

    @staticmethod
    def get_available_indicators() -> dict[str, Any]:
        """Fetch available built-in indicators (no connection needed)."""
        return fetch_available_indicators()

    # -- internal ----------------------------------------------------------

    def _acquire(self) -> bool:
        """Acquire the semaphore if configured. Returns True if acquired."""
        if self._semaphore is not None:
            self._semaphore.acquire()
            return True
        return False

    def _release(self) -> None:
        """Release the semaphore if configured."""
        if self._semaphore is not None:
            self._semaphore.release()
