"""Lender streamer factory — a fresh, isolated streamer per ``with`` block.

The streamer classes open a fresh WebSocket on every call and are not
thread-safe: ``self.ws`` is shared mutable state that concurrent
``connect()`` calls overwrite while a sibling thread is mid-``recv``.

``StreamerFactory`` is a *lender*: each ``candles()`` / ``forecast()`` call
returns a context manager that yields a **fresh isolated** streamer and closes
it on block exit. That gives two guarantees for concurrent consumers:

1. **Fresh instance per call** — every ``with`` block gets its own streamer, so
   ``self.ws`` is never shared across calls.
2. **Always closes on exit** — the streamer is closed when the block exits,
   even on an exception or when a realtime generator is abandoned mid-iteration.

The streamer classes are themselves leak-free (``connect()`` self-cleans on
handshake failure; each method body owns its socket's ``close()``). The
factory's block-exit close is the defense against generator abandonment — the
SDK pins the socket to a receiver thread (``enable_multithread=True``),
deferring GC, so prompt close at block exit matters. On the sync happy path
the factory close is an intentional idempotent no-op (the socket is already
``None``).

Concurrency bounding is deliberately **not** provided here — it is the
caller's capacity policy (an async caller uses ``asyncio.Semaphore``; a sync
caller wraps a lender in ``threading.Semaphore``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from tv_scraper.streaming.candle_streamer import CandleStreamer
from tv_scraper.streaming.forecast_streamer import ForecastStreamer

if TYPE_CHECKING:
    from types import TracebackType


class _Streamable(Protocol):
    """Structural type for anything the factory can lend."""

    def close(self) -> None: ...

    # CandleStreamer/ForecastStreamer supply get_candles/get_forecast and
    # stream_realtime_price; only close() is needed for the lender contract.


class _Lender:
    """A context manager that yields a fresh streamer and closes it on exit.

    Acquires nothing. Builds the streamer on ``__enter__`` so an exception
    before entry can't leak, and closes it in ``__exit__`` — the single place
    the factory touches the streamer's lifecycle.
    """

    __slots__ = ("_kwargs", "_streamer", "_streamer_cls")

    def __init__(self, streamer_cls: type[_Streamable], kwargs: dict[str, Any]) -> None:
        self._streamer_cls = streamer_cls
        self._kwargs = kwargs
        self._streamer: _Streamable | None = None

    def __enter__(self) -> _Streamable:
        self._streamer = self._streamer_cls(**self._kwargs)
        return self._streamer

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._streamer is not None:
            self._streamer.close()


class StreamerFactory:
    """Lender factory: ``candles()`` / ``forecast()`` return context managers
    that yield a fresh isolated streamer and close it on block exit.

    Args:
        export: Export format — ``"json"`` or ``"csv"``.
            If ``None`` (default), results are not exported.
        cookie: TradingView session cookies for session authentication.
            If not provided, unauthenticated access is used.

    Example::

        factory = StreamerFactory(cookie="...")
        with factory.candles() as s:
            result = s.get_candles(exchange="BINANCE", symbol="BTCUSDT",
                                   timeframe="1h", numb_candles=100)

    For realtime streams, the same lender guarantees prompt close when the
    generator is abandoned::

        with factory.candles() as s:
            for tick in s.stream_realtime_price(exchange="BINANCE",
                                                symbol="BTCUSDT"):
                ...
    """

    def __init__(
        self,
        export: str | None = None,
        cookie: str | None = None,
    ) -> None:
        self._kwargs: dict[str, Any] = {"export": export, "cookie": cookie}
        self._candle_streamer_cls: type[_Streamable] = CandleStreamer
        self._forecast_streamer_cls: type[_Streamable] = ForecastStreamer

    def candles(self) -> _Lender:
        """Return a context manager yielding a fresh ``CandleStreamer``.

        The streamer is closed on block exit (normal, exception, or generator
        abandonment).
        """
        return _Lender(self._candle_streamer_cls, self._kwargs)

    def forecast(self) -> _Lender:
        """Return a context manager yielding a fresh ``ForecastStreamer``.

        The streamer is closed on block exit.
        """
        return _Lender(self._forecast_streamer_cls, self._kwargs)
