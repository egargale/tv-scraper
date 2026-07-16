"""Unit tests for StreamerFactory.

The factory is a lender / context-manager: ``candles()`` and ``forecast()``
each yield a fresh, isolated streamer and close it on block exit. These tests
inject a fake streamer constructor (no live sockets) and assert on
object identity/freshness and ``close()`` calls — external orchestration
behavior, not internals.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pytest

from tv_scraper.streaming.factory import StreamerFactory


class _FakeStreamer:
    """Minimal stand-in for a streamer. Records construction and close().

    Holds a per-instance counter so tests can assert freshness (a new instance
    per ``with`` block) and records whether ``close()`` was called.
    """

    _counter = 0
    closed = False

    def __init__(self, **_: Any) -> None:
        type(self)._counter += 1
        self.id = type(self)._counter
        self.closed = False

    def stream_realtime_price(self, **_: Any) -> Generator[dict[str, Any], None, None]:
        """Yield one packet then stop — lets abandonment tests drive the gen."""
        yield {"id": self.id, "tick": 0}

    def close(self) -> None:
        self.closed = True


def _fake_candle_streamer_factory():
    """Return a fresh fake CandleStreamer class and a reset helper."""
    _FakeStreamer._counter = 0

    class FakeCandle(_FakeStreamer):
        pass

    return FakeCandle


class TestStreamerFactoryInit:
    def test_accepts_export_and_cookie(self):
        factory = StreamerFactory(export="csv", cookie="nomnom")
        # No public assertion of internals; construction must not raise and the
        # lenders must exist.
        assert callable(getattr(factory, "candles", None))
        assert callable(getattr(factory, "forecast", None))

    def test_no_max_concurrency_param(self):
        """The in-library semaphore / max_concurrency was removed (caller owns
        capacity policy). Passing it must be rejected, not silently ignored."""
        with pytest.raises(TypeError):
            StreamerFactory(max_concurrency=4)  # type: ignore[call-arg]

    def test_no_mirror_methods(self):
        """Mirror methods were removed — the surface is lenders only."""
        factory = StreamerFactory()
        for removed in (
            "get_candles",
            "get_forecast",
            "stream_realtime_price",
            "get_available_indicators",
            "_acquire",
            "_release",
        ):
            assert not hasattr(factory, removed), f"factory still exposes {removed}"


class TestLenderFreshness:
    """Part 2 invariant: a fresh, isolated streamer per call (kills self.ws race)."""

    def test_candles_yields_fresh_instance_per_call(self):
        factory = StreamerFactory()
        factory._candle_streamer_cls = _fake_candle_streamer_factory()

        with factory.candles() as s1:
            first = s1.id
        with factory.candles() as s2:
            second = s2.id

        assert first != second

    def test_two_concurrent_candles_blocks_yield_distinct_objects(self):
        factory = StreamerFactory()
        factory._candle_streamer_cls = _fake_candle_streamer_factory()

        with factory.candles() as s1:
            with factory.candles() as s2:
                assert s1 is not s2


class TestLenderClosesOnExit:
    """Part 2 invariant: the factory closes the streamer on block exit."""

    def test_candles_closed_on_normal_exit(self):
        factory = StreamerFactory()
        factory._candle_streamer_cls = _fake_candle_streamer_factory()

        with factory.candles() as s:
            assert not s.closed
        assert s.closed

    def test_candles_closed_when_exception_escapes(self):
        factory = StreamerFactory()
        factory._candle_streamer_cls = _fake_candle_streamer_factory()

        with pytest.raises(ValueError, match="boom"):
            with factory.candles() as s:
                raise ValueError("boom")
        assert s.closed

    def test_forecast_closed_on_exit(self):
        factory = StreamerFactory()
        factory._forecast_streamer_cls = _fake_candle_streamer_factory()

        with factory.forecast() as s:
            assert not s.closed
        assert s.closed


class TestLenderClosesOnGeneratorAbandonment:
    """Part 2 invariant: a realtime stream abandoned mid-iteration is closed
    promptly at block exit (before GC)."""

    def test_realtime_closed_when_block_exits_before_generator_exhausts(self):
        factory = StreamerFactory()
        factory._candle_streamer_cls = _fake_candle_streamer_factory()

        with factory.candles() as s:
            gen = s.stream_realtime_price(exchange="BINANCE", symbol="BTCUSDT")
            next(gen)  # one tick, then stop consuming
        # Block exited without exhausting gen — streamer still closed promptly.
        assert s.closed

    def test_realtime_closed_when_exception_during_iteration(self):
        factory = StreamerFactory()
        factory._candle_streamer_cls = _fake_candle_streamer_factory()

        with pytest.raises(RuntimeError, match="boom"):
            with factory.candles() as s:
                gen = s.stream_realtime_price(exchange="BINANCE", symbol="BTCUSDT")
                next(gen)
                raise RuntimeError("boom")
        assert s.closed


class TestStreamerFactoryUsesRealClassesByDefault:
    """Without injection, the factory builds the real streamer classes."""

    def test_default_candle_class_is_candle_streamer(self):
        from tv_scraper.streaming.candle_streamer import CandleStreamer

        factory = StreamerFactory()
        assert factory._candle_streamer_cls is CandleStreamer

    def test_default_forecast_class_is_forecast_streamer(self):
        from tv_scraper.streaming.forecast_streamer import ForecastStreamer

        factory = StreamerFactory()
        assert factory._forecast_streamer_cls is ForecastStreamer

    def test_export_and_cookie_propagate_to_streamer(self):
        """A lender should forward export/cookie to the streamer it builds."""
        factory = StreamerFactory(export="csv", cookie="nomnom")

        built: dict[str, Any] = {}

        class Spy(_FakeStreamer):
            def __init__(self, **kwargs: Any) -> None:
                built.update(kwargs)
                super().__init__(**kwargs)

        factory._candle_streamer_cls = Spy
        with factory.candles():
            pass

        assert built.get("export") == "csv"
        assert built.get("cookie") == "nomnom"


class TestStreamerFactoryPublicSurface:
    def test_lenders_are_context_managers(self):
        factory = StreamerFactory()
        factory._candle_streamer_cls = _fake_candle_streamer_factory()
        cm = factory.candles()
        assert hasattr(cm, "__enter__")
        assert hasattr(cm, "__exit__")
