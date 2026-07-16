# StreamerFactory

`StreamerFactory` is a **lender**: each `candles()` / `forecast()` call returns a context manager that yields a **fresh, isolated** streamer and closes it on block exit. Use it when your code opens **multiple concurrent** streaming connections, or when you want prompt, guaranteed socket cleanup — including when a realtime generator is abandoned mid-iteration.

The streamer classes themselves are [CandleStreamer](candle_streamer.md) and [ForecastStreamer](forecast_streamer.md); the factory lends you fresh instances of each.

## Why use the factory?

The streamer classes open a fresh WebSocket per call and are **not thread-safe** — `self.ws` is shared mutable state that concurrent `connect()` calls can overwrite while a sibling thread is mid-`recv`. The factory sidesteps that race by giving every `with` block its own instance, and guarantees the socket closes on exit even on exception or generator abandonment.

The streamer classes are themselves leak-free (`connect()` self-cleans on handshake failure; each method body owns its socket's `close()`). The factory's block-exit close is the defense against generator abandonment — the SDK pins the socket to a receiver thread (`enable_multithread=True`), deferring GC, so prompt close at block exit matters rather than waiting for collection. On the synchronous happy path the factory close is an intentional idempotent no-op (the socket is already `None`).

See [ADR-0001](https://github.com/egargale/tv-scraper/blob/main/docs/adr/0001-streaming-concurrency-and-lifecycle.md) for the full concurrency and lifecycle design.

## Quick Use

### Candle fetching

```python
from tv_scraper import StreamerFactory

factory = StreamerFactory(cookie="...")

with factory.candles() as s:
    result = s.get_candles(
        exchange="BINANCE",
        symbol="BTCUSDT",
        timeframe="1h",
        numb_candles=25,
    )
```

The output envelope is identical to calling [`CandleStreamer.get_candles(...)`](candle_streamer.md) directly.

### Forecast fetching

```python
with factory.forecast() as s:
    result = s.get_forecast(exchange="NASDAQ", symbol="AAPL")
```

### Realtime prices

The same lender guarantees prompt close when a realtime generator is abandoned — which matters here most, because the socket is pinned to a receiver thread and won't be collected promptly on its own:

```python
indicators = [("STD;RSI", "37.0")]

with factory.candles() as s:
    for tick in s.stream_realtime_price(
        exchange="BINANCE",
        symbol="BTCUSDT",
        indicators=indicators,
    ):
        price = tick["price"]
        rsi = tick["indicators"].get("STD;RSI", {}).get("0")
        print(f"Price: {price}, RSI: {rsi}")
```

!!! warning "Generator Behavior"
    `stream_realtime_price()` returns a raw generator and is **not wrapped with error envelopes**. It raises exceptions directly during iteration (e.g., `ValidationError` on invalid inputs or `RuntimeError` on connection failures). Wrap your iteration in a `try/except` block. Leaving the `with` block closes the socket whether you finish, break, or raise.

## Constructor

| Parameter | Notes |
|-----------|-------|
| `export` | Optional export format — `"json"` or `"csv"`. If `None` (default), results are not exported. Passed through to every lent streamer. |
| `cookie` | TradingView session cookies for session authentication. Required for indicator-based streaming; if not provided, unauthenticated access is used. Passed through to every lent streamer. |

The factory holds these as defaults; every `candles()` / `forecast()` block builds its streamer from them.

## Lender API

| Method | Yields | Closes on exit |
|--------|--------|----------------|
| `factory.candles()` | a fresh `CandleStreamer` | yes — normal exit, exception, or generator abandonment |
| `factory.forecast()` | a fresh `ForecastStreamer` | yes — normal exit, exception, or generator abandonment |

Each call returns an **independent** context manager, so you can nest or interleave them:

```python
with factory.candles() as c, factory.forecast() as f:
    candles = c.get_candles(exchange="BINANCE", symbol="BTCUSDT")
    forecast = f.get_forecast(exchange="BINANCE", symbol="BTCUSDT")
```

Both streamers are built on `__enter__` (so an exception before entry can't leak) and closed in `__exit__`.

## Concurrency bounding is the caller's job

The factory deliberately provides **no** `max_concurrency` or internal semaphore. Bounding how many connections are in flight at once is a capacity policy that belongs to the caller, because the bound depends on how the caller schedules work:

- **Async caller** — bound with `asyncio.Semaphore` around `asyncio.to_thread`:

    ```python
    import asyncio

    sem = asyncio.Semaphore(8)

    async def fetch(symbol: str) -> dict:
        async with sem:
            return await asyncio.to_thread(run_fetch, symbol)

    def run_fetch(symbol: str) -> dict:
        with factory.candles() as s:
            return s.get_candles(exchange="BINANCE", symbol=symbol, numb_candles=25)
    ```

- **Sync caller (threads)** — wrap the lender in a `threading.Semaphore`:

    ```python
    import threading

    sem = threading.Semaphore(8)

    def fetch(symbol: str) -> dict:
        with sem:
            with factory.candles() as s:
                return s.get_candles(exchange="BINANCE", symbol=symbol, numb_candles=25)
    ```

The async caller **cannot** use a synchronous blocking semaphore — its blocking `acquire()` would park executor threads — which is why the library doesn't pick one for you. See [ADR-0001](https://github.com/egargale/tv-scraper/blob/main/docs/adr/0001-streaming-concurrency-and-lifecycle.md) for the rationale.
