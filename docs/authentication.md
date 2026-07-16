# Authentication for TradingView

Two tracks: **unauthenticated** (most features) and **cookie-based** (Pine scripts, custom indicators).

## 1. Unauthenticated Mode (default)

Most HTTP scrapers — fundamentals, technicals, news, ideas, minds, screener, calendar, market movers — **don't need auth**. They hit public TradingView APIs.

WebSocket streaming uses a hardcoded sentinel:

```python
websocket_jwt_token = "unauthorized_user_token"  # BaseStreamer.connect()
```

This works for candle streaming and forecast without any setup.

## 2. Cookie-Based Auth (when you need it)

Required for:

| Feature | Why |
|---|---|
| **Pine Script CRUD** (`Pine` scraper) | Pine facade API requires auth |
| **WebSocket + custom indicators** | `CandleStreamer.get_candles()` with `indicators` param — validates cookie exists |
| **WebSocket JWT (optional)** | Resolves a real JWT from the cookie instead of the sentinel |

## 3. Auth Flow

```
Browser cookie (string)
  ├─→ Pine scraper: cookie → HTTP header → Pine facade API
  └─→ BaseStreamer:  cookie → fetch chart page → regex-extract JWT → WebSocket handshake
```

The JWT extraction lives in `tv_scraper/streaming/auth.py`:

1. HTTP GET `https://www.tradingview.com/chart/?symbol=BINANCE:BTCUSD` with your cookie in the request headers
2. Regex for `eyJ...` pattern (base64url JWT)
3. Decode + verify header has `"alg"` and `"typ"`
4. Cache with expiry check (thread-safe, module-level dict + lock)

Cache is reused across calls within the session (skips re-fetch if more than 60s before expiry).

## 4. How to Set It Up

**Option A — env var (recommended):**

```bash
export TRADINGVIEW_COOKIE="sessionid=abc123; sessionid_sign=xyz..."
```

**Option B — constructor param:**

```python
# HTTP (Pine scrapers)
from tv_scraper.scrapers.scripts.pine import Pine
pine = Pine(cookie="sessionid=abc123; ...")
pine.list_saved_scripts()

# WebSocket (streaming)
from tv_scraper.streaming.streamer import Streamer
streamer = Streamer(cookie="sessionid=abc123; ...")
result = streamer.get_candles(
    exchange="NASDAQ", symbol="AAPL",
    indicators=[("STD;RSI", "1")]
)
```

**Option C — `StreamerFactory` (thread-safe lender pattern):**

```python
from tv_scraper.streaming.factory import StreamerFactory
factory = StreamerFactory(cookie="sessionid=abc123; ...")
with factory.candles() as s:
    result = s.get_candles(exchange="BINANCE", symbol="BTCUSDT")
```

## 5. Getting Your Cookie

1. Open TradingView in browser, log in
2. DevTools → Application → Cookies → `tradingview.com`
3. Copy the full cookie string: `sessionid=...; sessionid_sign=...`

Or export the `cookie` header from a TradingView request (Network tab → request → Request Headers → cookie).

## 6. What Doesn't Need a Cookie

```python
# All of these work without auth:
from tv_scraper.streaming.streamer import Streamer
Streamer().get_candles("NASDAQ", "AAPL")
Streamer().get_forecast("NASDAQ", "AAPL")

from tv_scraper.scrapers.social.news import News
News().get_news(exchange="NASDAQ", symbol="AAPL")

from tv_scraper.scrapers.social.ideas import Ideas
Ideas().get_ideas("NASDAQ", "AAPL")

from tv_scraper.scrapers.market_data.fundamentals import Fundamentals
Fundamentals().get_fundamentals("NASDAQ", "AAPL")

from tv_scraper.scrapers.market_data.technicals import Technicals
Technicals().get_technicals("NASDAQ", "AAPL")

from tv_scraper.scrapers.screening.screener import Screener
Screener().get_screener()
```
