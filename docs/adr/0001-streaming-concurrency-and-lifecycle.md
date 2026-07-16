# ADR-0001: Streaming concurrency safety and socket lifecycle

- **Status:** Accepted
- **Date:** 2026-07-16
- **Issue:** [#2 — Leak-free streamers + lender StreamerFactory](https://github.com/egargale/tv-scraper/issues/2)
- **Companion (consumer repo):** `egargale/agentic-charts` ADR-0035 (build-fresh-per-call + semaphore, which this factory replaces)

## Context

Consumers that open **multiple concurrent** streaming connections hit two defects:

1. **Silent file-descriptor leaks.** A streamer opens a WebSocket inside `connect()`, but the socket is only closed inside the `receive_packets()` generator's `finally`. Two windows leak:
   - **Body window** — anything that raises between `connect()` and the packet loop orphans the socket. The `@catch_errors` decorator swallows both `ValidationError` *and* generic `Exception` into a standard error envelope, so the caller sees a normal error response and never an exception — the leak is silent.
   - **Handshake window** — inside `connect()`, the socket is assigned to `self.ws` *before* the session-handshake messages run; if those raise, `connect()` re-raised without closing the socket it just opened.
2. **A concurrency race.** The streamer classes are not thread-safe — `self.ws` is shared mutable state, so two threads sharing one instance race on `connect()`/`recv()`.

A `StreamerFactory` already existed to give consumers a safe surface, but its shape (mirror methods + a `threading.BoundedSemaphore`) was wrong.

## Decision

Two changes ship together — the leak fix is the **foundation**, the factory is the **deliverable** built on it.

### Part 1 — The streamer classes are leak-free under one invariant

> *If a socket was opened, it is closed — by exactly one owner per call (the method body), with `connect()` cleaning up its own handshake failures so it can't orphan a socket either.*

- `connect()` **self-cleans**: on any failure after `self.ws` is assigned, it closes that socket before re-raising.
- Each public fetch/stream **method body** owns its socket's `close()` via `try/finally: self.close()` — single owner per call. `receive_packets()` becomes a **pure generator** (no self-close). The socket still ends up closed on every path; just by the method body instead of the generator.
- The realtime generator keeps its raw-generator return type and gains its own `try/finally: self.close()`.
- `close()` remains idempotent, so a double close (once by `connect()` on failure, once by the body) is harmless.

### Part 2 — `StreamerFactory` is a lender / context-manager

> *Consumers can open multiple concurrent connections safely — correct under concurrency (fresh instance per call kills the `self.ws` race) and leak-free on every code path (streamer self-closes; factory lender closes on abandonment).*

- Two methods — `candles()` and `forecast()` — each a context manager that acquires nothing, yields a **fresh isolated** streamer, and closes it on block exit.
- Usage: `with factory.candles() as s: result = s.get_candles(...)`, and uniformly for realtime: `with factory.candles() as s: for p in s.stream_realtime_price(...): ...`

## Why-not (the decisions a future review would otherwise re-litigate)

### Why single-owner close, not double?

A double owner (method body **and** `receive_packets()` both closing) reintroduces a distributed-invariant failure: either side can drift, and the invariant is no longer checkable in one place. `receive_packets()` is called from exactly three sites (the two fetch methods and the realtime generator); removing its self-close changes nothing observable. **One owner per call, in the method body.**

### Why does `connect()` self-clean instead of the body wrapping `connect()`?

Every level is self-contained. If the body's `try` wrapped `connect()`, the close invariant would be split across `connect()` (handshake window) and the body (everything after) — re-distributed. With `connect()` cleaning its own handshake failures, the body's `finally` is a harmless idempotent no-op when `connect()` already closed. **`connect()` owns its window; the body owns its window.**

### Why no semaphore / `max_concurrency` in the library?

By the two-adapter rule, an in-library seam needs two real adapters. The one real consumer is async (`asyncio.Semaphore` over `asyncio.to_thread`) and **cannot** use a synchronous blocking `threading.BoundedSemaphore` — its blocking `acquire()` parks executor threads. Removing the in-library semaphore reappears complexity zero times (the consumer already owns its bound). **Capacity policy is the caller's, not the library's.**

### Why keep both the streamer close AND the factory close (complementary)?

Neither is removable in isolation:

- Part 1's method-body close makes the streamer leak-free **without** the factory — load-bearing on the sync fetch path and the generator's subscribe window.
- The factory's block-exit close is load-bearing for **generator abandonment / any orphan at block exit** — the SDK pins the socket to a receiver thread (`enable_multithread=True`), deferring GC, so prompt close at block exit matters rather than waiting for collection.
- On the sync happy path the factory close is an intentional **idempotent no-op** (the socket is already `None`).

Coupling the streamer to factory ownership (to deduplicate the close) would re-distribute the invariant — rejected. **Complementary closes, no coupling.**

### Why lender/context-manager, not mirror methods?

Mirror methods make the factory's surface track the streamer's 1:1 (shallow) and would require a gnarly `try/finally`-around-`yield from` for the realtime generator case. Lending makes the realtime path and the fetch path uniform and keeps the surface to two methods. **Lender, not mirror.**

## Consequences

- **Positive:** Concurrent consumers are safe by construction — fresh connection per call, leak-free on every path. The factory surface is small and uniform. The leak fix also hardens direct (non-factory) use of the streamer classes.
- **Positive:** Removing the in-library semaphore drops a seam with zero real adapters; the consumer's own `asyncio.Semaphore` is the one true bound.
- **Negative / migration:** `StreamerFactory`'s public API changed (mirror methods and `max_concurrency` removed; `candles()`/`forecast()` lenders added). The class name and its import paths are unchanged. Callers using the old mirror methods must migrate to the lender form. The one known consumer (`agentic-charts`) already builds fresh-per-call itself and does not yet use the factory; migrating it is tracked separately (out of scope for #2).
- **Testing:** Two seams — the established `create_connection` patch (streamer lifecycle invariant tests) and an injectable fake streamer constructor (factory orchestration tests). No new low-level seams.

## Out of scope

- Consumer-side cleanup: after Part 1 lands, the consumer repo's `TvScraperClient._close_safely` becomes redundant-but-harmless — a separate consumer-repo change.
- Migrating the consumer to use `StreamerFactory`.
- A sync concurrency bound *inside* the library — the caller wraps the lender in its own `threading.Semaphore`.
- Changing the realtime generator's return type to own the receiver-thread lifecycle.
