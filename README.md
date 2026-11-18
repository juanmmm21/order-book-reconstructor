# order-book-reconstructor

**In-memory L2** reconstructor that maintains ordered bids and asks in real time from exchange depth streams. Second module of the [quant-core-infra](https://github.com/juanmmm21/quant-core-infra) ecosystem.

Repository: [github.com/juanmmm21/order-book-reconstructor](https://github.com/juanmmm21/order-book-reconstructor)

---

## What it is and what problem it solves

An L2 order book is the list of prices and quantities available at each buy and sell level. Exchanges don't send the full book with every message: they send **incremental updates** (diffs) that must be applied on top of an initial snapshot. If you miss a message or the order is wrong, the local book stops reflecting reality.

This module implements that reconstruction with:

- Low-latency sorted structures (`bisect` over lists)
- Sequence validation (Binance `U` / `u`)
- Automatic REST resynchronization when gaps occur
- Normalized `OrderBookView` with best bid/ask, spread, and mid price

---

## Role in quant-core-infra

```text
websocket-feed-handler (trades)     order-book-reconstructor (depth L2)
         │                                      │
         └──────────► market-data-lakehouse ◄────┘
                              │
                    alpha-signal-generator (imbalance)
                              │
                    market-condition-simulator (slippage)
```

Complements `websocket-feed-handler`: that module ingests **trades**; this one ingests **depth** and exposes liquidity for strategies and slippage simulation.

---

## Objective

Demonstrates:

- Classic order book structures with O(log n) insertion
- Real-time L2 reconstruction from diffs
- Binance sequence validation (`U <= lastUpdateId + 1 <= u`)
- REST bootstrap for correct synchronization
- Decoupled per-exchange adapters

---

## How it works

### Binance (diff depth)

`DepthSynchronizer` (`sync.py`) is a pure state machine shared by the live service and the
offline replay:

1. A WebSocket `<symbol>@depth@100ms` is opened **first** and diffs are **buffered**.
2. A REST snapshot (`/api/v3/depth`) with `lastUpdateId` is downloaded while the buffer fills.
3. Buffered diffs with `u <= lastUpdateId` (stale) are discarded.
4. The first applied diff must satisfy `U <= lastUpdateId + 1 <= u` (it may straddle the snapshot,
   also when the snapshot is newer than everything buffered).
5. From then on every diff must start **exactly** at the previous `u + 1`; quantity `0` removes
   the level.
6. Any other `U` is a **gap**: missing ids are counted, the book goes back to step 1 (buffer +
   new snapshot) and later diffs are kept, not dropped. A snapshot older than the buffer is
   rejected and another one is requested.

Until v0.1 the live service fetched the snapshot *before* opening the socket, so the diffs in
between were lost and the first update always looked like a gap; that is fixed.

### Coinbase (level2)

1. Subscribes to the `level2` channel.
2. The first message is a complete `snapshot`.
3. Subsequent `l2update` messages modify individual levels.
4. No REST required; synchronization is via WebSocket snapshot.

In both cases the result is an `OrderBookView` with the N configured top levels.

---

## Supported exchanges

| Exchange | Stream | Sync strategy | Symbol |
|----------|--------|-------------------|---------|
| **Binance** | `@depth@100ms` | REST snapshot + diffs | `BTCUSDT` |
| **Coinbase** | `level2` | WS snapshot + `l2update` | `BTC-USD` |

Binance protocol: [Binance Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

---

## Architecture

```text
┌─────────────────────┐
│   CLI / Consumer    │
└──────────┬──────────┘
           │ OrderBookView
           ▼
┌─────────────────────┐
│ LiveOrderBookService│  reconnect + resync on gap
└──────────┬──────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
┌──────────┐  ┌─────────────────────┐
│ Snapshot │  │ OrderBookReconstructor │
│ Provider │  │  ├─ SortedBookSide (bids ↓)
└──────────┘  │  └─ SortedBookSide (asks ↑)
              └─────────────────────┘
```

### Components

| Module | Responsibility |
|--------|-----------------|
| `book_side.py` | Sorted side of the book using `bisect` |
| `reconstructor.py` | Applies snapshots/diffs, validates sequence |
| `snapshot.py` | Binance REST fetch (`httpx`) |
| `feed_service.py` | Async WS consumer with resync logic |
| `exchanges/` | Per-exchange parsers and adapters |

### Technical decisions

- **Decimal** for all prices and quantities
- **Bids descending, asks ascending** → best price in O(1)
- **Auto-resync** on Binance sequence gaps
- **Reconnection** with the same backoff policy as the feed handler

---

## Offline replay and verification

`order-book-reconstructor replay` runs the same synchroniser over a recording (JSONL from
`websocket-feed-handler --channels trade depth`, or `market-data-lakehouse export-depth`) and:

- reports applied / stale diffs, gaps, missing update ids, resyncs and rejected snapshots;
- **verifies** the book against every REST snapshot in the recording: just before applying the
  diff whose `[U, u]` contains the snapshot's `lastUpdateId`, every price level that diff does
  not touch must match the snapshot exactly (top `--verify-depth` levels per side);
- writes a validated stream (`--output`) for `market-condition-simulator`: `book_snapshot` after
  each (re)sync, `book_diff` per applied diff, `book_gap` on breaks and trades tagged
  `book_synced`;
- `--drop-diff-every N` deletes diffs on purpose to exercise resynchronisation on real data.

```bash
order-book-reconstructor replay --input depth.jsonl --symbol BTCUSDT --output book.jsonl
```

Measured on 2026-11-17, BTCUSDT from `data-stream.binance.vision` (6.5 min: 3,856 diffs,
10,070 trades, 2 REST snapshots): 3,462 diffs applied, 394 stale (pre-snapshot), 0 gaps;
739/739 verified levels matched. With `--drop-diff-every 500`: 2 gaps detected and 2 resyncs.
Larger runs are in the quant-terminal-web PR.

### Level updates in O(log n)

`SortedBookSide` used to rebuild the whole list of sort keys on every insert/remove. The keys now
live in a list next to the prices, so a lookup is one `bisect`. On a 4-vCPU Linux x86_64
container, 20,000 random inserts + 10,000 removes on one side went from **38.97 s to 0.37 s**.

---

## Data model: `OrderBookView`

| Field | Description |
|-------|-------------|
| `bids` / `asks` | Ordered `PriceLevel(price, quantity)` tuples |
| `best_bid` / `best_ask` | Best price on each side |
| `spread` | `best_ask - best_bid` |
| `mid_price` | Midpoint of the spread |
| `last_update_id` | Sequence ID (Binance) |
| `event_time` | Timestamp of the last applied event |

Derived metrics (for `alpha-signal-generator` and `market-condition-simulator`):

```json
{
  "symbol": "BTCUSDT",
  "best_bid": "100.4",
  "best_ask": "100.6",
  "spread": "0.2",
  "mid_price": "100.5",
  "bid_depth": "12.5",
  "ask_depth": "8.0",
  "imbalance": 0.22,
  "event_time": "2024-01-01T12:00:00Z"
}
```

`imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)`

---

## Optional Rust accelerator (v0.2.0)

`order_book_reconstructor.fast.FastOrderBook` applies snapshots and diffs with the same
sequence rules as `DepthSynchronizer` on string levels; with the accelerator the levels live in a
Rust `BTreeMap` keyed by `rust_decimal` (numeric keys: `"100.0"` and `"100.00"` are one level).

`rust/` holds a PyO3 extension built with maturin. It is **optional**: without it the package
uses the pure-Python implementation of the same functions (`fast.py`), and
`QUANT_DISABLE_RUST=1` forces Python even when it is installed.

- **Identical results**: both implementations accept the same grammar (decimal strings
  `^\d{1,15}(\.\d{1,12})?$`, 64-bit integer ids) and fail the same way. Hypothesis property tests
  (`tests/test_fast_parity.py`) feed both random valid and malformed inputs and require the same
  output or the same `ValueError`, and compare them with the original `Decimal` implementation.
- **Strings end to end**: decimal fields stay as the exchange's text until the caller needs a
  `Decimal`, which is where most of the Python time went.
- **Wheels**: `.github/workflows/wheels.yml` builds abi3 wheels (Python ≥ 3.11) for Linux x86_64
  (manylinux) and macOS arm64, smoke-tests each one against the Python fallback and uploads them
  as artifacts. `ci.yml` runs the whole suite with and without the accelerator, plus
  `cargo fmt`, `clippy -D warnings` and `cargo test`.

```bash
pip install ./rust            # needs a Rust toolchain; or install a wheel from the CI artifacts
python -c "import order_book_reconstructor.fast as f; print(f.BACKEND)"   # rust | python
```

Measured end to end in quant-terminal-web (`scripts/latency_bench.py`, 25,589 recorded BTCUSDT
depth diffs, 2026-11-17, 4-vCPU Linux x86_64 container, Python 3.12): tick-to-decision p50
131 µs → 15.5 µs and p99 3.70 ms → 166 µs versus the v0.1 Decimal path, with the same decision
on every message.

---

## Requirements

- Python **3.11+**

---

## Installation

```bash
cd order-book-reconstructor
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## CLI usage

```bash
# Binance
order-book-reconstructor --exchange binance --symbols BTCUSDT --top-levels 10

# Coinbase
order-book-reconstructor --exchange coinbase --symbols BTC-USD --top-levels 10
```

Expected output:

```text
book exchange=binance symbol=BTCUSDT bid=67234.50 ask=67234.51 spread=0.01 levels=10/10
```

---

## Programmatic usage

```python
import asyncio
import logging

from order_book_reconstructor import BookFeedConfig, ExchangeId, LiveOrderBookService, OrderBookView

logging.basicConfig(level=logging.INFO)


async def on_book_update(view: OrderBookView) -> None:
    print(view.symbol, view.best_bid, view.best_ask, view.spread)


async def main() -> None:
    config = BookFeedConfig(
        exchange=ExchangeId.BINANCE,
        symbols=("BTCUSDT",),
        top_levels=5,
    )
    service = LiveOrderBookService(config)
    await service.run(on_book_update=on_book_update)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Development

```bash
pytest -q
ruff check src tests
mypy src
```

---

## Roadmap

- [ ] Support for `@1000ms` depth and dynamic configuration
- [x] Offline replay with verification against REST snapshots (v0.2.0)
- [ ] JSONL export of book metrics for offline backtesting
- [ ] Drift metrics between local book and ticker mid

---

## License

MIT
