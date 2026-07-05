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

1. A WebSocket `<symbol>@depth@100ms` is opened.
2. A REST snapshot (`/api/v3/depth`) with `lastUpdateId` is downloaded.
3. Diffs with `u <= lastUpdateId` (stale) are discarded.
4. The first valid diff must satisfy `U <= lastUpdateId + 1 <= u`.
5. Each diff updates levels: quantity `0` removes the level.
6. If the sequence breaks → **resync**: a new REST snapshot and buffer restart.

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
- [ ] JSONL export of book metrics for offline backtesting
- [ ] Drift metrics between local book and ticker mid

---

## License

MIT
