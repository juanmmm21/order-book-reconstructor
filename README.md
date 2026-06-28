# order-book-reconstructor

In-memory **L2 order book reconstructor** that maintains sorted bids and asks in real time from exchange depth streams. Second module of the [quant-core-infra](https://github.com/juanmmm21/quant-core-infra) ecosystem, consuming raw WebSocket depth data downstream of `websocket-feed-handler`.

Repository: [github.com/juanmmm21/order-book-reconstructor](https://github.com/juanmmm21/order-book-reconstructor)

---

## Objective

This project demonstrates:

- Classic order book data structures with binary-search insertion (`bisect`)
- Real-time L2 reconstruction from incremental depth updates
- Sequence validation for Binance diff depth (`U` / `u` update IDs)
- REST snapshot bootstrap for correct local book synchronization
- Exchange-specific adapters (Binance, Coinbase level2)

---

## Supported exchanges

| Exchange | Stream / Channel | Sync strategy | Symbol format |
|----------|------------------|---------------|---------------|
| **Binance** | `<symbol>@depth@100ms` | REST `/api/v3/depth` + diff updates | `BTCUSDT` |
| **Coinbase** | `level2` channel | WebSocket `snapshot` + `l2update` | `BTC-USD` |

Binance diff depth protocol: [Binance Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

---

## Architecture

```text
┌─────────────────────┐
│   CLI / Consumer    │
└──────────┬──────────┘
           │ OrderBookView
           ▼
┌─────────────────────┐
│ LiveOrderBookService│  reconnect + resync on sequence gap
└──────────┬──────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
┌──────────┐  ┌─────────────────────┐
│ Snapshot │  │ OrderBookReconstructor │
│ Provider │  │  ├─ SortedBookSide (bids)
└──────────┘  │  └─ SortedBookSide (asks)
              └─────────────────────┘
```

### Core components

| Module | Responsibility |
|--------|----------------|
| `book_side.py` | Sorted bid/ask side with O(log n) insertion via `bisect` |
| `reconstructor.py` | Applies snapshots/updates and builds `OrderBookView` |
| `snapshot.py` | Binance REST snapshot fetcher (`httpx`) |
| `feed_service.py` | Async WebSocket consumer with resync logic |
| `exchanges/` | Depth parsers and adapters per exchange |

### Technical decisions

- **Decimal precision** for all prices and quantities
- **Bids descending, asks ascending** for O(1) best price access
- **Binance sequence tracker** enforces `U <= lastUpdateId + 1 <= u`
- **Auto-resync** on sequence gaps by reloading REST snapshot

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

### Binance

```bash
order-book-reconstructor --exchange binance --symbols BTCUSDT --top-levels 10
```

### Coinbase

```bash
order-book-reconstructor --exchange coinbase --symbols BTC-USD --top-levels 10
```

Example output:

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

## Ecosystem integration

```text
websocket-feed-handler → order-book-reconstructor → market-data-lakehouse
                              │
                              └── sorted L2 book views
```

---

## License

MIT
