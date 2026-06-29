# order-book-reconstructor

Reconstructor **L2 en memoria** que mantiene bids y asks ordenados en tiempo real a partir de streams de profundidad de exchanges. Segundo módulo del ecosistema [quant-core-infra](https://github.com/juanmmm21/quant-core-infra).

Repositorio: [github.com/juanmmm21/order-book-reconstructor](https://github.com/juanmmm21/order-book-reconstructor)

---

## Qué es y qué problema resuelve

Un libro de órdenes L2 es la lista de precios y cantidades disponibles en cada nivel de compra y venta. Los exchanges no envían el libro completo en cada mensaje: envían **actualizaciones incrementales** (diffs) que hay que aplicar sobre un snapshot inicial. Si pierdes un mensaje o el orden es incorrecto, el libro local deja de reflejar la realidad.

Este módulo implementa esa reconstrucción con:

- Estructuras ordenadas de baja latencia (`bisect` sobre listas)
- Validación de secuencia (Binance `U` / `u`)
- Resincronización automática vía REST cuando hay gaps
- Vista normalizada `OrderBookView` con best bid/ask, spread y mid price

---

## Rol en quant-core-infra

```text
websocket-feed-handler (trades)     order-book-reconstructor (depth L2)
         │                                      │
         └──────────► market-data-lakehouse ◄────┘
                              │
                    alpha-signal-generator (imbalance)
                              │
                    market-condition-simulator (slippage)
```

Complementa a `websocket-feed-handler`: ese módulo ingiere **trades**; este ingiere **profundidad** y expone liquidez para estrategias y simulación de slippage.

---

## Objetivo

Demuestra:

- Estructuras clásicas de order book con inserción O(log n)
- Reconstrucción L2 en tiempo real desde diffs
- Validación de secuencia Binance (`U <= lastUpdateId + 1 <= u`)
- Bootstrap REST para sincronización correcta
- Adapters desacoplados por exchange

---

## Cómo funciona

### Binance (diff depth)

1. Se abre WebSocket `<symbol>@depth@100ms`.
2. Se descarga snapshot REST (`/api/v3/depth`) con `lastUpdateId`.
3. Se descartan diffs con `u <= lastUpdateId` (obsoletos).
4. El primer diff válido debe cumplir `U <= lastUpdateId + 1 <= u`.
5. Cada diff actualiza niveles: cantidad `0` elimina el nivel.
6. Si la secuencia se rompe → **resync**: nuevo snapshot REST y reinicio del buffer.

### Coinbase (level2)

1. Se suscribe al canal `level2`.
2. El primer mensaje es un `snapshot` completo.
3. Los `l2update` posteriores modifican niveles individuales.
4. No requiere REST; la sincronización es por snapshot WebSocket.

En ambos casos el resultado es un `OrderBookView` con los N mejores niveles configurados.

---

## Exchanges soportados

| Exchange | Stream | Estrategia de sync | Símbolo |
|----------|--------|-------------------|---------|
| **Binance** | `@depth@100ms` | REST snapshot + diffs | `BTCUSDT` |
| **Coinbase** | `level2` | WS snapshot + `l2update` | `BTC-USD` |

Protocolo Binance: [Binance Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

---

## Arquitectura

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

### Componentes

| Módulo | Responsabilidad |
|--------|-----------------|
| `book_side.py` | Lado del libro ordenado con `bisect` |
| `reconstructor.py` | Aplica snapshots/diffs, valida secuencia |
| `snapshot.py` | Fetch REST Binance (`httpx`) |
| `feed_service.py` | Consumidor WS async con lógica de resync |
| `exchanges/` | Parsers y adapters por exchange |

### Decisiones técnicas

- **Decimal** en todos los precios y cantidades
- **Bids descendente, asks ascendente** → best price en O(1)
- **Auto-resync** en gaps de secuencia Binance
- **Reconexión** con la misma política de backoff que el feed handler

---

## Modelo de datos: `OrderBookView`

| Campo | Descripción |
|-------|-------------|
| `bids` / `asks` | Tuplas `PriceLevel(price, quantity)` ordenadas |
| `best_bid` / `best_ask` | Mejor precio de cada lado |
| `spread` | `best_ask - best_bid` |
| `mid_price` | Punto medio del spread |
| `last_update_id` | ID de secuencia (Binance) |
| `event_time` | Timestamp del último evento aplicado |

Métricas derivadas (para `alpha-signal-generator` y `market-condition-simulator`):

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

## Requisitos

- Python **3.11+**

---

## Instalación

```bash
cd order-book-reconstructor
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## Uso CLI

```bash
# Binance
order-book-reconstructor --exchange binance --symbols BTCUSDT --top-levels 10

# Coinbase
order-book-reconstructor --exchange coinbase --symbols BTC-USD --top-levels 10
```

Salida esperada:

```text
book exchange=binance symbol=BTCUSDT bid=67234.50 ask=67234.51 spread=0.01 levels=10/10
```

---

## Uso programático

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

## Desarrollo

```bash
pytest -q
ruff check src tests
mypy src
```

---

## Roadmap

- [ ] Soporte depth `@1000ms` y configuración dinámica
- [ ] Export JSONL de métricas de libro para backtest offline
- [ ] Métricas de drift entre libro local y ticker mid

---

## Licencia

MIT
