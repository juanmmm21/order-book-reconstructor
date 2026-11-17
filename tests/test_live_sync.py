from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import websockets

from order_book_reconstructor.feed_service import LiveOrderBookService
from order_book_reconstructor.models import (
    BookFeedConfig,
    BookSnapshot,
    ExchangeId,
    OrderBookView,
    PriceLevel,
)


def _diff(first: int, final: int, bid_qty: str) -> str:
    return json.dumps(
        {
            "e": "depthUpdate",
            "E": 1700000000000 + final,
            "s": "BTCUSDT",
            "U": first,
            "u": final,
            "b": [["100.00", bid_qty]],
            "a": [["101.00", "1"]],
        }
    )


class FakeSnapshots:
    """Returns snapshots in order; records when each was requested."""

    def __init__(self, snapshots: list[BookSnapshot]) -> None:
        self._snapshots = snapshots
        self.calls = 0

    async def fetch_snapshot(self, symbol: str) -> BookSnapshot:
        snapshot = self._snapshots[min(self.calls, len(self._snapshots) - 1)]
        self.calls += 1
        await asyncio.sleep(0.05)  # REST latency: diffs keep arriving meanwhile
        return snapshot


def _snapshot(last_id: int, qty: str) -> BookSnapshot:
    return BookSnapshot(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        bids=(PriceLevel(Decimal("100.00"), Decimal(qty)),),
        asks=(PriceLevel(Decimal("101.00"), Decimal("1")),),
        last_update_id=last_id,
    )


async def test_live_service_buffers_syncs_and_resyncs_after_a_gap() -> None:
    async def server(connection: websockets.ServerConnection) -> None:
        # ids 1..10 contiguous, then 11..12 are lost, then 13..30
        for update_id in range(1, 31):
            if update_id in (11, 12):
                continue
            await connection.send(_diff(update_id, update_id, str(update_id)))
            await asyncio.sleep(0.02)
        await asyncio.sleep(1)

    provider = FakeSnapshots([_snapshot(3, "3"), _snapshot(20, "20")])
    views: list[OrderBookView] = []
    async with websockets.serve(server, "127.0.0.1", 0) as ws_server:
        port = ws_server.sockets[0].getsockname()[1]
        config = BookFeedConfig(
            exchange=ExchangeId.BINANCE,
            symbols=("BTCUSDT",),
            websocket_base_url=f"ws://127.0.0.1:{port}",
        )
        service = LiveOrderBookService(config, snapshot_provider=provider)
        done = asyncio.Event()

        async def on_view(view: OrderBookView) -> None:
            views.append(view)
            if view.last_update_id == 30:
                done.set()

        task = asyncio.create_task(service.run(on_view))
        await asyncio.wait_for(done.wait(), timeout=10)
        await service.stop()
        await asyncio.wait_for(task, timeout=5)

    stats = service.last_sync_stats
    assert stats is not None
    assert provider.calls == 2  # initial sync + one resync
    assert stats.gaps == 1 and stats.gap_events == [(10, 13)]
    assert stats.missing_update_ids == 2
    final = views[-1]
    assert final.best_bid == PriceLevel(Decimal("100.00"), Decimal("30"))
    # every published book is continuous: ids never jump inside a synced stretch
    synced_ids = [view.last_update_id for view in views]
    assert 11 not in synced_ids and 12 not in synced_ids
