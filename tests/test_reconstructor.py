from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from order_book_reconstructor.models import (
    BookLevelChange,
    BookSide,
    BookSnapshot,
    DepthUpdate,
    ExchangeId,
    PriceLevel,
    UpdateApplyResult,
)
from order_book_reconstructor.reconstructor import OrderBookReconstructor, UpdateSequenceTracker


def test_reconstructor_applies_snapshot_and_updates() -> None:
    book = OrderBookReconstructor(ExchangeId.BINANCE, "BTCUSDT")
    snapshot = BookSnapshot(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        bids=(PriceLevel(Decimal("100"), Decimal("1")),),
        asks=(PriceLevel(Decimal("101"), Decimal("2")),),
        last_update_id=10,
    )
    book.load_snapshot(snapshot)

    update = DepthUpdate(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        bids=(BookLevelChange(BookSide.BID, Decimal("100.5"), Decimal("4")),),
        asks=(BookLevelChange(BookSide.ASK, Decimal("101"), Decimal("0")),),
        event_time=datetime.now(tz=UTC),
        first_update_id=11,
        final_update_id=11,
    )

    result = book.apply_update(update)
    view = book.view(depth=5)

    assert result == UpdateApplyResult.APPLIED
    assert view.best_bid == PriceLevel(Decimal("100.5"), Decimal("4"))
    assert view.best_ask is None
    assert view.spread is None


def test_sequence_tracker_detects_gap() -> None:
    tracker = UpdateSequenceTracker()
    tracker.bootstrap(100)

    stale = DepthUpdate(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        bids=(),
        asks=(),
        event_time=datetime.now(tz=UTC),
        first_update_id=90,
        final_update_id=95,
    )
    valid = DepthUpdate(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        bids=(),
        asks=(),
        event_time=datetime.now(tz=UTC),
        first_update_id=101,
        final_update_id=102,
    )
    gap = DepthUpdate(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        bids=(),
        asks=(),
        event_time=datetime.now(tz=UTC),
        first_update_id=110,
        final_update_id=111,
    )

    assert tracker.evaluate(stale) == UpdateApplyResult.IGNORED_STALE
    assert tracker.evaluate(valid) == UpdateApplyResult.APPLIED
    tracker.commit(valid)
    assert tracker.evaluate(gap) == UpdateApplyResult.OUT_OF_SYNC
