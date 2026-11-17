from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from order_book_reconstructor.models import (
    BookLevelChange,
    BookSide,
    BookSnapshot,
    DepthUpdate,
    ExchangeId,
    PriceLevel,
)
from order_book_reconstructor.reconstructor import OrderBookReconstructor
from order_book_reconstructor.sync import DepthSynchronizer, DiffOutcome, SyncState

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _diff(first: int, final: int, bids: dict[int, int], asks: dict[int, int]) -> DepthUpdate:
    return DepthUpdate(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        bids=tuple(BookLevelChange(BookSide.BID, Decimal(p), Decimal(q)) for p, q in bids.items()),
        asks=tuple(BookLevelChange(BookSide.ASK, Decimal(p), Decimal(q)) for p, q in asks.items()),
        event_time=NOW,
        first_update_id=first,
        final_update_id=final,
    )


def _snapshot(last_id: int, bids: dict[int, int], asks: dict[int, int]) -> BookSnapshot:
    return BookSnapshot(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        bids=tuple(PriceLevel(Decimal(p), Decimal(q)) for p, q in bids.items() if q),
        asks=tuple(PriceLevel(Decimal(p), Decimal(q)) for p, q in asks.items() if q),
        last_update_id=last_id,
    )


def _sync() -> DepthSynchronizer:
    return DepthSynchronizer(OrderBookReconstructor(ExchangeId.BINANCE, "BTCUSDT"))


def test_buffers_until_snapshot_then_drops_stale_and_bridges() -> None:
    sync = _sync()
    assert sync.on_diff(_diff(95, 99, {100: 1}, {})) is DiffOutcome.BUFFERED
    assert sync.on_diff(_diff(100, 103, {100: 2}, {})) is DiffOutcome.BUFFERED
    assert sync.on_diff(_diff(104, 104, {}, {101: 5})) is DiffOutcome.BUFFERED
    assert sync.on_snapshot(_snapshot(101, {100: 9}, {101: 1}))
    assert sync.state is SyncState.SYNCED
    assert sync.last_update_id == 104
    assert sync.stats.stale == 1  # 95..99 is older than the snapshot
    view = sync.book.view(5)
    assert view.best_bid == PriceLevel(Decimal(100), Decimal(2))
    assert view.best_ask == PriceLevel(Decimal(101), Decimal(5))


def test_snapshot_older_than_buffer_is_rejected() -> None:
    sync = _sync()
    sync.on_diff(_diff(200, 210, {100: 1}, {}))
    assert not sync.on_snapshot(_snapshot(150, {100: 3}, {}))
    assert sync.needs_snapshot
    assert sync.stats.snapshots_rejected == 1
    assert sync.on_snapshot(_snapshot(205, {100: 3}, {}))
    assert sync.last_update_id == 210


def test_gap_triggers_resync_and_keeps_later_diffs() -> None:
    sync = _sync()
    sync.on_snapshot(_snapshot(10, {100: 1}, {101: 1}))
    assert sync.on_diff(_diff(11, 12, {100: 2}, {})) is DiffOutcome.APPLIED
    assert sync.on_diff(_diff(15, 16, {100: 3}, {})) is DiffOutcome.GAP
    assert sync.stats.missing_update_ids == 2
    assert sync.stats.gap_events == [(12, 15)]
    assert sync.on_diff(_diff(17, 18, {100: 4}, {})) is DiffOutcome.BUFFERED
    assert sync.on_snapshot(_snapshot(16, {100: 3}, {101: 1}))
    assert sync.book.view(1).best_bid == PriceLevel(Decimal(100), Decimal(4))
    assert sync.stats.resyncs == 1


def test_stale_diffs_after_sync_are_ignored() -> None:
    sync = _sync()
    sync.on_snapshot(_snapshot(10, {100: 1}, {}))
    assert sync.on_diff(_diff(5, 10, {100: 7}, {})) is DiffOutcome.STALE
    assert sync.book.view(1).best_bid == PriceLevel(Decimal(100), Decimal(1))


def test_buffer_is_bounded() -> None:
    sync = DepthSynchronizer(OrderBookReconstructor(ExchangeId.BINANCE, "BTCUSDT"), max_buffer=3)
    for i in range(5):
        sync.on_diff(_diff(i * 2 + 1, i * 2 + 2, {100: i + 1}, {}))
    assert sync.buffered == 3
    assert sync.stats.buffer_overflows == 2


Changes = dict[int, int]


@st.composite
def _scenario(
    draw: st.DrawFn,
) -> tuple[list[list[tuple[Changes, Changes]]], set[int], int, list[int]]:
    """Messages made of 1-3 sub-updates each (like Binance's 100 ms aggregation)."""
    steps = draw(st.integers(min_value=5, max_value=40))
    level = st.dictionaries(
        st.integers(min_value=90, max_value=110), st.integers(min_value=0, max_value=5), max_size=3
    )
    messages = [
        [(draw(level), draw(level)) for _ in range(draw(st.integers(1, 3)))]
        for _ in range(steps)
    ]
    dropped = draw(st.sets(st.integers(min_value=1, max_value=steps - 1), max_size=4))
    lag = draw(st.integers(min_value=0, max_value=3))
    # how far back (in sub-updates) each snapshot is taken from the newest delivered id
    snapshot_offsets = draw(st.lists(st.integers(0, 2), min_size=steps, max_size=steps))
    return messages, dropped, lag, snapshot_offsets


@settings(max_examples=200, deadline=None)
@given(_scenario())
def test_property_synced_book_always_equals_the_true_book(
    scenario: tuple[list[list[tuple[Changes, Changes]]], set[int], int, list[int]],
) -> None:
    messages, dropped, lag, offsets = scenario
    truth_bids: Changes = {100: 1}
    truth_asks: Changes = {105: 1}
    history: dict[int, tuple[Changes, Changes]] = {0: (dict(truth_bids), dict(truth_asks))}
    sync = _sync()
    update_id = 0
    waited = 0
    for step, subs in enumerate(messages):
        first = update_id + 1
        merged_bids: Changes = {}
        merged_asks: Changes = {}
        for bids, asks in subs:
            update_id += 1
            truth_bids.update(bids)
            truth_asks.update(asks)
            merged_bids.update(bids)
            merged_asks.update(asks)
            history[update_id] = (dict(truth_bids), dict(truth_asks))
        if step not in dropped:
            sync.on_diff(_diff(first, update_id, merged_bids, merged_asks))
        if sync.needs_snapshot:
            waited += 1
            if waited > lag:
                snap_id = max(0, update_id - offsets[step])
                snap_bids, snap_asks = history[snap_id]
                sync.on_snapshot(_snapshot(snap_id, snap_bids, snap_asks))
                waited = 0
        if sync.state is SyncState.SYNCED:
            assert sync.last_update_id is not None
            want_bids, want_asks = history[sync.last_update_id]
            got_bids = sync.book.levels(BookSide.BID)
            got_asks = sync.book.levels(BookSide.ASK)
            assert got_bids == {Decimal(p): Decimal(q) for p, q in want_bids.items() if q}
            assert got_asks == {Decimal(p): Decimal(q) for p, q in want_asks.items() if q}
