"""Rust levels, Python levels and the Decimal reconstructor must agree exactly."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from order_book_reconstructor.fast import FastOrderBook, PyBookLevels
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

rust = pytest.importorskip("order_book_reconstructor_rs", reason="Rust accelerator not built")

# Small price grid with several spellings of the same number, plus zero spellings.
prices = st.sampled_from(["99", "99.0", "99.50", "99.5", "100", "100.00", "100.25", "101",
                          "101.000", "65089.08000000", "0.00000001"])
quantities = st.sampled_from(["0", "0.0", "0.00000000", "1", "1.5", "2.50", "0.001", "10"])
bad = st.sampled_from(["", "-1", "1e3", "NaN", "1.", ".1", "x", "1234567890123456"])
levels = st.lists(st.tuples(prices, quantities), max_size=6)


def _state(book: object) -> object:
    return book.top(10_000), book.best(), book.sizes()  # type: ignore[attr-defined]


@settings(max_examples=500, deadline=None)
@given(st.lists(st.tuples(levels, levels), min_size=1, max_size=25))
def test_property_rust_and_python_levels_are_identical(
    batches: list[tuple[list[tuple[str, str]], list[tuple[str, str]]]],
) -> None:
    py, rs = PyBookLevels(), rust.BookLevels()
    for bids, asks in batches:
        py.apply(bids, asks)
        rs.apply(bids, asks)
        assert _state(py) == _state(rs)


@settings(max_examples=200, deadline=None)
@given(levels, levels, bad, st.booleans())
def test_property_invalid_input_changes_nothing_in_either(
    bids: list[tuple[str, str]], asks: list[tuple[str, str]], wrong: str, in_price: bool
) -> None:
    py, rs = PyBookLevels(), rust.BookLevels()
    py.apply(bids, asks)
    rs.apply(bids, asks)
    before = _state(py)
    poisoned = [("100", "1"), (wrong, "1") if in_price else ("100", wrong)]
    for book in (py, rs):
        with pytest.raises(ValueError):
            book.apply(list(bids), poisoned)
    assert _state(py) == _state(rs) == before


@settings(max_examples=300, deadline=None)
@given(st.lists(st.tuples(levels, levels), min_size=1, max_size=20))
def test_property_levels_match_the_decimal_reconstructor(
    batches: list[tuple[list[tuple[str, str]], list[tuple[str, str]]]],
) -> None:
    reference = OrderBookReconstructor(ExchangeId.BINANCE, "BTCUSDT")
    reference.load_snapshot(BookSnapshot(ExchangeId.BINANCE, "BTCUSDT", (), (), 0))
    fast = rust.BookLevels()
    for index, (bids, asks) in enumerate(batches, start=1):
        reference.apply_unchecked(_update(index, index, bids, asks))
        fast.apply(bids, asks)
        view = reference.view(10_000)
        fast_bids, fast_asks = fast.top(10_000)
        assert [(Decimal(p), Decimal(q)) for p, q in fast_bids] == [
            (lvl.price, lvl.quantity) for lvl in view.bids
        ]
        assert [(Decimal(p), Decimal(q)) for p, q in fast_asks] == [
            (lvl.price, lvl.quantity) for lvl in view.asks
        ]


def _update(first: int, final: int, bids: list[tuple[str, str]],
            asks: list[tuple[str, str]]) -> DepthUpdate:
    return DepthUpdate(
        exchange=ExchangeId.BINANCE, symbol="BTCUSDT",
        bids=tuple(BookLevelChange(BookSide.BID, Decimal(p), Decimal(q)) for p, q in bids),
        asks=tuple(BookLevelChange(BookSide.ASK, Decimal(p), Decimal(q)) for p, q in asks),
        event_time=datetime(2026, 1, 1, tzinfo=UTC), first_update_id=first, final_update_id=final,
    )


@settings(max_examples=300, deadline=None)
@given(
    st.lists(st.tuples(levels, levels, st.integers(1, 3)), min_size=2, max_size=25),
    st.sets(st.integers(0, 24), max_size=3),
    st.sampled_from(["rust", "python"]),
)
def test_property_fast_order_book_follows_the_same_sequence_rules(
    messages: list[tuple[list[tuple[str, str]], list[tuple[str, str]], int]],
    dropped: set[int],
    backend: str,
) -> None:
    snapshot_bids, snapshot_asks = [("100", "1")], [("101", "1")]
    fast = FastOrderBook(backend)
    fast.load_snapshot(0, snapshot_bids, snapshot_asks)
    sync = DepthSynchronizer(OrderBookReconstructor(ExchangeId.BINANCE, "BTCUSDT"))
    sync.on_snapshot(BookSnapshot(
        ExchangeId.BINANCE, "BTCUSDT",
        tuple(PriceLevel(Decimal(p), Decimal(q)) for p, q in snapshot_bids),
        tuple(PriceLevel(Decimal(p), Decimal(q)) for p, q in snapshot_asks), 0,
    ))
    next_id = 1
    for index, (bids, asks, width) in enumerate(messages):
        first, final = next_id, next_id + width - 1
        next_id = final + 1
        if index in dropped:
            continue
        fast_outcome = fast.apply_diff(first, final, bids, asks)
        sync_outcome = sync.on_diff(_update(first, final, bids, asks))
        if sync.state is SyncState.SYNCED or sync_outcome is DiffOutcome.GAP:
            assert fast_outcome == sync_outcome
        if sync_outcome is DiffOutcome.GAP:
            break  # from here the reference buffers and waits for a new snapshot
        view = sync.book.view(50)
        fast_bids, fast_asks = fast.levels.top(50)
        assert [(Decimal(p), Decimal(q)) for p, q in fast_bids] == [
            (lvl.price, lvl.quantity) for lvl in view.bids
        ]
        assert [(Decimal(p), Decimal(q)) for p, q in fast_asks] == [
            (lvl.price, lvl.quantity) for lvl in view.asks
        ]


def test_level_keeps_its_first_spelling_and_latest_quantity() -> None:
    for book in (PyBookLevels(), rust.BookLevels()):
        book.apply([("100.0", "1")], [])
        book.apply([("100.00", "2.50")], [])
        assert book.best() == (("100.0", "2.50"), None)
        book.apply([("100", "0.000")], [])
        assert book.sizes() == (0, 0)
