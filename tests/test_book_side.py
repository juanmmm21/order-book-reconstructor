from __future__ import annotations

from decimal import Decimal

from order_book_reconstructor.book_side import SortedBookSide
from order_book_reconstructor.models import BookSide


def test_bids_are_sorted_descending() -> None:
    side = SortedBookSide(BookSide.BID)
    side.upsert(Decimal("100"), Decimal("1"))
    side.upsert(Decimal("102"), Decimal("2"))
    side.upsert(Decimal("101"), Decimal("3"))

    levels = side.top_levels(3)
    assert [level.price for level in levels] == [Decimal("102"), Decimal("101"), Decimal("100")]


def test_asks_are_sorted_ascending() -> None:
    side = SortedBookSide(BookSide.ASK)
    side.upsert(Decimal("100"), Decimal("1"))
    side.upsert(Decimal("102"), Decimal("2"))
    side.upsert(Decimal("101"), Decimal("3"))

    levels = side.top_levels(3)
    assert [level.price for level in levels] == [Decimal("100"), Decimal("101"), Decimal("102")]


def test_zero_quantity_removes_level() -> None:
    side = SortedBookSide(BookSide.ASK)
    side.upsert(Decimal("100"), Decimal("1"))
    side.upsert(Decimal("100"), Decimal("0"))

    assert len(side) == 0
    assert side.best() is None
