from __future__ import annotations

import bisect
from decimal import Decimal

from order_book_reconstructor.models import BookSide, PriceLevel


class SortedBookSide:
    """Lado del libro mantenido ordenado con inserción binaria para latencia mínima.

    ``_ranks`` holds the sort keys (``-price`` for bids, ``price`` for asks) next to
    ``_prices`` so that locating a level is a single O(log n) bisect, not a rebuild of the
    key list on every update.
    """

    def __init__(self, side: BookSide) -> None:
        self._side = side
        self._quantities: dict[Decimal, Decimal] = {}
        self._prices: list[Decimal] = []
        self._ranks: list[Decimal] = []

    @property
    def side(self) -> BookSide:
        return self._side

    def __len__(self) -> int:
        return len(self._prices)

    def _rank(self, price: Decimal) -> Decimal:
        return -price if self._side == BookSide.BID else price

    def clear(self) -> None:
        self._quantities.clear()
        self._prices.clear()
        self._ranks.clear()

    def _locate_insert_index(self, price: Decimal) -> int:
        return bisect.bisect_left(self._ranks, self._rank(price))

    def upsert(self, price: Decimal, quantity: Decimal) -> None:
        if quantity < Decimal("0"):
            raise ValueError("quantity must be non-negative")
        if quantity == Decimal("0"):
            self.remove(price)
            return

        if price in self._quantities:
            self._quantities[price] = quantity
            return

        index = self._locate_insert_index(price)
        self._prices.insert(index, price)
        self._ranks.insert(index, self._rank(price))
        self._quantities[price] = quantity

    def remove(self, price: Decimal) -> None:
        if price not in self._quantities:
            return
        del self._quantities[price]
        index = self._locate_insert_index(price)
        if index < len(self._prices) and self._prices[index] == price:
            self._prices.pop(index)
            self._ranks.pop(index)

    def get(self, price: Decimal) -> Decimal | None:
        return self._quantities.get(price)

    def best(self) -> PriceLevel | None:
        if not self._prices:
            return None
        price = self._prices[0]
        return PriceLevel(price=price, quantity=self._quantities[price])

    def top_levels(self, depth: int) -> tuple[PriceLevel, ...]:
        if depth <= 0:
            raise ValueError("depth must be positive")
        levels: list[PriceLevel] = []
        for price in self._prices[:depth]:
            levels.append(PriceLevel(price=price, quantity=self._quantities[price]))
        return tuple(levels)

    def load_levels(self, levels: tuple[PriceLevel, ...]) -> None:
        self.clear()
        for level in levels:
            self.upsert(level.price, level.quantity)
