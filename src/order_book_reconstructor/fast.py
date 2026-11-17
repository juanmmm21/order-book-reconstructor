"""Latency-oriented L2 book with an optional Rust accelerator.

``BookLevels`` keeps levels as the exchange's decimal strings (no ``Decimal`` objects on the hot
path); ``FastOrderBook`` adds the same diff-depth sequence rules as ``DepthSynchronizer`` (strict
``U == last u + 1`` continuity, a bridging first diff after a snapshot, gaps reported). With the
``order-book-reconstructor-rs`` wheel installed (extra ``[rust]``) the levels live in Rust;
otherwise in ``PyBookLevels``. Property tests check that both backends, and the Decimal-based
reconstructor, return identical books. ``QUANT_DISABLE_RUST=1`` forces Python.
"""

from __future__ import annotations

import bisect
import os
import re
from decimal import Decimal
from typing import Any, Protocol

from order_book_reconstructor.sync import DiffOutcome

Level = tuple[str, str]
_DECIMAL = re.compile(r"[0-9]{1,15}(\.[0-9]{1,12})?")


def _parse(text: str) -> Decimal:
    if not isinstance(text, str) or _DECIMAL.fullmatch(text) is None:
        raise ValueError(f"invalid decimal: {text!r}")
    return Decimal(text)


class Levels(Protocol):
    def clear(self) -> None: ...
    def apply(self, bids: list[Level], asks: list[Level]) -> None: ...
    def top(self, depth: int) -> tuple[list[Level], list[Level]]: ...
    def best(self) -> tuple[Level | None, Level | None]: ...
    def sizes(self) -> tuple[int, int]: ...


class _PySide:
    def __init__(self, descending: bool) -> None:
        self._descending = descending
        self._levels: dict[Decimal, Level] = {}
        self._keys: list[Decimal] = []  # sort keys: -price for bids, price for asks

    def apply(self, parsed: list[tuple[Decimal, Decimal]], changes: list[Level]) -> None:
        for (price, quantity), (price_text, quantity_text) in zip(parsed, changes, strict=True):
            key = -price if self._descending else price
            if quantity == 0:
                if self._levels.pop(price, None) is not None:
                    index = bisect.bisect_left(self._keys, key)
                    del self._keys[index]
            elif price in self._levels:
                self._levels[price] = (self._levels[price][0], quantity_text)
            else:
                self._levels[price] = (price_text, quantity_text)
                bisect.insort(self._keys, key)

    def top(self, depth: int) -> list[Level]:
        sign = -1 if self._descending else 1
        return [self._levels[sign * key] for key in self._keys[:depth]]

    def clear(self) -> None:
        self._levels.clear()
        self._keys.clear()

    def __len__(self) -> int:
        return len(self._levels)


class PyBookLevels:
    def __init__(self) -> None:
        self._bids = _PySide(descending=True)
        self._asks = _PySide(descending=False)

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()

    def apply(self, bids: list[Level], asks: list[Level]) -> None:
        parsed_bids = [(_parse(p), _parse(q)) for p, q in bids]  # validate both sides first
        parsed_asks = [(_parse(p), _parse(q)) for p, q in asks]
        self._bids.apply(parsed_bids, bids)
        self._asks.apply(parsed_asks, asks)

    def top(self, depth: int) -> tuple[list[Level], list[Level]]:
        return self._bids.top(depth), self._asks.top(depth)

    def best(self) -> tuple[Level | None, Level | None]:
        bids, asks = self.top(1)
        return (bids[0] if bids else None, asks[0] if asks else None)

    def sizes(self) -> tuple[int, int]:
        return len(self._bids), len(self._asks)


def _load_rust(*, honour_env: bool = True) -> Any:
    if honour_env and os.environ.get("QUANT_DISABLE_RUST") == "1":
        return None
    try:
        import order_book_reconstructor_rs
    except ImportError:
        return None
    return order_book_reconstructor_rs


_RUST = _load_rust()
BACKEND = "rust" if _RUST is not None else "python"


def new_levels(backend: str | None = None) -> Levels:
    chosen = backend or BACKEND
    if chosen == "rust":
        # QUANT_DISABLE_RUST only changes the default; an explicit request still loads it.
        module = _RUST or _load_rust(honour_env=False)
        if module is None:
            raise RuntimeError("Rust accelerator not installed")
        levels: Levels = module.BookLevels()
        return levels
    return PyBookLevels()


class FastOrderBook:
    """Snapshot + diffs with Binance sequence rules, on string levels."""

    def __init__(self, backend: str | None = None) -> None:
        self.levels = new_levels(backend)
        self.last_update_id: int | None = None
        self._awaiting_bridge = False

    @property
    def synced(self) -> bool:
        return self.last_update_id is not None

    def load_snapshot(self, last_update_id: int, bids: list[Level], asks: list[Level]) -> None:
        self.levels.clear()
        self.levels.apply(bids, asks)
        self.last_update_id = last_update_id
        self._awaiting_bridge = True

    def apply_diff(
        self, first_id: int, final_id: int, bids: list[Level], asks: list[Level]
    ) -> DiffOutcome:
        if self.last_update_id is None:
            return DiffOutcome.BUFFERED  # caller must load a snapshot first
        if final_id <= self.last_update_id:
            return DiffOutcome.STALE
        expected = self.last_update_id + 1
        bridges = self._awaiting_bridge and first_id <= expected
        if first_id != expected and not bridges:
            self.last_update_id = None
            self._awaiting_bridge = False
            return DiffOutcome.GAP
        self.levels.apply(bids, asks)
        self.last_update_id = final_id
        self._awaiting_bridge = False
        return DiffOutcome.APPLIED
