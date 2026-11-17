from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from order_book_reconstructor.book_side import SortedBookSide
from order_book_reconstructor.models import (
    BookLevelChange,
    BookSide,
    BookSnapshot,
    DepthUpdate,
    ExchangeId,
    OrderBookView,
    UpdateApplyResult,
)


class UpdateSequenceTracker:
    """Valida continuidad de update IDs para flujos diff-depth de Binance."""

    def __init__(self) -> None:
        self._last_update_id: int | None = None
        self._ready = False

    @property
    def last_update_id(self) -> int | None:
        return self._last_update_id

    @property
    def is_ready(self) -> bool:
        return self._ready

    def reset(self) -> None:
        self._last_update_id = None
        self._ready = False

    def bootstrap(self, snapshot_last_update_id: int) -> None:
        self._last_update_id = snapshot_last_update_id
        self._ready = True

    def evaluate(self, update: DepthUpdate) -> UpdateApplyResult:
        if update.final_update_id is None or update.first_update_id is None:
            return UpdateApplyResult.APPLIED

        if not self._ready or self._last_update_id is None:
            return UpdateApplyResult.NOT_READY

        last_id = self._last_update_id
        first_id = update.first_update_id
        final_id = update.final_update_id

        if final_id <= last_id:
            return UpdateApplyResult.IGNORED_STALE

        if first_id <= last_id + 1 <= final_id:
            return UpdateApplyResult.APPLIED

        return UpdateApplyResult.OUT_OF_SYNC

    def commit(self, update: DepthUpdate) -> None:
        if update.final_update_id is not None:
            self._last_update_id = update.final_update_id


class OrderBookReconstructor:
    """Reconstructor L2 en memoria con bids descendentes y asks ascendentes."""

    def __init__(self, exchange: ExchangeId, symbol: str) -> None:
        self._exchange = exchange
        self._symbol = symbol
        self._bids = SortedBookSide(BookSide.BID)
        self._asks = SortedBookSide(BookSide.ASK)
        self._sequence = UpdateSequenceTracker()
        self._last_event_time = datetime.now(tz=UTC)

    @property
    def exchange(self) -> ExchangeId:
        return self._exchange

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def is_ready(self) -> bool:
        return self._sequence.is_ready

    @property
    def last_update_id(self) -> int | None:
        return self._sequence.last_update_id

    def load_snapshot(self, snapshot: BookSnapshot) -> None:
        if snapshot.exchange != self._exchange or snapshot.symbol != self._symbol:
            raise ValueError("snapshot exchange or symbol mismatch")

        self._bids.load_levels(snapshot.bids)
        self._asks.load_levels(snapshot.asks)
        if snapshot.last_update_id is not None:
            self._sequence.bootstrap(snapshot.last_update_id)
        if snapshot.event_time is not None:
            self._last_event_time = snapshot.event_time

    def apply_update(self, update: DepthUpdate) -> UpdateApplyResult:
        if update.exchange != self._exchange or update.symbol != self._symbol:
            raise ValueError("update exchange or symbol mismatch")

        result = self._sequence.evaluate(update)
        if result != UpdateApplyResult.APPLIED:
            return result

        self._apply_side_changes(self._bids, update.bids)
        self._apply_side_changes(self._asks, update.asks)
        self._sequence.commit(update)
        self._last_event_time = update.event_time
        return UpdateApplyResult.APPLIED

    def apply_unchecked(self, update: DepthUpdate) -> None:
        """Apply level changes whose ordering was already validated (``DepthSynchronizer``)."""
        self._apply_side_changes(self._bids, update.bids)
        self._apply_side_changes(self._asks, update.asks)
        self._sequence.commit(update)
        self._last_event_time = update.event_time

    def levels(self, side: BookSide) -> dict[Decimal, Decimal]:
        """Copy of every level on one side (price -> quantity)."""
        book_side = self._bids if side is BookSide.BID else self._asks
        if not len(book_side):
            return {}
        return {level.price: level.quantity for level in book_side.top_levels(len(book_side))}

    def view(self, depth: int) -> OrderBookView:
        best_bid = self._bids.best()
        best_ask = self._asks.best()
        spread = None
        mid_price = None

        if best_bid is not None and best_ask is not None:
            spread = best_ask.price - best_bid.price
            mid_price = (best_ask.price + best_bid.price) / Decimal("2")

        return OrderBookView(
            exchange=self._exchange,
            symbol=self._symbol,
            bids=self._bids.top_levels(depth),
            asks=self._asks.top_levels(depth),
            last_update_id=self._sequence.last_update_id,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            mid_price=mid_price,
            event_time=self._last_event_time,
        )

    def reset(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self._sequence.reset()
        self._last_event_time = datetime.now(tz=UTC)

    @staticmethod
    def _apply_side_changes(side: SortedBookSide, changes: tuple[BookLevelChange, ...]) -> None:
        for change in changes:
            side.upsert(change.price, change.quantity)
