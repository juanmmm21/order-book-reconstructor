"""Binance diff-depth synchronisation as a pure state machine (no I/O).

The documented procedure (Binance Spot "How to manage a local order book correctly"):

1. Open the stream and **buffer** diffs.
2. Fetch a REST snapshot (``lastUpdateId``).
3. Drop buffered diffs with ``u <= lastUpdateId``.
4. The first diff applied must satisfy ``U <= lastUpdateId + 1 <= u``.
5. Every following diff must start exactly at the previous ``u + 1``; otherwise updates were
   lost and the book must be resynchronised from a new snapshot (back to step 1).

``DepthSynchronizer`` implements steps 3-5 and tells the caller when a snapshot is needed; the
live service and the offline replay share it, so both are tested by the same unit tests.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum

from order_book_reconstructor.models import BookSnapshot, DepthUpdate
from order_book_reconstructor.reconstructor import OrderBookReconstructor


class SyncState(StrEnum):
    AWAITING_SNAPSHOT = "awaiting_snapshot"
    SYNCED = "synced"


class DiffOutcome(StrEnum):
    APPLIED = "applied"
    BUFFERED = "buffered"
    STALE = "stale"
    GAP = "gap"


@dataclass(slots=True)
class SyncStats:
    applied: int = 0
    stale: int = 0
    gaps: int = 0
    resyncs: int = 0
    snapshots_rejected: int = 0
    buffer_overflows: int = 0
    max_buffered: int = 0
    missing_update_ids: int = 0
    gap_events: list[tuple[int, int]] = field(default_factory=list)


class DepthSynchronizer:
    def __init__(self, book: OrderBookReconstructor, *, max_buffer: int = 50_000) -> None:
        if max_buffer <= 0:
            raise ValueError("max_buffer must be positive")
        self._book = book
        self._buffer: deque[DepthUpdate] = deque()
        self._max_buffer = max_buffer
        self._state = SyncState.AWAITING_SNAPSHOT
        self._last_final_id: int | None = None
        self.stats = SyncStats()

    @property
    def state(self) -> SyncState:
        return self._state

    @property
    def book(self) -> OrderBookReconstructor:
        return self._book

    @property
    def needs_snapshot(self) -> bool:
        return self._state is SyncState.AWAITING_SNAPSHOT

    @property
    def last_update_id(self) -> int | None:
        return self._last_final_id

    @property
    def buffered(self) -> int:
        return len(self._buffer)

    def on_diff(self, update: DepthUpdate) -> DiffOutcome:
        if update.first_update_id is None or update.final_update_id is None:
            raise ValueError("diff-depth updates need first and final update ids")
        if self._state is SyncState.AWAITING_SNAPSHOT:
            self._push(update)
            return DiffOutcome.BUFFERED

        assert self._last_final_id is not None
        if update.final_update_id <= self._last_final_id:
            self.stats.stale += 1
            return DiffOutcome.STALE
        if update.first_update_id != self._last_final_id + 1:
            self._enter_gap(update)
            return DiffOutcome.GAP
        self._apply(update)
        return DiffOutcome.APPLIED

    def on_snapshot(self, snapshot: BookSnapshot) -> bool:
        """Load ``snapshot`` and replay the buffer. False if it cannot bridge the buffer."""
        if snapshot.last_update_id is None:
            raise ValueError("diff-depth synchronisation needs snapshot.last_update_id")
        if self._state is SyncState.SYNCED:
            return True
        last_id = snapshot.last_update_id
        while self._buffer and self._buffer[0].final_update_id is not None and (
            self._buffer[0].final_update_id <= last_id
        ):
            self._buffer.popleft()
            self.stats.stale += 1
        if self._buffer:
            first = self._buffer[0]
            assert first.first_update_id is not None
            if first.first_update_id > last_id + 1:
                # Snapshot older than the oldest buffered diff: updates in between are unknown.
                self.stats.snapshots_rejected += 1
                return False

        self._book.reset()
        self._book.load_snapshot(snapshot)
        self._last_final_id = last_id
        self._state = SyncState.SYNCED
        pending = list(self._buffer)
        self._buffer.clear()
        if pending:
            # The first diff may straddle the snapshot (U <= lastUpdateId + 1 <= u); checked
            # above, so it is applied as is and strict continuity starts from its u.
            self._apply(pending.pop(0))
        # After a gap inside the buffer the synchroniser is awaiting a snapshot again, so the
        # remaining diffs are re-buffered rather than dropped.
        outcomes = [self.on_diff(update) for update in pending]
        return DiffOutcome.GAP not in outcomes

    def _apply(self, update: DepthUpdate) -> None:
        self._book.apply_unchecked(update)
        assert update.final_update_id is not None
        self._last_final_id = update.final_update_id
        self.stats.applied += 1

    def _enter_gap(self, update: DepthUpdate) -> None:
        assert self._last_final_id is not None and update.first_update_id is not None
        self.stats.gaps += 1
        self.stats.resyncs += 1
        self.stats.missing_update_ids += update.first_update_id - self._last_final_id - 1
        self.stats.gap_events.append((self._last_final_id, update.first_update_id))
        self._state = SyncState.AWAITING_SNAPSHOT
        self._buffer.clear()
        self._push(update)

    def _push(self, update: DepthUpdate) -> None:
        if len(self._buffer) >= self._max_buffer:
            self._buffer.popleft()
            self.stats.buffer_overflows += 1
        self._buffer.append(update)
        self.stats.max_buffered = max(self.stats.max_buffered, len(self._buffer))
