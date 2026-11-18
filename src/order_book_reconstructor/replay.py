"""Offline replay of recorded Binance depth (JSONL from websocket-feed-handler / lakehouse).

Input records (any order of types, in the order they were received):

* ``{"type": "depth_snapshot", "last_update_id", "bids", "asks", "received_at", ...}``
* ``{"type": "depth_diff", "first_update_id", "final_update_id", "bids", "asks", "event_time"}``
* ``{"type": "trade", ...}`` — passed through untouched.

Snapshots are used two ways. In arrival order they (re)synchronise the book exactly like the live
service would. Independently, every snapshot is also used to **verify** the reconstruction: just
before applying the diff whose ``[U, u]`` contains the snapshot's ``lastUpdateId``, the book must
equal the snapshot on every price level that diff does not touch.

Output (``emit``) is a validated stream for downstream consumers (market-condition-simulator):
``book_snapshot`` after each (re)sync, ``book_diff`` for every applied diff, ``book_gap`` when
continuity breaks, and the trades, each tagged with ``book_synced``.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from order_book_reconstructor.models import (
    BookLevelChange,
    BookSide,
    BookSnapshot,
    DepthUpdate,
    ExchangeId,
    PriceLevel,
    utc_from_iso8601,
)
from order_book_reconstructor.reconstructor import OrderBookReconstructor
from order_book_reconstructor.sync import DepthSynchronizer, DiffOutcome, SyncStats

Emit = Callable[[dict[str, Any]], None]


@dataclass(slots=True)
class Verification:
    snapshot_id: int
    compared_levels: int
    mismatched_levels: int


@dataclass(slots=True)
class ReplayReport:
    symbol: str
    diffs_in: int = 0
    trades_in: int = 0
    snapshots_in: int = 0
    dropped_on_purpose: int = 0
    stats: SyncStats = field(default_factory=SyncStats)
    verifications: list[Verification] = field(default_factory=list)
    first_event_time: str | None = None
    last_event_time: str | None = None

    def as_dict(self) -> dict[str, Any]:
        compared = sum(v.compared_levels for v in self.verifications)
        mismatched = sum(v.mismatched_levels for v in self.verifications)
        return {
            "symbol": self.symbol,
            "diffs_in": self.diffs_in,
            "trades_in": self.trades_in,
            "snapshots_in": self.snapshots_in,
            "dropped_on_purpose": self.dropped_on_purpose,
            "applied": self.stats.applied,
            "stale": self.stats.stale,
            "gaps": self.stats.gaps,
            "resyncs": self.stats.resyncs,
            "snapshots_rejected": self.stats.snapshots_rejected,
            "missing_update_ids": self.stats.missing_update_ids,
            "max_buffered": self.stats.max_buffered,
            "verifications": len(self.verifications),
            "verified_levels": compared,
            "mismatched_levels": mismatched,
            "first_event_time": self.first_event_time,
            "last_event_time": self.last_event_time,
        }


def read_jsonl(paths: Iterable[str | Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with Path(path).open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError(f"{path}:{line_number} is not a JSON object")
                yield record


def _levels(raw: Any) -> list[tuple[Decimal, Decimal]]:
    if not isinstance(raw, list):
        raise ValueError("levels must be a list of [price, quantity]")
    return [(Decimal(str(price)), Decimal(str(quantity))) for price, quantity in raw]


def snapshot_from_record(record: dict[str, Any], symbol: str) -> BookSnapshot:
    return BookSnapshot(
        exchange=ExchangeId.BINANCE,
        symbol=symbol,
        bids=tuple(PriceLevel(p, q) for p, q in _levels(record["bids"]) if q > 0),
        asks=tuple(PriceLevel(p, q) for p, q in _levels(record["asks"]) if q > 0),
        last_update_id=int(record["last_update_id"]),
        event_time=_time(record.get("received_at") or record.get("event_time")),
    )


def diff_from_record(record: dict[str, Any], symbol: str) -> DepthUpdate:
    return DepthUpdate(
        exchange=ExchangeId.BINANCE,
        symbol=symbol,
        bids=tuple(BookLevelChange(BookSide.BID, p, q) for p, q in _levels(record["bids"])),
        asks=tuple(BookLevelChange(BookSide.ASK, p, q) for p, q in _levels(record["asks"])),
        event_time=_time(record.get("event_time")),
        first_update_id=int(record["first_update_id"]),
        final_update_id=int(record["final_update_id"]),
    )


def _time(value: Any) -> datetime:
    return utc_from_iso8601(str(value)) if value else datetime.now(tz=UTC)


def _encode(levels: Iterable[PriceLevel]) -> list[list[str]]:
    return [[str(level.price), str(level.quantity)] for level in levels]


class _Verifier:
    def __init__(self, snapshots: list[BookSnapshot], depth: int) -> None:
        self._pending = sorted(snapshots, key=lambda s: s.last_update_id or 0)
        self._depth = depth
        self.results: list[Verification] = []

    def discard(self, snapshot_id: int | None) -> None:
        """A snapshot used to (re)synchronise cannot also verify the book it seeded."""
        self._pending = [s for s in self._pending if s.last_update_id != snapshot_id]

    def before_apply(self, book: OrderBookReconstructor, diff: DepthUpdate) -> None:
        assert diff.first_update_id is not None and diff.final_update_id is not None
        current = book.last_update_id
        while self._pending:
            snap = self._pending[0]
            snap_id = snap.last_update_id or 0
            if current is not None and snap_id < current:
                self._pending.pop(0)  # passed while unsynced: not comparable
                continue
            if snap_id >= diff.final_update_id:
                return
            self._pending.pop(0)
            if current is None or not diff.first_update_id - 1 <= snap_id:
                continue
            # Exactly on a diff boundary nothing straddles the snapshot: compare every level.
            touched = (
                set()
                if diff.first_update_id == snap_id + 1
                else {change.price for change in (*diff.bids, *diff.asks)}
            )
            self.results.append(self._compare(book, snap, touched))

    def _compare(
        self, book: OrderBookReconstructor, snap: BookSnapshot, touched: set[Decimal]
    ) -> Verification:
        compared = 0
        mismatched = 0
        for side, levels in ((BookSide.BID, snap.bids), (BookSide.ASK, snap.asks)):
            ours = book.levels(side)
            for level in levels[: self._depth]:
                if level.price in touched:
                    continue
                compared += 1
                if ours.get(level.price) != level.quantity:
                    mismatched += 1
        return Verification(snap.last_update_id or 0, compared, mismatched)


def replay(
    records: Iterable[dict[str, Any]],
    *,
    symbol: str,
    emit: Emit | None = None,
    emit_levels: int = 1000,
    verify_depth: int = 200,
    drop_diff_every: int = 0,
) -> ReplayReport:
    """Validate a recording; ``drop_diff_every=N`` deletes every Nth diff to exercise resyncs."""
    items = list(records)
    snapshots = [
        snapshot_from_record(r, symbol) for r in items if r.get("type") == "depth_snapshot"
    ]
    book = OrderBookReconstructor(ExchangeId.BINANCE, symbol)
    sync = DepthSynchronizer(book)
    verifier = _Verifier(snapshots, verify_depth)
    report = ReplayReport(symbol=symbol, stats=sync.stats)
    out: Emit = emit or (lambda _record: None)
    snapshot_index = 0

    def publish_sync() -> None:
        view = book.view(emit_levels)
        out(
            {
                "type": "book_snapshot",
                "symbol": symbol,
                "last_update_id": sync.last_update_id,
                "event_time": view.event_time.isoformat(),
                "bids": _encode(view.bids),
                "asks": _encode(view.asks),
            }
        )

    for record in items:
        kind = record.get("type")
        if kind == "depth_snapshot":
            snapshot = snapshots[snapshot_index]
            snapshot_index += 1
            report.snapshots_in += 1
            if sync.needs_snapshot:
                verifier.discard(snapshot.last_update_id)
                if sync.on_snapshot(snapshot):
                    publish_sync()
            continue
        if kind == "trade":
            report.trades_in += 1
            out({**record, "book_synced": not sync.needs_snapshot})
            continue
        if kind != "depth_diff":
            continue

        report.diffs_in += 1
        if drop_diff_every and report.diffs_in % drop_diff_every == 0:
            report.dropped_on_purpose += 1
            continue
        diff = diff_from_record(record, symbol)
        report.first_event_time = report.first_event_time or str(record.get("event_time"))
        report.last_event_time = str(record.get("event_time"))
        if not sync.needs_snapshot:
            verifier.before_apply(book, diff)
        outcome = sync.on_diff(diff)
        if outcome is DiffOutcome.APPLIED:
            out(
                {
                    "type": "book_diff",
                    "symbol": symbol,
                    "first_update_id": diff.first_update_id,
                    "final_update_id": diff.final_update_id,
                    "event_time": record.get("event_time"),
                    "bids": record["bids"],
                    "asks": record["asks"],
                }
            )
        elif outcome is DiffOutcome.GAP:
            out({"type": "book_gap", "symbol": symbol, "event_time": record.get("event_time")})

    report.verifications = verifier.results
    return report
