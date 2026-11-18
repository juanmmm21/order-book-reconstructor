from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from order_book_reconstructor.replay import replay


def _diff(first: int, final: int, bid: str) -> dict[str, Any]:
    return {
        "type": "depth_diff",
        "first_update_id": first,
        "final_update_id": final,
        "event_time": f"2026-11-17T23:00:{final % 60:02d}+00:00",
        "bids": [["100.0", bid]],
        "asks": [["101.0", "1"]],
    }


def _snap(last_id: int, bid: str) -> dict[str, Any]:
    return {
        "type": "depth_snapshot",
        "last_update_id": last_id,
        "bids": [["100.0", bid], ["99.0", "7"]],
        "asks": [["101.0", "1"], ["102.0", "3"]],
        "received_at": "2026-11-17T23:00:00+00:00",
    }


def _recording() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = [_diff(1, 2, "2"), _diff(3, 4, "4"), _snap(3, "3")]
    records += [_diff(i, i, str(i)) for i in range(5, 21)]
    records.insert(8, {"type": "trade", "price": "100.0", "quantity": "0.1"})
    records.append(_snap(12, "12"))  # arrives late; still verifies the book at id 12
    return records


def test_replay_syncs_verifies_and_emits_clean_stream() -> None:
    emitted: list[dict[str, Any]] = []
    report = replay(_recording(), symbol="BTCUSDT", emit=emitted.append, verify_depth=10)
    summary = report.as_dict()
    assert summary["gaps"] == 0
    assert summary["applied"] == 17
    assert summary["verifications"] == 1  # the sync snapshot is not self-verified
    assert summary["mismatched_levels"] == 0
    assert summary["verified_levels"] == 4  # id 12 is a diff boundary: all 4 levels compared
    types = [record["type"] for record in emitted]
    assert types[0] == "book_snapshot"
    assert types.count("book_diff") == 16
    trade = next(record for record in emitted if record["type"] == "trade")
    assert trade["book_synced"] is True


def test_replay_detects_a_corrupted_book() -> None:
    records = _recording()
    records[-1] = {**_snap(12, "12"), "bids": [["100.0", "12"], ["99.0", "8"]]}
    report = replay(records, symbol="BTCUSDT", verify_depth=10)
    assert report.as_dict()["mismatched_levels"] == 1


def test_replay_resyncs_after_dropped_diffs() -> None:
    records = _recording()
    records.append(_snap(25, "25"))
    records += [_diff(i, i, str(i)) for i in range(26, 30)]
    emitted: list[dict[str, Any]] = []
    report = replay(records, symbol="BTCUSDT", emit=emitted.append, drop_diff_every=6)
    summary = report.as_dict()
    assert summary["dropped_on_purpose"] >= 2
    assert summary["gaps"] >= 1
    assert "book_gap" in [record["type"] for record in emitted]
    assert emitted[-1]["final_update_id"] == 29


def test_cli_replay(tmp_path: Path) -> None:
    source = tmp_path / "recording.jsonl"
    source.write_text("\n".join(json.dumps(r) for r in _recording()), encoding="utf-8")
    out = tmp_path / "book.jsonl"
    result = subprocess.run(
        [sys.executable, "-m", "order_book_reconstructor", "replay", "--input", str(source),
         "--symbol", "btcusdt", "--output", str(out)],
        check=True, capture_output=True, text=True,
    )
    assert json.loads(result.stdout)["applied"] == 17
    assert out.read_text(encoding="utf-8").count("book_diff") == 16
