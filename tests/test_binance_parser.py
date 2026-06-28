from __future__ import annotations

from decimal import Decimal

from order_book_reconstructor.exchanges.binance import (
    BinanceDepthParser,
    parse_binance_rest_snapshot,
)
from order_book_reconstructor.models import ExchangeId


def test_binance_depth_update_parser() -> None:
    parser = BinanceDepthParser()
    payload = {
        "e": "depthUpdate",
        "E": 1672515782136,
        "s": "BNBBTC",
        "U": 157,
        "u": 160,
        "b": [["0.0024", "10"]],
        "a": [["0.0026", "0"]],
    }

    parsed = parser.parse(payload)

    assert parsed is not None
    assert parsed.exchange == ExchangeId.BINANCE
    assert parsed.symbol == "BNBBTC"
    assert parsed.bids[0].price == Decimal("0.0024")
    assert parsed.asks[0].quantity == Decimal("0")
    assert parsed.first_update_id == 157
    assert parsed.final_update_id == 160


def test_binance_rest_snapshot_parser() -> None:
    payload = {
        "lastUpdateId": 1027024,
        "bids": [["4.00000000", "431.00000000"]],
        "asks": [["4.00000200", "12.00000000"]],
    }

    snapshot = parse_binance_rest_snapshot("BNBBTC", payload)

    assert snapshot.last_update_id == 1027024
    assert snapshot.bids[0].price == Decimal("4.00000000")
    assert snapshot.asks[0].price == Decimal("4.00000200")


def test_binance_parser_ignores_unknown_events() -> None:
    parser = BinanceDepthParser()
    assert parser.parse({"e": "trade"}) is None
