from __future__ import annotations

from decimal import Decimal

from order_book_reconstructor.exchanges.coinbase import CoinbaseDepthParser
from order_book_reconstructor.models import BookSide, ExchangeId


def test_coinbase_snapshot_parser() -> None:
    parser = CoinbaseDepthParser()
    payload = {
        "type": "snapshot",
        "product_id": "BTC-USD",
        "bids": [["10101.10", "0.45054140"]],
        "asks": [["10102.55", "1.52000000"]],
    }

    snapshot = parser.parse(payload)

    assert snapshot is not None
    assert snapshot.exchange == ExchangeId.COINBASE
    assert snapshot.symbol == "BTC-USD"
    assert snapshot.bids[0].price == Decimal("10101.10")
    assert snapshot.asks[0].price == Decimal("10102.55")


def test_coinbase_l2_update_parser() -> None:
    parser = CoinbaseDepthParser()
    payload = {
        "type": "l2update",
        "product_id": "BTC-USD",
        "time": "2014-11-07T08:19:27.028459Z",
        "changes": [["buy", "10101.80", "0.162567"]],
    }

    update = parser.parse(payload)

    assert update is not None
    assert update.bids[0].side == BookSide.BID
    assert update.bids[0].price == Decimal("10101.80")
    assert update.bids[0].quantity == Decimal("0.162567")
