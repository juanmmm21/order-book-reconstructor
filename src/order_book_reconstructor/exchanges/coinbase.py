from __future__ import annotations

from decimal import Decimal

from order_book_reconstructor.models import (
    BookLevelChange,
    BookSide,
    BookSnapshot,
    DepthUpdate,
    ExchangeId,
    PriceLevel,
    utc_from_iso8601,
)


class CoinbaseDepthParser:
    """Parser de mensajes `snapshot` y `l2update` del canal level2 de Coinbase."""

    def parse(self, payload: dict[str, object]) -> DepthUpdate | BookSnapshot | None:
        message_type = payload.get("type")
        if message_type == "snapshot":
            return self._parse_snapshot(payload)
        if message_type == "l2update":
            return self._parse_l2_update(payload)
        return None

    def _parse_snapshot(self, payload: dict[str, object]) -> BookSnapshot:
        symbol = _require_str(payload, "product_id")
        bids = _parse_price_levels(payload.get("bids"))
        asks = _parse_price_levels(payload.get("asks"))
        return BookSnapshot(
            exchange=ExchangeId.COINBASE,
            symbol=symbol,
            bids=bids,
            asks=asks,
        )

    def _parse_l2_update(self, payload: dict[str, object]) -> DepthUpdate:
        symbol = _require_str(payload, "product_id")
        event_time = utc_from_iso8601(_require_str(payload, "time"))

        bids: list[BookLevelChange] = []
        asks: list[BookLevelChange] = []
        changes = payload.get("changes")
        if isinstance(changes, list):
            for change in changes:
                if not isinstance(change, list) or len(change) != 3:
                    continue
                side_raw, price_raw, quantity_raw = change
                if not all(isinstance(value, str) for value in (side_raw, price_raw, quantity_raw)):
                    continue
                side = BookSide.BID if side_raw == "buy" else BookSide.ASK
                level = BookLevelChange(
                    side=side,
                    price=Decimal(price_raw),
                    quantity=Decimal(quantity_raw),
                )
                if side == BookSide.BID:
                    bids.append(level)
                else:
                    asks.append(level)

        return DepthUpdate(
            exchange=ExchangeId.COINBASE,
            symbol=symbol,
            bids=tuple(bids),
            asks=tuple(asks),
            event_time=event_time,
            raw_payload=payload,
        )


def _parse_price_levels(raw_levels: object) -> tuple[PriceLevel, ...]:
    if not isinstance(raw_levels, list):
        return ()

    levels: list[PriceLevel] = []
    for entry in raw_levels:
        if not isinstance(entry, list) or len(entry) != 2:
            continue
        price_raw, quantity_raw = entry
        if not isinstance(price_raw, str) or not isinstance(quantity_raw, str):
            continue
        quantity = Decimal(quantity_raw)
        if quantity == Decimal("0"):
            continue
        levels.append(PriceLevel(price=Decimal(price_raw), quantity=quantity))
    return tuple(levels)


def _require_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError(f"expected string field '{key}'")
    return value
