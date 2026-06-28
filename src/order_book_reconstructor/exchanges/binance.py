from __future__ import annotations

from decimal import Decimal

from order_book_reconstructor.models import (
    BookLevelChange,
    BookSide,
    BookSnapshot,
    DepthUpdate,
    ExchangeId,
    PriceLevel,
    utc_from_millis,
)


class BinanceDepthParser:
    """Parser de `depthUpdate` y snapshots parciales del stream diff depth de Binance."""

    def parse(self, payload: dict[str, object]) -> DepthUpdate | BookSnapshot | None:
        event_type = payload.get("e")
        if event_type == "depthUpdate":
            return self._parse_depth_update(payload)
        if event_type == "depthSnapshot":
            return self._parse_snapshot(payload)
        return None

    def _parse_depth_update(self, payload: dict[str, object]) -> DepthUpdate:
        symbol = _require_str(payload, "s")
        event_time = utc_from_millis(_require_int(payload, "E"))
        bids = _parse_level_changes(payload.get("b"), BookSide.BID)
        asks = _parse_level_changes(payload.get("a"), BookSide.ASK)

        return DepthUpdate(
            exchange=ExchangeId.BINANCE,
            symbol=symbol,
            bids=bids,
            asks=asks,
            event_time=event_time,
            first_update_id=_require_int(payload, "U"),
            final_update_id=_require_int(payload, "u"),
            raw_payload=payload,
        )

    def _parse_snapshot(self, payload: dict[str, object]) -> BookSnapshot:
        symbol = _require_str(payload, "s")
        bids = _parse_price_levels(payload.get("bids"))
        asks = _parse_price_levels(payload.get("asks"))
        last_update_raw = payload.get("lastUpdateId")
        update_id = last_update_raw if isinstance(last_update_raw, int) else None

        return BookSnapshot(
            exchange=ExchangeId.BINANCE,
            symbol=symbol,
            bids=bids,
            asks=asks,
            last_update_id=update_id,
        )


def parse_binance_rest_snapshot(symbol: str, payload: dict[str, object]) -> BookSnapshot:
    """Normaliza la respuesta REST `/api/v3/depth` de Binance."""
    bids = _parse_price_levels(payload.get("bids"))
    asks = _parse_price_levels(payload.get("asks"))
    return BookSnapshot(
        exchange=ExchangeId.BINANCE,
        symbol=symbol,
        bids=bids,
        asks=asks,
        last_update_id=_require_int(payload, "lastUpdateId"),
    )


def _parse_level_changes(
    raw_levels: object,
    side: BookSide,
) -> tuple[BookLevelChange, ...]:
    if not isinstance(raw_levels, list):
        return ()

    changes: list[BookLevelChange] = []
    for entry in raw_levels:
        if not isinstance(entry, list) or len(entry) != 2:
            continue
        price_raw, quantity_raw = entry
        if not isinstance(price_raw, str) or not isinstance(quantity_raw, str):
            continue
        changes.append(
            BookLevelChange(
                side=side,
                price=Decimal(price_raw),
                quantity=Decimal(quantity_raw),
            ),
        )
    return tuple(changes)


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


def _require_int(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"expected integer field '{key}'")
    return value
