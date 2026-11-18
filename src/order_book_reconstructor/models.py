from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal


class ExchangeId(StrEnum):
    BINANCE = "binance"
    COINBASE = "coinbase"


class BookSide(StrEnum):
    BID = "bid"
    ASK = "ask"


class ConnectionState(StrEnum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    RESYNCING = "resyncing"
    STOPPED = "stopped"


class UpdateApplyResult(StrEnum):
    APPLIED = "applied"
    IGNORED_STALE = "ignored_stale"
    OUT_OF_SYNC = "out_of_sync"
    NOT_READY = "not_ready"


@dataclass(frozen=True, slots=True)
class PriceLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        if self.price <= Decimal("0"):
            raise ValueError("price must be positive")
        if self.quantity < Decimal("0"):
            raise ValueError("quantity must be non-negative")


@dataclass(frozen=True, slots=True)
class BookLevelChange:
    side: BookSide
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    exchange: ExchangeId
    symbol: str
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    last_update_id: int | None = None
    event_time: datetime | None = None


@dataclass(frozen=True, slots=True)
class DepthUpdate:
    exchange: ExchangeId
    symbol: str
    bids: tuple[BookLevelChange, ...]
    asks: tuple[BookLevelChange, ...]
    event_time: datetime
    first_update_id: int | None = None
    final_update_id: int | None = None
    raw_payload: dict[str, object] | None = None


@dataclass(frozen=True, slots=True)
class OrderBookView:
    exchange: ExchangeId
    symbol: str
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    last_update_id: int | None
    best_bid: PriceLevel | None
    best_ask: PriceLevel | None
    spread: Decimal | None
    mid_price: Decimal | None
    event_time: datetime


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_multiplier: float = 2.0
    jitter_ratio: float = 0.1

    def __post_init__(self) -> None:
        if self.initial_delay_seconds <= 0:
            raise ValueError("initial_delay_seconds must be positive")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max_delay_seconds must be >= initial_delay_seconds")
        if self.backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")
        if not 0.0 <= self.jitter_ratio <= 1.0:
            raise ValueError("jitter_ratio must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class BookFeedConfig:
    exchange: ExchangeId
    symbols: tuple[str, ...]
    depth_speed_ms: int = 100
    reconnect_policy: ReconnectPolicy = ReconnectPolicy()
    ping_interval_seconds: float = 20.0
    ping_timeout_seconds: float = 20.0
    close_timeout_seconds: float = 10.0
    top_levels: int = 10
    websocket_base_url: str | None = None
    rest_base_url: str | None = None
    snapshot_limit: int = 1000

    def __post_init__(self) -> None:
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        if self.depth_speed_ms not in {100, 1000}:
            raise ValueError("depth_speed_ms must be 100 or 1000")
        if self.top_levels <= 0:
            raise ValueError("top_levels must be positive")


ConnectionEventType = Literal[
    "connected",
    "disconnected",
    "reconnecting",
    "resyncing",
    "stopped",
    "error",
]


@dataclass(frozen=True, slots=True)
class ConnectionEvent:
    exchange: ExchangeId
    event_type: ConnectionEventType
    message: str
    timestamp: datetime
    symbol: str | None = None
    attempt: int | None = None


def utc_from_millis(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=UTC)


def utc_from_iso8601(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
