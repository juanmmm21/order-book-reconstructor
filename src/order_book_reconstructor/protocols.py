from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Protocol

from order_book_reconstructor.models import (
    BookFeedConfig,
    BookSnapshot,
    ConnectionEvent,
    DepthUpdate,
    OrderBookView,
    UpdateApplyResult,
)


class DepthMessageParser(Protocol):
    """Contrato para transformar payloads crudos en snapshots o actualizaciones L2."""

    def parse(self, payload: dict[str, object]) -> DepthUpdate | BookSnapshot | None:
        ...


class ExchangeBookAdapter(Protocol):
    """Contrato que encapsula la lógica específica de profundidad por exchange."""

    exchange_id: str

    def build_websocket_url(self, config: BookFeedConfig, symbol: str) -> str:
        ...

    def build_subscribe_messages(
        self,
        config: BookFeedConfig,
        symbol: str,
    ) -> list[dict[str, object]]:
        ...

    def create_parser(self) -> DepthMessageParser:
        ...

    def unwrap_payload(self, raw_message: dict[str, object]) -> dict[str, object]:
        ...

    def requires_rest_snapshot(self) -> bool:
        ...


class SnapshotProvider(Protocol):
    async def fetch_snapshot(self, symbol: str) -> BookSnapshot:
        ...


BookViewHandler = Callable[[OrderBookView], Awaitable[None]]
ConnectionEventHandler = Callable[[ConnectionEvent], Awaitable[None]]
UpdateResultHandler = Callable[[str, UpdateApplyResult], Awaitable[None]]


class BookReconstructorService(Protocol):
    async def run(
        self,
        on_book_update: BookViewHandler,
        on_connection_event: ConnectionEventHandler | None = None,
        on_update_result: UpdateResultHandler | None = None,
    ) -> None:
        ...

    async def stop(self) -> None:
        ...

    def books(self) -> AsyncIterator[OrderBookView]:
        ...
