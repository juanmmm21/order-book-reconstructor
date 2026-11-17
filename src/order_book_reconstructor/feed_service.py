from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import websockets
from websockets.asyncio.client import ClientConnection

from order_book_reconstructor.exchanges import create_exchange_adapter
from order_book_reconstructor.models import (
    BookFeedConfig,
    BookSnapshot,
    ConnectionEvent,
    ConnectionState,
    DepthUpdate,
    OrderBookView,
    UpdateApplyResult,
)
from order_book_reconstructor.protocols import (
    BookViewHandler,
    ConnectionEventHandler,
    DepthMessageParser,
    SnapshotProvider,
    UpdateResultHandler,
)
from order_book_reconstructor.reconnect import compute_backoff_delay
from order_book_reconstructor.reconstructor import OrderBookReconstructor
from order_book_reconstructor.snapshot import create_snapshot_provider
from order_book_reconstructor.sync import DepthSynchronizer, DiffOutcome, SyncStats

_OUTCOME_TO_RESULT = {
    DiffOutcome.APPLIED: UpdateApplyResult.APPLIED,
    DiffOutcome.STALE: UpdateApplyResult.IGNORED_STALE,
    DiffOutcome.GAP: UpdateApplyResult.OUT_OF_SYNC,
    DiffOutcome.BUFFERED: UpdateApplyResult.NOT_READY,
}

logger = logging.getLogger(__name__)


class LiveOrderBookService:
    """Servicio async que mantiene libros L2 sincronizados desde streams WebSocket."""

    def __init__(
        self,
        config: BookFeedConfig,
        *,
        snapshot_provider: SnapshotProvider | None = None,
    ) -> None:
        self._config = config
        self._adapter = create_exchange_adapter(config.exchange)
        self._snapshot_provider = snapshot_provider or create_snapshot_provider(
            config.exchange,
            base_url=config.rest_base_url,
            limit=config.snapshot_limit,
        )
        self.last_sync_stats: SyncStats | None = None
        self._books: dict[str, OrderBookReconstructor] = {
            symbol: OrderBookReconstructor(config.exchange, symbol) for symbol in config.symbols
        }
        self._state = ConnectionState.DISCONNECTED
        self._stop_event = asyncio.Event()
        self._book_queue: asyncio.Queue[OrderBookView | None] = asyncio.Queue()
        self._connection: ClientConnection | None = None

    @property
    def state(self) -> ConnectionState:
        return self._state

    async def run(
        self,
        on_book_update: BookViewHandler,
        on_connection_event: ConnectionEventHandler | None = None,
        on_update_result: UpdateResultHandler | None = None,
    ) -> None:
        attempt = 0
        self._stop_event.clear()

        while not self._stop_event.is_set():
            attempt += 1
            try:
                await self._connect_session(
                    on_book_update,
                    on_connection_event,
                    on_update_result,
                    attempt,
                )
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(
                    "book feed session failed exchange=%s attempt=%s",
                    self._config.exchange.value,
                    attempt,
                )
                await self._emit_connection_event(
                    on_connection_event,
                    "error",
                    f"session error: {exc}",
                    attempt=attempt,
                )

            if self._stop_event.is_set():
                break

            self._state = ConnectionState.RECONNECTING
            delay = compute_backoff_delay(attempt, self._config.reconnect_policy)
            await self._emit_connection_event(
                on_connection_event,
                "reconnecting",
                f"retrying in {delay:.2f}s",
                attempt=attempt,
            )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except TimeoutError:
                continue

        self._state = ConnectionState.STOPPED
        await self._book_queue.put(None)
        await self._emit_connection_event(on_connection_event, "stopped", "book feed stopped")

    async def stop(self) -> None:
        self._stop_event.set()
        connection = self._connection
        if connection is not None:
            await connection.close()

    async def books(self) -> AsyncIterator[OrderBookView]:
        while True:
            view = await self._book_queue.get()
            if view is None:
                break
            yield view

    async def _connect_session(
        self,
        on_book_update: BookViewHandler,
        on_connection_event: ConnectionEventHandler | None,
        on_update_result: UpdateResultHandler | None,
        attempt: int,
    ) -> None:
        if len(self._config.symbols) != 1:
            raise ValueError("current implementation supports one symbol per session")

        symbol = self._config.symbols[0]
        book = self._books[symbol]
        book.reset()

        url = self._adapter.build_websocket_url(self._config, symbol)
        self._state = ConnectionState.CONNECTING
        logger.info(
            "connecting exchange=%s symbol=%s url=%s",
            self._config.exchange.value,
            symbol,
            url,
        )

        async with websockets.connect(
            url,
            ping_interval=self._config.ping_interval_seconds,
            ping_timeout=self._config.ping_timeout_seconds,
            close_timeout=self._config.close_timeout_seconds,
            open_timeout=self._config.close_timeout_seconds,
            max_queue=None,
        ) as connection:
            self._connection = connection
            self._state = ConnectionState.CONNECTED
            await self._emit_connection_event(
                on_connection_event,
                "connected",
                "depth websocket session established",
                symbol=symbol,
                attempt=attempt if attempt > 1 else None,
            )

            await self._send_subscriptions(connection, symbol)
            parser = self._adapter.create_parser()

            if self._adapter.requires_rest_snapshot():
                await self._consume_synced(
                    connection,
                    parser,
                    book,
                    symbol,
                    on_book_update,
                    on_connection_event,
                    on_update_result,
                )
                return

            async for raw_message in connection:
                if self._stop_event.is_set():
                    break

                try:
                    parsed = self._parse_message(parser, raw_message)
                except Exception:
                    logger.exception(
                        "failed to parse depth message exchange=%s",
                        self._config.exchange.value,
                    )
                    continue

                if parsed is None:
                    continue

                if isinstance(parsed, BookSnapshot):
                    book.load_snapshot(parsed)
                    view = book.view(self._config.top_levels)
                    await self._publish_view(view, on_book_update)
                    continue

                result = book.apply_update(parsed)
                if on_update_result is not None:
                    await on_update_result(symbol, result)

                if result == UpdateApplyResult.OUT_OF_SYNC:
                    self._state = ConnectionState.RESYNCING
                    await self._emit_connection_event(
                        on_connection_event,
                        "resyncing",
                        "sequence gap detected, reloading snapshot",
                        symbol=symbol,
                    )
                    await self._bootstrap_from_snapshot(book, symbol, on_connection_event)
                    retry_result = book.apply_update(parsed)
                    if on_update_result is not None:
                        await on_update_result(symbol, retry_result)
                    if retry_result != UpdateApplyResult.APPLIED:
                        continue

                if result == UpdateApplyResult.APPLIED:
                    view = book.view(self._config.top_levels)
                    await self._publish_view(view, on_book_update)

    async def _consume_synced(
        self,
        connection: ClientConnection,
        parser: DepthMessageParser,
        book: OrderBookReconstructor,
        symbol: str,
        on_book_update: BookViewHandler,
        on_connection_event: ConnectionEventHandler | None,
        on_update_result: UpdateResultHandler | None,
    ) -> None:
        """Binance: buffer diffs from the open socket, then fetch the snapshot (never before)."""
        sync = DepthSynchronizer(book)
        self.last_sync_stats = sync.stats
        snapshot_task: asyncio.Task[BookSnapshot] | None = None

        def request_snapshot() -> asyncio.Task[BookSnapshot]:
            if self._snapshot_provider is None:
                raise RuntimeError("snapshot provider is required for this exchange")
            self._state = ConnectionState.RESYNCING
            return asyncio.create_task(self._snapshot_provider.fetch_snapshot(symbol))

        await self._emit_connection_event(
            on_connection_event,
            "resyncing",
            "buffering diffs, fetching REST snapshot",
            symbol=symbol,
        )
        snapshot_task = request_snapshot()
        try:
            async for raw_message in connection:
                if self._stop_event.is_set():
                    break
                try:
                    parsed = self._parse_message(parser, raw_message)
                except Exception:
                    logger.exception("failed to parse depth message symbol=%s", symbol)
                    continue
                if not isinstance(parsed, DepthUpdate):
                    continue

                outcome = sync.on_diff(parsed)
                if on_update_result is not None:
                    await on_update_result(symbol, _OUTCOME_TO_RESULT[outcome])
                if outcome is DiffOutcome.GAP:
                    await self._emit_connection_event(
                        on_connection_event,
                        "resyncing",
                        f"sequence gap after update {sync.stats.gap_events[-1][0]}, resyncing",
                        symbol=symbol,
                    )
                if sync.needs_snapshot:
                    if snapshot_task is None:
                        snapshot_task = request_snapshot()
                    elif snapshot_task.done():
                        snapshot = snapshot_task.result()
                        snapshot_task = None
                        if sync.on_snapshot(snapshot):
                            self._state = ConnectionState.CONNECTED
                            logger.info(
                                "book synced symbol=%s last_update_id=%s buffered_max=%s",
                                symbol,
                                sync.last_update_id,
                                sync.stats.max_buffered,
                            )
                            await self._publish_view(
                                book.view(self._config.top_levels), on_book_update
                            )
                        else:
                            snapshot_task = request_snapshot()
                    continue
                if outcome is DiffOutcome.APPLIED:
                    await self._publish_view(book.view(self._config.top_levels), on_book_update)
        finally:
            if snapshot_task is not None and not snapshot_task.done():
                snapshot_task.cancel()

    async def _bootstrap_from_snapshot(
        self,
        book: OrderBookReconstructor,
        symbol: str,
        on_connection_event: ConnectionEventHandler | None,
    ) -> None:
        if self._snapshot_provider is None:
            raise RuntimeError("snapshot provider is required for this exchange")

        self._state = ConnectionState.RESYNCING
        await self._emit_connection_event(
            on_connection_event,
            "resyncing",
            "fetching REST depth snapshot",
            symbol=symbol,
        )
        snapshot = await self._snapshot_provider.fetch_snapshot(symbol)
        book.load_snapshot(snapshot)
        logger.info(
            "snapshot loaded exchange=%s symbol=%s last_update_id=%s",
            self._config.exchange.value,
            symbol,
            snapshot.last_update_id,
        )

    async def _send_subscriptions(self, connection: ClientConnection, symbol: str) -> None:
        for message in self._adapter.build_subscribe_messages(self._config, symbol):
            await connection.send(json.dumps(message))

    def _parse_message(
        self,
        parser: DepthMessageParser,
        raw_message: str | bytes,
    ) -> BookSnapshot | DepthUpdate | None:
        decoded = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message
        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            return None
        normalized = self._adapter.unwrap_payload(payload)
        return parser.parse(normalized)

    async def _publish_view(self, view: OrderBookView, handler: BookViewHandler) -> None:
        try:
            await handler(view)
        except Exception:
            logger.exception("book view handler failed")
        await self._book_queue.put(view)

    async def _emit_connection_event(
        self,
        handler: ConnectionEventHandler | None,
        event_type: str,
        message: str,
        *,
        symbol: str | None = None,
        attempt: int | None = None,
    ) -> None:
        if handler is None:
            return

        event = ConnectionEvent(
            exchange=self._config.exchange,
            event_type=event_type,  # type: ignore[arg-type]
            message=message,
            timestamp=datetime.now(tz=UTC),
            symbol=symbol,
            attempt=attempt,
        )
        try:
            await handler(event)
        except Exception:
            logger.exception("connection event handler failed")
