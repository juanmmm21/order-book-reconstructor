from __future__ import annotations

import logging

import httpx

from order_book_reconstructor.exchanges.binance import parse_binance_rest_snapshot
from order_book_reconstructor.models import BookSnapshot, ExchangeId
from order_book_reconstructor.protocols import SnapshotProvider

logger = logging.getLogger(__name__)


class BinanceSnapshotProvider:
    """Obtiene snapshot REST requerido para sincronizar diff depth de Binance."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.binance.com",
        limit: int = 1000,
        timeout_seconds: float = 10.0,
    ) -> None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        self._base_url = base_url.rstrip("/")
        self._limit = limit
        self._timeout_seconds = timeout_seconds

    async def fetch_snapshot(self, symbol: str) -> BookSnapshot:
        normalized = symbol.replace("-", "").replace("/", "").upper()
        url = f"{self._base_url}/api/v3/depth"
        params = {"symbol": normalized, "limit": str(self._limit)}

        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPError as exc:
            logger.exception("binance snapshot request failed symbol=%s", normalized)
            raise RuntimeError(f"failed to fetch binance snapshot for {normalized}") from exc

        if not isinstance(payload, dict):
            raise RuntimeError("binance snapshot payload must be a JSON object")

        return parse_binance_rest_snapshot(normalized, payload)


def create_snapshot_provider(exchange: ExchangeId) -> SnapshotProvider | None:
    if exchange == ExchangeId.BINANCE:
        return BinanceSnapshotProvider()
    return None
