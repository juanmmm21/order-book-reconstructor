from __future__ import annotations

from order_book_reconstructor.exchanges.binance import BinanceDepthParser
from order_book_reconstructor.exchanges.coinbase import CoinbaseDepthParser
from order_book_reconstructor.models import BookFeedConfig, ExchangeId
from order_book_reconstructor.protocols import DepthMessageParser, ExchangeBookAdapter


class BinanceBookAdapter:
    exchange_id = ExchangeId.BINANCE.value

    def build_websocket_url(self, config: BookFeedConfig, symbol: str) -> str:
        suffix = "@100ms" if config.depth_speed_ms == 100 else ""
        stream = f"{_normalize_binance_symbol(symbol)}@depth{suffix}"
        base = (config.websocket_base_url or "wss://stream.binance.com:9443").rstrip("/")
        return f"{base}/ws/{stream}"

    def build_subscribe_messages(
        self,
        config: BookFeedConfig,
        symbol: str,
    ) -> list[dict[str, object]]:
        return []

    def create_parser(self) -> DepthMessageParser:
        return BinanceDepthParser()

    def unwrap_payload(self, raw_message: dict[str, object]) -> dict[str, object]:
        data = raw_message.get("data")
        if isinstance(data, dict):
            return data
        return raw_message

    def requires_rest_snapshot(self) -> bool:
        return True


class CoinbaseBookAdapter:
    exchange_id = ExchangeId.COINBASE.value

    def build_websocket_url(self, config: BookFeedConfig, symbol: str) -> str:
        return "wss://ws-feed.exchange.coinbase.com"

    def build_subscribe_messages(
        self,
        config: BookFeedConfig,
        symbol: str,
    ) -> list[dict[str, object]]:
        return [
            {
                "type": "subscribe",
                "product_ids": [symbol],
                "channels": ["level2"],
            }
        ]

    def create_parser(self) -> DepthMessageParser:
        return CoinbaseDepthParser()

    def unwrap_payload(self, raw_message: dict[str, object]) -> dict[str, object]:
        return raw_message

    def requires_rest_snapshot(self) -> bool:
        return False


def create_exchange_adapter(exchange: ExchangeId) -> ExchangeBookAdapter:
    if exchange == ExchangeId.BINANCE:
        return BinanceBookAdapter()
    if exchange == ExchangeId.COINBASE:
        return CoinbaseBookAdapter()
    raise ValueError(f"unsupported exchange: {exchange.value}")


def _normalize_binance_symbol(symbol: str) -> str:
    return symbol.replace("-", "").replace("/", "").lower()
