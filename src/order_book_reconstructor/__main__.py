from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from collections.abc import Sequence

from order_book_reconstructor.feed_service import LiveOrderBookService
from order_book_reconstructor.models import (
    BookFeedConfig,
    ConnectionEvent,
    ExchangeId,
    OrderBookView,
    UpdateApplyResult,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconstructor L2 en tiempo real para streams de profundidad de exchanges.",
    )
    parser.add_argument(
        "--exchange",
        choices=[exchange.value for exchange in ExchangeId],
        default=ExchangeId.BINANCE.value,
    )
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--depth-speed-ms", type=int, choices=[100, 1000], default=100)
    parser.add_argument("--top-levels", type=int, default=10)
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser


async def _run_service(
    exchange: ExchangeId,
    symbols: Sequence[str],
    depth_speed_ms: int,
    top_levels: int,
) -> None:
    config = BookFeedConfig(
        exchange=exchange,
        symbols=tuple(symbols),
        depth_speed_ms=depth_speed_ms,
        top_levels=top_levels,
    )
    service = LiveOrderBookService(config=config)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _request_shutdown)

    async def on_book_update(view: OrderBookView) -> None:
        best_bid = view.best_bid.price if view.best_bid else None
        best_ask = view.best_ask.price if view.best_ask else None
        logging.getLogger(__name__).info(
            "book exchange=%s symbol=%s bid=%s ask=%s spread=%s levels=%s/%s",
            view.exchange.value,
            view.symbol,
            best_bid,
            best_ask,
            view.spread,
            len(view.bids),
            len(view.asks),
        )

    async def on_connection_event(event: ConnectionEvent) -> None:
        logging.getLogger(__name__).info(
            "connection exchange=%s type=%s symbol=%s message=%s attempt=%s",
            event.exchange.value,
            event.event_type,
            event.symbol,
            event.message,
            event.attempt,
        )

    async def on_update_result(symbol: str, result: UpdateApplyResult) -> None:
        if result == UpdateApplyResult.OUT_OF_SYNC:
            logging.getLogger(__name__).warning("out of sync symbol=%s", symbol)

    service_task = asyncio.create_task(
        service.run(
            on_book_update=on_book_update,
            on_connection_event=on_connection_event,
            on_update_result=on_update_result,
        ),
    )
    stop_task = asyncio.create_task(stop_event.wait())

    done, _ = await asyncio.wait({service_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)

    for task in done:
        if task is service_task and service_task.exception() is not None:
            raise service_task.exception()  # type: ignore[misc]

    await service.stop()
    if not service_task.done():
        service_task.cancel()
        await asyncio.gather(service_task, return_exceptions=True)


def build_replay_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="order-book-reconstructor replay",
        description="Valida diffs Binance grabados (secuencia, resync) y emite un flujo L2.",
    )
    parser.add_argument("--input", nargs="+", required=True, help="JSONL grabados (mezclados).")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output", default=None, help="Flujo validado JSONL para el simulador.")
    parser.add_argument("--report", default=None, help="Informe JSON (por defecto stdout).")
    parser.add_argument("--emit-levels", type=int, default=1000)
    parser.add_argument("--verify-depth", type=int, default=200)
    parser.add_argument(
        "--drop-diff-every",
        type=int,
        default=0,
        help="Borra 1 de cada N diffs para ejercitar la resincronización.",
    )
    return parser


def replay_main(argv: Sequence[str]) -> None:
    import json

    from order_book_reconstructor.replay import read_jsonl, replay

    args = build_replay_parser().parse_args(argv)
    sink = open(args.output, "w", encoding="utf-8") if args.output else None  # noqa: SIM115

    def write(record: dict[str, object]) -> None:
        if sink is not None:
            sink.write(json.dumps(record) + "\n")

    try:
        report = replay(
            read_jsonl(args.input),
            symbol=args.symbol.upper(),
            emit=write if sink else None,
            emit_levels=args.emit_levels,
            verify_depth=args.verify_depth,
            drop_diff_every=args.drop_diff_every,
        )
    finally:
        if sink is not None:
            sink.close()
    rendered = json.dumps(report.as_dict(), indent=2)
    if args.report:
        with open(args.report, "w", encoding="utf-8") as handle:
            handle.write(rendered + "\n")
    else:
        print(rendered)


def main() -> None:
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "replay":
        replay_main(sys.argv[2:])
        return
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    exchange = ExchangeId(args.exchange)
    asyncio.run(
        _run_service(
            exchange=exchange,
            symbols=args.symbols,
            depth_speed_ms=args.depth_speed_ms,
            top_levels=args.top_levels,
        ),
    )


if __name__ == "__main__":
    main()
