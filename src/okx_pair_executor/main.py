from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from decimal import Decimal
import logging
import signal
from datetime import datetime, timezone
from pathlib import Path

from .config import AppConfig
from .executor import PairExecutor
from .market_data import OkxBookStream
from .models import Direction, ParentOrderState
from .notifier import LarkNotifier
from .okx_client import OkxV5Client
from .persistence import JsonStateStore


async def run(config: AppConfig, request_id: str) -> None:
    client = OkxV5Client(
        config.api_key,
        config.secret_key,
        config.passphrase,
        demo=config.demo,
    )
    notifier = LarkNotifier(config.lark_webhook_url, config.lark_secret) if config.lark_webhook_url else None
    Path(config.state_path).parent.mkdir(parents=True, exist_ok=True)
    executor = PairExecutor(client, notifier, JsonStateStore(config.state_path))

    async def on_book(inst_id, best_bid, best_ask):
        client.update_orderbook(inst_id, best_bid=best_bid, best_ask=best_ask)
        await executor.on_book(inst_id, best_bid, best_ask)

    book_stream = OkxBookStream([config.spot_inst_id, config.swap_inst_id], on_book, demo=config.demo)
    book_task = asyncio.create_task(book_stream.run(), name="okx-public-books")
    order_task = asyncio.create_task(client.subscribe_orders(executor.on_order_event), name="okx-private-orders")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    try:
        await client.wait_for_book([config.spot_inst_id, config.swap_inst_id])
        parent = await executor.submit(config.request(request_id))
        logging.info("submitted %s with %d children", request_id, len(parent.children))

        while not stop_event.is_set() and parent.state.value not in {"completed", "failed", "canceled", "recovery"}:
            await asyncio.sleep(5)
            await executor.reconcile()
        if stop_event.is_set() and parent.state.value == "running":
            parent.state = ParentOrderState.RECOVERY
            await executor.reconcile()
    finally:
        try:
            await executor.cancel_active_makers()
        except Exception:
            logging.exception("failed to cancel active Maker orders during shutdown")
        await executor.stop_repricing()
        book_stream.stop()
        for task in (book_task, order_task):
            task.cancel()
        await asyncio.gather(book_task, order_task, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the OKX pair executor")
    parser.add_argument("--request-id", default=None)
    parser.add_argument("--direction", choices=[item.value for item in Direction])
    parser.add_argument("--target-base-qty", type=Decimal)
    parser.add_argument("--child-base-qty", type=Decimal)
    parser.add_argument("--max-unhedged-base-qty", type=Decimal)
    parser.add_argument("--max-hedge-retries", type=int)
    parser.add_argument("--maker-reprice-interval-ms", type=int)
    parser.add_argument("--state-path")
    parser.add_argument("--allow-live", action="store_true", help="allow live OKX endpoint; Demo is the default")
    args = parser.parse_args()
    config = AppConfig.from_env()
    overrides = {}
    if args.direction is not None:
        overrides["direction"] = Direction(args.direction)
    if args.target_base_qty is not None:
        overrides["target_base_qty"] = args.target_base_qty
    if args.child_base_qty is not None:
        overrides["child_base_qty"] = args.child_base_qty
    if args.max_unhedged_base_qty is not None:
        overrides["max_unhedged_base_qty"] = args.max_unhedged_base_qty
    if args.max_hedge_retries is not None:
        overrides["max_hedge_retries"] = args.max_hedge_retries
    if args.maker_reprice_interval_ms is not None:
        overrides["maker_reprice_interval_ms"] = args.maker_reprice_interval_ms
    if args.state_path is not None:
        overrides["state_path"] = args.state_path
    if overrides:
        config = replace(config, **overrides)
    if not config.demo and not args.allow_live:
        raise SystemExit("live trading blocked: set OKX_DEMO=0 and pass --allow-live explicitly")
    request_id = args.request_id or datetime.now(timezone.utc).strftime("ARB-%Y%m%d-%H%M%S")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(config, request_id))


if __name__ == "__main__":
    main()
