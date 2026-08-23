from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from datetime import datetime, timezone
from pathlib import Path

from .config import AppConfig
from .executor import PairExecutor
from .market_data import OkxBookStream
from .models import ParentOrderState
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

        while not stop_event.is_set() and parent.state.value not in {"completed", "failed", "canceled"}:
            await asyncio.sleep(5)
            await executor.reconcile()
        if stop_event.is_set() and parent.state.value == "running":
            parent.state = ParentOrderState.RECOVERY
            await executor.reconcile()
    finally:
        book_stream.stop()
        for task in (book_task, order_task):
            task.cancel()
        await asyncio.gather(book_task, order_task, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the OKX pair executor")
    parser.add_argument("--request-id", default=None)
    parser.add_argument("--allow-live", action="store_true", help="allow live OKX endpoint; Demo is the default")
    args = parser.parse_args()
    config = AppConfig.from_env()
    if not config.demo and not args.allow_live:
        raise SystemExit("live trading blocked: set OKX_DEMO=0 and pass --allow-live explicitly")
    request_id = args.request_id or datetime.now(timezone.utc).strftime("ARB-%Y%m%d-%H%M%S")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(config, request_id))


if __name__ == "__main__":
    main()
