from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import logging
import signal
import time
from pathlib import Path

from .basis_strategy import BasisArbStrategy
from .config import AppConfig
from .dispatch import BookUpdate, LatestBookQueue, consume_order_events
from .executor import PairExecutor
from .market_data import OkxBookStream
from .models import Direction, OrderAction, ParentOrderState
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
    executor.restore()
    strategy: BasisArbStrategy | None = None
    book_queue = LatestBookQueue()
    order_queue: asyncio.Queue = asyncio.Queue(maxsize=4096)
    book_processing_task: asyncio.Task | None = None

    async def on_book(inst_id, best_bid, best_ask):
        # Keep the WS callback O(1): record the quote and enqueue only the
        # latest decision input. Slow strategy/execution work runs elsewhere.
        received_at = time.perf_counter()
        client.update_orderbook(inst_id, best_bid=best_bid, best_ask=best_ask)
        executor.record_book(inst_id, best_bid, best_ask)
        coalesced = book_queue.submit(BookUpdate(
            inst_id=inst_id,
            best_bid=best_bid,
            best_ask=best_ask,
            received_at=received_at,
        ))
        executor.record_bbo_queue(coalesced)

    book_stream = OkxBookStream([config.spot_inst_id, config.swap_inst_id], on_book, demo=config.demo)
    stop_event = asyncio.Event()

    async def book_processing_loop() -> None:
        while not stop_event.is_set():
            updates = await book_queue.get_batch()
            if not updates:
                return
            for update in updates:
                if stop_event.is_set():
                    return
                try:
                    if strategy is not None:
                        await strategy.on_book(
                            update.inst_id,
                            update.best_bid,
                            update.best_ask,
                        )
                    await executor.process_book(
                        update.inst_id,
                        received_at=update.received_at,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logging.exception(
                        "book decision failed for %s (%s)",
                        request_id,
                        update.inst_id,
                    )
                    await executor.notify_runtime_warning(
                        request_id,
                        f"market event processing failed: {exc}",
                    )

    async def public_book_stream_loop() -> None:
        backoff = 1.0
        while not stop_event.is_set():
            try:
                await book_stream.run()
                if stop_event.is_set():
                    return
                message = "public book WebSocket closed unexpectedly"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                message = f"public book WebSocket/callback error: {exc}"
                logging.exception("public book stream failed for %s", request_id)
            await executor.notify_runtime_warning(request_id, message)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=backoff)
                return
            except asyncio.TimeoutError:
                backoff = min(backoff * 2, 30.0)

    book_task = asyncio.create_task(
        public_book_stream_loop(),
        name="okx-public-books",
    )

    async def private_order_stream_loop() -> None:
        backoff = 1.0
        while not stop_event.is_set():
            try:
                await client.subscribe_orders(order_queue.put)
                if stop_event.is_set():
                    return
                message = "private order WebSocket closed unexpectedly"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                message = f"private order WebSocket/order callback error: {exc}"
                logging.exception("private order stream failed for %s", request_id)
            await executor.notify_runtime_warning(request_id, message)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=backoff)
                return
            except asyncio.TimeoutError:
                backoff = min(backoff * 2, 30.0)
    order_task = asyncio.create_task(
        private_order_stream_loop(),
        name="okx-private-orders",
    )

    async def order_event_error(event, exc) -> None:
        logging.error(
            "order event processing failed: %s",
            exc,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        await executor.notify_runtime_warning(
            request_id,
            f"订单回报处理失败: {exc}",
        )

    order_dispatch_task = asyncio.create_task(
        consume_order_events(
            order_queue,
            executor.on_order_event,
            stop_event,
            order_event_error,
        ),
        name="okx-order-event-dispatcher",
    )

    async def status_report_loop() -> None:
        interval = max(1, config.status_report_interval_seconds)
        terminal_states = {"completed", "failed", "canceled", "recovery"}
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
            parent = executor.parents.get(request_id)
            if parent is None:
                continue
            if parent.state.value in terminal_states:
                return
            try:
                await executor.notify_status(request_id)
            except Exception:
                # A status notification must never interrupt order management.
                logging.exception("failed to send execution status for %s", request_id)

    status_task = asyncio.create_task(
        status_report_loop(),
        name=f"execution-status-{request_id}",
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    try:
        await client.wait_for_book([config.spot_inst_id, config.swap_inst_id])
        book_processing_task = asyncio.create_task(
            book_processing_loop(),
            name="okx-book-decision-dispatcher",
        )
        if config.strategy_mode == "basis":
            strategy = BasisArbStrategy(
                executor,
                config.request(request_id),
                config.basis_config(),
                notifier,
            )
            logging.info("basis strategy %s waiting for entry basis", request_id)
            last_reconcile = time.monotonic() - 5
            while not stop_event.is_set() and not strategy.terminal:
                await asyncio.sleep(1)
                await strategy.refresh()
                if time.monotonic() - last_reconcile >= 5:
                    await executor.reconcile()
                    last_reconcile = time.monotonic()
        else:
            parent = executor.parents.get(request_id)
            if parent is None:
                parent = await executor.submit(config.request(request_id))
                logging.info("submitted %s with %d children", request_id, len(parent.children))
            else:
                logging.info("restored %s with %d children in state %s", request_id, len(parent.children), parent.state.value)
                await executor.fail_orphaned_parent(request_id)
            while not stop_event.is_set() and parent.state.value not in {"completed", "failed", "canceled", "recovery"}:
                await asyncio.sleep(5)
                await executor.reconcile()
            if stop_event.is_set() and parent.state.value == "running":
                parent.state = ParentOrderState.RECOVERY
                await executor.reconcile()
    finally:
        status_task.cancel()
        await asyncio.gather(status_task, return_exceptions=True)
        stop_event.set()
        book_queue.stop()
        if strategy is not None:
            await strategy.shutdown()
        try:
            await executor.cancel_active_makers()
        except Exception:
            logging.exception("failed to cancel active Maker orders during shutdown")
        try:
            await executor.notify_terminal(request_id)
        except Exception:
            logging.exception("failed to send terminal execution report for %s", request_id)
        await executor.stop_repricing()
        book_stream.stop()
        tasks = [book_task, order_task, order_dispatch_task]
        if book_processing_task is not None:
            tasks.append(book_processing_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the OKX pair executor")
    parser.add_argument("--request-id", default=None)
    parser.add_argument("--strategy-mode", choices=["pair", "basis"])
    parser.add_argument("--spot-td-mode", choices=["cash", "cross", "isolated"])
    parser.add_argument("--direction", choices=[item.value for item in Direction])
    parser.add_argument("--action", choices=[item.value for item in OrderAction])
    parser.add_argument("--position-mode", choices=["net", "long_short"])
    parser.add_argument("--target-base-qty", type=Decimal)
    parser.add_argument("--child-base-qty", type=Decimal)
    parser.add_argument("--max-unhedged-base-qty", type=Decimal)
    parser.add_argument("--max-hedge-retries", type=int)
    parser.add_argument("--max-maker-attempts", type=int)
    parser.add_argument("--status-report-interval-seconds", type=int)
    parser.add_argument("--maker-reprice-interval-ms", type=int)
    parser.add_argument("--basis-entry-threshold-bp", type=Decimal)
    parser.add_argument("--basis-pause-threshold-bp", type=Decimal)
    parser.add_argument("--basis-resume-threshold-bp", type=Decimal)
    parser.add_argument("--basis-exit-threshold-bp", type=Decimal)
    parser.add_argument("--basis-resume-exposure-base-qty", type=Decimal)
    parser.add_argument("--basis-signal-interval-ms", type=int)
    parser.add_argument("--state-path")
    parser.add_argument("--allow-live", action="store_true", help="allow live OKX endpoint; Demo is the default")
    args = parser.parse_args()
    config = AppConfig.from_env()
    overrides = {}
    if args.strategy_mode is not None:
        overrides["strategy_mode"] = args.strategy_mode
    if args.spot_td_mode is not None:
        overrides["spot_td_mode"] = args.spot_td_mode
    if args.direction is not None:
        overrides["direction"] = Direction(args.direction)
    if args.action is not None:
        overrides["action"] = OrderAction(args.action)
    if args.position_mode is not None:
        overrides["position_mode"] = args.position_mode
    if args.target_base_qty is not None:
        overrides["target_base_qty"] = args.target_base_qty
    if args.child_base_qty is not None:
        overrides["child_base_qty"] = args.child_base_qty
    if args.max_unhedged_base_qty is not None:
        overrides["max_unhedged_base_qty"] = args.max_unhedged_base_qty
    if args.max_hedge_retries is not None:
        overrides["max_hedge_retries"] = args.max_hedge_retries
    if args.max_maker_attempts is not None:
        overrides["max_maker_attempts"] = args.max_maker_attempts
    if args.status_report_interval_seconds is not None:
        overrides["status_report_interval_seconds"] = args.status_report_interval_seconds
    if args.basis_entry_threshold_bp is not None:
        overrides["basis_entry_threshold_bp"] = args.basis_entry_threshold_bp
    if args.basis_pause_threshold_bp is not None:
        overrides["basis_pause_threshold_bp"] = args.basis_pause_threshold_bp
    if args.basis_resume_threshold_bp is not None:
        overrides["basis_resume_threshold_bp"] = args.basis_resume_threshold_bp
    if args.basis_exit_threshold_bp is not None:
        overrides["basis_exit_threshold_bp"] = args.basis_exit_threshold_bp
    if args.basis_resume_exposure_base_qty is not None:
        overrides["basis_resume_exposure_base_qty"] = args.basis_resume_exposure_base_qty
    if args.basis_signal_interval_ms is not None:
        overrides["basis_signal_interval_ms"] = args.basis_signal_interval_ms
    if args.state_path is not None:
        overrides["state_path"] = args.state_path
    if overrides:
        config = replace(config, **overrides)
    if config.strategy_mode not in {"pair", "basis"}:
        raise SystemExit("STRATEGY_MODE must be pair or basis")
    if config.spot_td_mode not in {"cash", "cross", "isolated"}:
        raise SystemExit("SPOT_TD_MODE must be cash, cross or isolated")
    if config.position_mode not in {"net", "long_short"}:
        raise SystemExit("OKX_POSITION_MODE must be net or long_short")
    if not config.demo and not args.allow_live:
        raise SystemExit("live trading blocked: set OKX_DEMO=0 and pass --allow-live explicitly")
    request_id = args.request_id or datetime.now(timezone.utc).strftime("ARB-%Y%m%d-%H%M%S")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(config, request_id))


if __name__ == "__main__":
    main()
