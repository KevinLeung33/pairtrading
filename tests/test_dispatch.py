import asyncio
from decimal import Decimal

import pytest

from okx_pair_executor.dispatch import BookUpdate, LatestBookQueue, consume_order_events


@pytest.mark.asyncio
async def test_latest_book_queue_keeps_only_newest_quote_per_instrument():
    queue = LatestBookQueue()
    assert queue.submit(BookUpdate("BTC-USDT-SWAP", Decimal("100"), Decimal("101"), 1.0)) is False
    assert queue.submit(BookUpdate("BTC-USDT-SWAP", Decimal("102"), Decimal("103"), 2.0)) is True
    assert queue.submit(BookUpdate("BTC-USDT", Decimal("99"), Decimal("100"), 3.0)) is False

    batch = await queue.get_batch()

    assert [(item.inst_id, item.best_bid) for item in batch] == [
        ("BTC-USDT-SWAP", Decimal("102")),
        ("BTC-USDT", Decimal("99")),
    ]
    assert queue.received == 3
    assert queue.coalesced == 1
    assert queue.dispatched == 2
    assert queue.pending == 0


@pytest.mark.asyncio
async def test_order_event_consumer_keeps_ingress_available_while_handler_waits():
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=2)
    stop_event = asyncio.Event()
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    seen: list[int] = []

    async def handler(value: int) -> None:
        seen.append(value)
        if value == 1:
            first_started.set()
            await release_first.wait()

    task = asyncio.create_task(consume_order_events(queue, handler, stop_event))
    await queue.put(1)
    await asyncio.wait_for(first_started.wait(), timeout=1)
    await queue.put(2)

    assert queue.qsize() == 1
    release_first.set()
    await asyncio.wait_for(queue.join(), timeout=1)
    stop_event.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert seen == [1, 2]

@pytest.mark.asyncio
async def test_order_event_consumer_survives_handler_error():
    queue: asyncio.Queue[int] = asyncio.Queue()
    stop_event = asyncio.Event()
    handled: list[int] = []
    errors: list[str] = []

    async def handler(value: int) -> None:
        if value == 1:
            raise RuntimeError("bad event")
        handled.append(value)

    async def error_handler(event: int, exc: Exception) -> None:
        errors.append(f"{event}:{exc}")

    task = asyncio.create_task(consume_order_events(queue, handler, stop_event, error_handler))
    await queue.put(1)
    await queue.put(2)
    await asyncio.wait_for(queue.join(), timeout=1)
    stop_event.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert handled == [2]
    assert errors == ["1:bad event"]