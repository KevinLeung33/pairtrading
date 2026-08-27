from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable


@dataclass(frozen=True)
class BookUpdate:
    inst_id: str
    best_bid: Decimal
    best_ask: Decimal
    received_at: float


class LatestBookQueue:
    """Coalesce public-book updates while retaining the newest quote per instrument.

    The execution spread tracker is updated at ingress before this queue, so
    coalescing only affects decision processing and not market observations.
    """

    def __init__(self) -> None:
        self._latest: dict[str, BookUpdate] = {}
        self._ready = asyncio.Event()
        self._stopped = False
        self.received = 0
        self.coalesced = 0
        self.dispatched = 0

    def submit(self, update: BookUpdate) -> bool:
        if self._stopped:
            return False
        was_pending = update.inst_id in self._latest
        if was_pending:
            self.coalesced += 1
        self.received += 1
        self._latest[update.inst_id] = update
        self._ready.set()
        return was_pending

    async def get_batch(self) -> list[BookUpdate]:
        await self._ready.wait()
        batch = list(self._latest.values())
        self._latest.clear()
        self._ready.clear()
        self.dispatched += len(batch)
        return batch

    def stop(self) -> None:
        self._stopped = True
        self._ready.set()

    @property
    def pending(self) -> int:
        return len(self._latest)


async def consume_order_events(
    queue: "asyncio.Queue[Any]",
    handler: Callable[[Any], Awaitable[None]],
    stop_event: asyncio.Event,
    error_handler: Callable[[Any, Exception], Awaitable[None]] | None = None,
) -> None:
    """Consume private order events serially without blocking the WS reader.

    One malformed event or transient handler error must not terminate the
    dispatcher and silently stop all later order updates.
    """

    while not stop_event.is_set():
        try:
            event = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        try:
            await handler(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if error_handler is not None:
                try:
                    await error_handler(event, exc)
                except Exception:
                    logging.exception("order event error handler failed")
            else:
                logging.exception("order event handler failed")
        finally:
            queue.task_done()