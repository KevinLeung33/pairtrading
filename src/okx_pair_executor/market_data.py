from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any

import websockets


BookHandler = Callable[[str, Decimal, Decimal], Awaitable[None]]


class OkxBookStream:
    """Public books5 stream with reconnect and callback delivery."""

    def __init__(self, inst_ids: list[str], handler: BookHandler, *, demo: bool = True):
        self.inst_ids = inst_ids
        self.handler = handler
        self.url = "wss://wspap.okx.com:8443/ws/v5/public" if demo else "wss://ws.okx.com:8443/ws/v5/public"
        self._stop = asyncio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=10) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": [
                        {"channel": "books5", "instId": inst_id} for inst_id in self.inst_ids
                    ]}))
                    async for raw in ws:
                        if raw == "pong":
                            continue
                        message: dict[str, Any] = json.loads(raw)
                        for row in message.get("data", []):
                            bids, asks = row.get("bids", []), row.get("asks", [])
                            if bids and asks:
                                await self.handler(
                                    message["arg"]["instId"],
                                    Decimal(bids[0][0]),
                                    Decimal(asks[0][0]),
                                )
            except (OSError, asyncio.CancelledError):
                if self._stop.is_set():
                    raise
                await asyncio.sleep(1)

    def stop(self) -> None:
        self._stop.set()
