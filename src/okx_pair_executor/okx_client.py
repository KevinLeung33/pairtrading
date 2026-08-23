from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
import websockets

from .exchange import FillHandler
from .models import FillEvent, InstrumentRules, OrderAck, OrderRequest


class OkxV5Client:
    """Small native OKX V5 adapter.

    It deliberately does not enable trading by itself. Construct it only after
    validating credentials and use demo=True for the first integration test.
    """

    def __init__(self, api_key: str, secret_key: str, passphrase: str, *, demo: bool = True):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.demo = demo
        self.rest_url = "https://openapi.okx.com"
        self.ws_url = "wss://wspap.okx.com:8443/ws/v5/private" if demo else "wss://ws.okx.com:8443/ws/v5/private"
        self._book: dict[str, dict[str, Decimal]] = {}
        self._known_order_ids: set[str] = set()

    def update_orderbook(self, inst_id: str, *, best_bid: Decimal, best_ask: Decimal) -> None:
        self._book[inst_id] = {"best_bid": best_bid, "best_ask": best_ask}

    async def wait_for_book(self, inst_ids: list[str], timeout: float = 10.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while any(inst_id not in self._book for inst_id in inst_ids):
            if asyncio.get_running_loop().time() >= deadline:
                missing = [inst_id for inst_id in inst_ids if inst_id not in self._book]
                raise TimeoutError(f"order book timeout: {missing}")
            await asyncio.sleep(0.05)

    async def maker_price(self, inst_id: str, side: str, offset_ticks: int = 0) -> Decimal:
        book = self._book.get(inst_id)
        if not book:
            raise RuntimeError(f"no order book for {inst_id}")
        # Offset handling is intentionally left to the caller's price policy in MVP.
        return book["best_bid" if side == "buy" else "best_ask"]

    def _sign(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        raw = timestamp + method.upper() + path + body
        digest = hmac.new(self.secret_key.encode(), raw.encode(), hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    async def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps(payload, separators=(",", ":")) if payload is not None else ""
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.demo:
            headers["x-simulated-trading"] = "1"
        async with httpx.AsyncClient(base_url=self.rest_url, timeout=5) as client:
            response = await client.request(method, path, content=body, headers=headers)
            response.raise_for_status()
            result = response.json()
        if result.get("code") != "0":
            raise RuntimeError(f"OKX API error: {result}")
        return result

    async def instrument_rules(self, inst_id: str) -> InstrumentRules:
        inst_type = "SWAP" if inst_id.endswith("-SWAP") else "SPOT"
        result = await self._request("GET", f"/api/v5/account/instruments?instType={inst_type}")
        row = next(item for item in result["data"] if item["instId"] == inst_id)
        return InstrumentRules(
            tick_size=Decimal(row["tickSz"]),
            lot_size=Decimal(row["lotSz"]),
            min_size=Decimal(row["minSz"]),
            contract_value=Decimal(row.get("ctVal") or "1"),
        )

    async def place_order(self, request: OrderRequest) -> OrderAck:
        payload: dict[str, Any] = {
            "instId": request.inst_id,
            "tdMode": "cross",
            "side": request.side,
            "ordType": request.ord_type,
            "sz": str(request.size),
            "clOrdId": request.cl_ord_id,
        }
        if request.price is not None:
            payload["px"] = str(request.price)
        if request.reduce_only:
            payload["reduceOnly"] = "true"
        if request.slippage_bps is not None:
            payload["slippagePct"] = str(request.slippage_bps / Decimal("10000"))
        result = await self._request("POST", "/api/v5/trade/order", payload)
        row = result["data"][0]
        if row.get("sCode") != "0":
            raise RuntimeError(f"OKX order rejected: {row}")
        self._known_order_ids.add(row["ordId"])
        return OrderAck(row["ordId"], row.get("clOrdId", request.cl_ord_id), "live")

    async def cancel_order(self, inst_id: str, ord_id: str, cl_ord_id: str) -> None:
        await self._request("POST", "/api/v5/trade/cancel-order", {"instId": inst_id, "ordId": ord_id, "clOrdId": cl_ord_id})

    async def get_order(self, inst_id: str, ord_id: str, cl_ord_id: str) -> FillEvent:
        result = await self._request("GET", f"/api/v5/trade/order?instId={inst_id}&ordId={ord_id}")
        return self._fill_event(result["data"][0])

    async def reconcile(self, inst_ids: list[str]) -> list[FillEvent]:
        events: list[FillEvent] = []
        seen: set[str] = set()
        for inst_id in inst_ids:
            inst_type = "SWAP" if inst_id.endswith("-SWAP") else "SPOT"
            for endpoint in ("orders-pending", "orders-history"):
                result = await self._request("GET", f"/api/v5/trade/{endpoint}?instType={inst_type}&instId={inst_id}&limit=100")
                for row in result["data"]:
                    if row["ordId"] not in seen:
                        seen.add(row["ordId"])
                        events.append(self._fill_event(row))
        return events

    async def subscribe_orders(self, handler: FillHandler) -> None:
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
            timestamp = str(int(time.time()))
            sign = self._sign(timestamp, "GET", "/users/self/verify")
            await ws.send(json.dumps({"op": "login", "args": [{
                "apiKey": self.api_key, "passphrase": self.passphrase,
                "timestamp": timestamp, "sign": sign,
            }]}))
            await ws.recv()
            await ws.send(json.dumps({"op": "subscribe", "args": [{"channel": "orders", "instType": "ANY"}]}))
            async for raw in ws:
                message = json.loads(raw)
                for row in message.get("data", []):
                    await handler(self._fill_event(row))

    @staticmethod
    def _fill_event(row: dict[str, Any]) -> FillEvent:
        return FillEvent(
            ord_id=row["ordId"], cl_ord_id=row.get("clOrdId", ""), inst_id=row["instId"],
            state=row.get("state", ""), acc_fill_sz=Decimal(row.get("accFillSz") or "0"),
            fill_px=Decimal(row.get("fillPx") or row.get("avgPx") or "0"),
            fee=Decimal(row.get("fee") or "0"), trade_id=row.get("tradeId", ""),
        )
