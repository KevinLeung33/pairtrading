from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any

import httpx
import websockets

from .exchange import FillHandler
from .models import ChildOrder, FillEvent, InstrumentRules, OrderAck, OrderRequest, ParentOrder


class OkxHttpError(RuntimeError):
    def __init__(self, status_code: int, method: str, path: str, body: str):
        super().__init__(f"OKX HTTP {status_code} {method} {path}: {body[:1000]}")
        self.status_code = status_code
        self.method = method
        self.path = path
        self.body = body


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
        self._rules: dict[str, InstrumentRules] = {}

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

    async def ioc_price(self, inst_id: str, side: str, slippage_bps: Decimal) -> Decimal:
        book = self._book.get(inst_id)
        if not book:
            raise RuntimeError(f"no order book for {inst_id}")
        if side == "buy":
            raw = book["best_ask"] * (Decimal("1") + slippage_bps / Decimal("10000"))
            rounding = ROUND_CEILING
        else:
            raw = book["best_bid"] * (Decimal("1") - slippage_bps / Decimal("10000"))
            rounding = ROUND_FLOOR
        rules = await self.instrument_rules(inst_id)
        ticks = (raw / rules.tick_size).to_integral_value(rounding=rounding)
        return ticks * rules.tick_size

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
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise OkxHttpError(
                    response.status_code,
                    method,
                    path,
                    response.text,
                ) from exc
            result = response.json()
        if result.get("code") != "0":
            raise RuntimeError(f"OKX API error: {result}")
        return result

    async def instrument_rules(self, inst_id: str) -> InstrumentRules:
        if inst_id in self._rules:
            return self._rules[inst_id]
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
        try:
            result = await self._request("POST", "/api/v5/trade/order", payload)
        except OkxHttpError as exc:
            if exc.status_code < 500:
                raise
            # A 5xx response is ambiguous: OKX may have accepted the order
            # before the gateway returned an error. Resolve by clOrdId before
            # allowing the caller to fail or retry.
            existing = await self._find_order_by_client_id(request.inst_id, request.cl_ord_id)
            if existing is not None:
                self._known_order_ids.add(existing["ordId"])
                return OrderAck(existing["ordId"], existing.get("clOrdId", request.cl_ord_id), existing.get("state", "live"))
            raise RuntimeError(
                f"OKX order submission ambiguous; no order found for clOrdId={request.cl_ord_id}; {exc}"
            ) from exc
        row = result["data"][0]
        if row.get("sCode") != "0":
            raise RuntimeError(f"OKX order rejected: {row}")
        self._known_order_ids.add(row["ordId"])
        return OrderAck(row["ordId"], row.get("clOrdId", request.cl_ord_id), "live")

    async def _find_order_by_client_id(self, inst_id: str, cl_ord_id: str) -> dict[str, Any] | None:
        try:
            result = await self._request(
                "GET",
                f"/api/v5/trade/order?instId={inst_id}&clOrdId={cl_ord_id}",
            )
        except Exception:
            return None
        rows = result.get("data") or []
        return rows[0] if rows else None

    async def cancel_order(self, inst_id: str, ord_id: str, cl_ord_id: str) -> None:
        await self._request("POST", "/api/v5/trade/cancel-order", {"instId": inst_id, "ordId": ord_id, "clOrdId": cl_ord_id})

    async def get_order(self, inst_id: str, ord_id: str, cl_ord_id: str) -> FillEvent:
        result = await self._request("GET", f"/api/v5/trade/order?instId={inst_id}&ordId={ord_id}")
        return self._fill_event(result["data"][0])

    async def account_snapshot(self, inst_ids: list[str]) -> dict[str, Any]:
        balance = await self._request("GET", "/api/v5/account/balance")
        balances: dict[str, str] = {}
        for row in (balance.get("data") or [{}])[0].get("details", []):
            ccy = row.get("ccy")
            if ccy:
                balances[ccy] = row.get("cashBal") or row.get("eq") or "0"

        positions: dict[str, str] = {}
        for inst_id in inst_ids:
            if not inst_id.endswith("-SWAP"):
                continue
            result = await self._request(
                "GET",
                f"/api/v5/account/positions?instType=SWAP&instId={inst_id}",
            )
            for row in result.get("data", []):
                pos = Decimal(row.get("pos") or "0")
                if row.get("posSide") == "short":
                    pos = -pos
                positions[inst_id] = str(pos)
        return {"balances": balances, "positions": positions}

    async def _trade_fills(self, inst_id: str, ord_id: str) -> list[dict[str, Any]]:
        if not ord_id:
            return []
        inst_type = "SWAP" if inst_id.endswith("-SWAP") else "SPOT"
        result = await self._request(
            "GET",
            f"/api/v5/trade/fills?instType={inst_type}&instId={inst_id}&ordId={ord_id}",
        )
        return result.get("data", [])

    async def execution_details(
        self,
        parent: ParentOrder,
        child: ChildOrder | None = None,
        include_account: bool = True,
    ) -> dict[str, Any]:
        fills: list[tuple[str, Decimal, dict[str, Any]]] = []
        child_by_order: dict[str, Any] = {}
        selected_children = [child] if child is not None else parent.children
        for child in selected_children:
            if child.perp_order_id:
                child_by_order[child.perp_order_id] = child
            for order_id in child.spot_order_ids:
                child_by_order[order_id] = child

        for order_id, child in child_by_order.items():
            inst_id = (
                parent.request.swap_inst_id
                if order_id == child.perp_order_id
                else parent.request.spot_inst_id
            )
            for row in await self._trade_fills(inst_id, order_id):
                fills.append((
                    "perp" if inst_id == parent.request.swap_inst_id else "spot",
                    child.contract_value if inst_id == parent.request.swap_inst_id else Decimal("1"),
                    row,
                ))

        legs: dict[str, dict[str, Any]] = {}
        for leg in ("perp", "spot"):
            rows = [(multiplier, row) for kind, multiplier, row in fills if kind == leg]
            qty = Decimal("0")
            notional = Decimal("0")
            fees: dict[str, Decimal] = {}
            for multiplier, row in rows:
                size = Decimal(row.get("fillSz") or "0") * multiplier
                price = Decimal(row.get("fillPx") or "0")
                qty += size
                notional += size * price
                fee_ccy = row.get("feeCcy") or row.get("fillFeeCcy") or "UNKNOWN"
                fee = Decimal(row.get("fee") or row.get("fillFee") or "0")
                fees[fee_ccy] = fees.get(fee_ccy, Decimal("0")) + fee
            legs[leg] = {
                "filled_base_qty": str(qty),
                "avg_price": str(notional / qty if qty else Decimal("0")),
                "fees": {ccy: str(value) for ccy, value in fees.items()},
                "fill_count": len(rows),
            }

        spot_avg = Decimal(legs["spot"]["avg_price"])
        perp_avg = Decimal(legs["perp"]["avg_price"])
        spread = (perp_avg / spot_avg - Decimal("1")) * Decimal("100") if spot_avg else Decimal("0")
        perp_buy = parent.request.direction.value == "short_spot_long_swap"
        spot_buy = parent.request.direction.value == "long_spot_short_swap"
        if parent.request.action.value == "close":
            perp_buy = not perp_buy
            spot_buy = not spot_buy
        if perp_buy:
            effective_spread = (spot_avg / perp_avg - Decimal("1")) * Decimal("100") if perp_avg else Decimal("0")
        else:
            effective_spread = (perp_avg / spot_avg - Decimal("1")) * Decimal("100") if spot_avg else Decimal("0")
        expected_balances: dict[str, Decimal] = {}
        expected_positions: dict[str, Decimal] = {}
        base_ccy, quote_ccy = parent.request.spot_inst_id.split("-", 1)
        for kind, multiplier, row in fills:
            size_raw = Decimal(row.get("fillSz") or "0")
            price = Decimal(row.get("fillPx") or "0")
            sign = Decimal("1") if row.get("side") == "buy" else Decimal("-1")
            fee_ccy = row.get("feeCcy") or row.get("fillFeeCcy")
            fee = Decimal(row.get("fee") or row.get("fillFee") or "0")
            if fee_ccy:
                expected_balances[fee_ccy] = expected_balances.get(fee_ccy, Decimal("0")) + fee
            if kind == "spot":
                size = size_raw * multiplier
                expected_balances[base_ccy] = expected_balances.get(base_ccy, Decimal("0")) + sign * size
                expected_balances[quote_ccy] = expected_balances.get(quote_ccy, Decimal("0")) - sign * size * price
            else:
                expected_positions[parent.request.swap_inst_id] = (
                    expected_positions.get(parent.request.swap_inst_id, Decimal("0")) + sign * size_raw
                )

        after = await self.account_snapshot([parent.request.spot_inst_id, parent.request.swap_inst_id]) if include_account else {}
        before = parent.request.account_before or {} if include_account else {}
        balance_before = {k: Decimal(v) for k, v in before.get("balances", {}).items()}
        balance_after = {k: Decimal(v) for k, v in after.get("balances", {}).items()}
        balance_delta_raw = {
            key: balance_after.get(key, Decimal("0")) - balance_before.get(key, Decimal("0"))
            for key in sorted(set(balance_before) | set(balance_after))
        }
        balance_delta = {
            key: str(value) for key, value in balance_delta_raw.items() if value != 0
        }
        balance_difference = {
            key: str(balance_delta_raw.get(key, Decimal("0")) - expected_balances.get(key, Decimal("0")))
            for key in sorted(set(balance_delta_raw) | set(expected_balances))
            if balance_delta_raw.get(key, Decimal("0")) != expected_balances.get(key, Decimal("0"))
        }
        position_before = {k: Decimal(v) for k, v in before.get("positions", {}).items()}
        position_after = {k: Decimal(v) for k, v in after.get("positions", {}).items()}
        position_delta_raw = {
            key: position_after.get(key, Decimal("0")) - position_before.get(key, Decimal("0"))
            for key in sorted(set(position_before) | set(position_after))
        }
        position_delta = {
            key: str(value) for key, value in position_delta_raw.items() if value != 0
        }
        position_difference = {
            key: str(position_delta_raw.get(key, Decimal("0")) - expected_positions.get(key, Decimal("0")))
            for key in sorted(set(position_delta_raw) | set(expected_positions))
            if position_delta_raw.get(key, Decimal("0")) != expected_positions.get(key, Decimal("0"))
        }
        report_available = bool(before) and bool(after)
        status = "UNAVAILABLE"
        if report_available:
            status = "MATCHED" if not balance_difference and not position_difference else "CHECK_REQUIRED"
        details = {
            "legs": legs,
            "spread_rate_pct": str(spread),
            "effective_spread_rate_pct": str(effective_spread),
            "unhedged_base_qty": str(
                child.unhedged_base_qty if child is not None else parent.exposure
            ),
        }
        if include_account:
            details["account_reconciliation"] = {
                "before": before,
                "after": after,
                "balance_delta": balance_delta,
                "expected_balance_delta": {key: str(value) for key, value in expected_balances.items() if value != 0},
                "balance_difference": balance_difference,
                "position_delta_contracts": position_delta,
                "expected_position_delta_contracts": {key: str(value) for key, value in expected_positions.items() if value != 0},
                "position_difference": position_difference,
                "status": status,
            }
        return details

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
