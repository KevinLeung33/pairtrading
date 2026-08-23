from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from okx_pair_executor.executor import PairExecutor
from okx_pair_executor.models import Direction, FillEvent, InstrumentRules, OrderAck, OrderRequest, ParentOrderRequest
from okx_pair_executor.persistence import JsonStateStore


@dataclass
class ScenarioResult:
    name: str
    passed: bool
    details: str


class ScenarioExchange:
    def __init__(self):
        self.orders: dict[str, FillEvent] = {}
        self.requests: list[OrderRequest] = []
        self.counter = 0
        self.ioc_fill_ratio = Decimal("1")
        self.fail_ioc = False

    async def instrument_rules(self, inst_id):
        if inst_id.endswith("SWAP"):
            return InstrumentRules(Decimal("0.1"), Decimal("1"), Decimal("1"), Decimal("0.01"))
        return InstrumentRules(Decimal("0.01"), Decimal("0.001"), Decimal("0.001"))

    async def maker_price(self, inst_id, side, offset_ticks=0):
        return Decimal("65000")

    async def place_order(self, request: OrderRequest):
        self.counter += 1
        order_id = str(self.counter)
        self.requests.append(request)
        state = "live"
        fill = Decimal("0")
        if request.ord_type == "ioc":
            if self.fail_ioc:
                raise RuntimeError("simulated IOC failure")
            fill = request.size * self.ioc_fill_ratio
            state = "filled" if fill == request.size else "canceled"
        self.orders[order_id] = FillEvent(order_id, request.cl_ord_id, request.inst_id, state, fill)
        return OrderAck(order_id, request.cl_ord_id, state)

    async def cancel_order(self, inst_id, ord_id, cl_ord_id):
        if ord_id in self.orders:
            self.orders[ord_id].state = "canceled"

    async def get_order(self, inst_id, ord_id, cl_ord_id):
        return self.orders[ord_id]

    async def subscribe_orders(self, handler):
        return None

    async def reconcile(self, inst_ids):
        return list(self.orders.values())


def make_request(name: str, **kwargs) -> ParentOrderRequest:
    values = {
        "request_id": name,
        "direction": Direction.SHORT_SPOT_LONG_SWAP,
        "spot_inst_id": "BTC-USDT",
        "swap_inst_id": "BTC-USDT-SWAP",
        "target_base_qty": Decimal("0.1"),
        "child_base_qty": Decimal("0.1"),
        "max_unhedged_base_qty": Decimal("0.01"),
        "max_hedge_retries": 3,
    }
    values.update(kwargs)
    return ParentOrderRequest(**values)


async def full_fill() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("full"))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("10")))
    assert parent.state.value == "completed"
    assert parent.exposure == Decimal("0")
    assert sum(item.ord_type == "ioc" for item in exchange.requests) == 1
    return "full Maker fill, one IOC hedge, zero exposure"


async def partial_maker() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("partial-maker"))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "partially_filled", Decimal("4")))
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("10")))
    assert child.perp_filled_base_qty == Decimal("0.1")
    assert child.spot_filled_base_qty == Decimal("0.1")
    assert parent.exposure == Decimal("0")
    assert sum(item.ord_type == "ioc" for item in exchange.requests) == 2
    return "4 contracts and then 6 contracts hedged independently"


async def partial_ioc() -> str:
    exchange = ScenarioExchange()
    exchange.ioc_fill_ratio = Decimal("0.5")
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "partial-ioc",
        max_unhedged_base_qty=Decimal("0.2"),
        hedge_tolerance_base_qty=Decimal("0.0001"),
        max_hedge_retries=10,
    ))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("10")))
    assert parent.exposure.copy_abs() <= Decimal("0.0001")
    assert sum(item.ord_type == "ioc" for item in exchange.requests) > 2
    return "partial IOC retried until residual was within tolerance"


async def exposure_limit() -> str:
    exchange = ScenarioExchange()
    exchange.fail_ioc = True
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("exposure-limit", max_hedge_retries=1))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("10")))
    assert parent.state.value == "recovery"
    assert child.state.value == "recovery"
    return "failed hedge stopped normal execution and entered recovery"


async def reprice_and_duplicate() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("reprice"))
    child = parent.children[0]
    old_order = child.perp_order_id
    event = FillEvent(old_order, child.perp_cl_ord_id, "BTC-USDT-SWAP", "partially_filled", Decimal("4"))
    await executor.on_order_event(event)
    await executor.on_order_event(event)
    assert child.perp_filled_contracts == Decimal("4")
    await executor.reprice_child(child.child_id)
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("6")))
    assert child.perp_filled_contracts == Decimal("10")
    assert child.spot_filled_base_qty == Decimal("0.1")
    return "duplicate fill ignored and reprice preserved total fill"


async def persistence() -> str:
    path = Path("runtime/reports/local-state-test.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    exchange = ScenarioExchange()
    store = JsonStateStore(path)
    executor = PairExecutor(exchange, store=store)
    parent = await executor.submit(make_request("persistence"))
    restored = JsonStateStore(path).load()
    assert restored["persistence"].children[0].perp_order_id == parent.children[0].perp_order_id
    return "parent and child mappings round-tripped through JSON"


async def run() -> list[ScenarioResult]:
    scenarios = [
        ("full_fill", full_fill),
        ("partial_maker", partial_maker),
        ("partial_ioc", partial_ioc),
        ("exposure_limit", exposure_limit),
        ("reprice_and_duplicate", reprice_and_duplicate),
        ("persistence", persistence),
    ]
    results = []
    for name, function in scenarios:
        try:
            results.append(ScenarioResult(name, True, await function()))
        except Exception as exc:
            results.append(ScenarioResult(name, False, f"{type(exc).__name__}: {exc}"))
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="runtime/reports")
    args = parser.parse_args()
    results = asyncio.run(run())
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    payload = {
        "generated_at": stamp,
        "mode": "local_fake_exchange_no_network",
        "passed": all(item.passed for item in results),
        "results": [item.__dict__ for item in results],
    }
    json_path = output_dir / f"local-scenarios-{stamp}.json"
    md_path = output_dir / f"local-scenarios-{stamp}.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = [f"# Local Scenario Report ({stamp})", "", f"Overall: {'PASS' if payload['passed'] else 'FAIL'}", ""]
    for item in results:
        lines.append(f"- {'PASS' if item.passed else 'FAIL'} `{item.name}` — {item.details}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))
    print(f"\nReports: {json_path} and {md_path}")
    raise SystemExit(0 if payload["passed"] else 1)


if __name__ == "__main__":
    main()
