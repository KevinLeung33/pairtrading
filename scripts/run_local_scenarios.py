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
        self.maker_bid = Decimal("65000")
        self.maker_ask = Decimal("65000")
        self.cancel_count = 0

    async def instrument_rules(self, inst_id):
        if inst_id.endswith("SWAP"):
            return InstrumentRules(Decimal("0.1"), Decimal("1"), Decimal("1"), Decimal("0.01"))
        return InstrumentRules(Decimal("0.01"), Decimal("0.001"), Decimal("0.001"))

    async def maker_price(self, inst_id, side, offset_ticks=0):
        return self.maker_bid if side == "buy" else self.maker_ask

    async def ioc_price(self, inst_id, side, slippage_bps):
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
        self.cancel_count += 1
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


async def bbo_reprice_debounce() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("bbo-reprice", maker_reprice_interval_ms=50))
    child = parent.children[0]
    exchange.maker_bid = Decimal("64999.9")
    await executor.on_book("BTC-USDT-SWAP", Decimal("64999.9"), Decimal("65000.1"))
    await executor.on_book("BTC-USDT-SWAP", Decimal("64999.8"), Decimal("65000.2"))
    await asyncio.sleep(0.08)
    assert exchange.cancel_count == 1
    makers = [item for item in exchange.requests if item.ord_type == "post_only"]
    assert len(makers) == 2
    assert makers[-1].price == Decimal("64999.9")
    await executor.stop_repricing()
    return "BBO move triggered one debounced Maker reprice, not one reprice per tick"


async def parameter_matrix() -> str:
    cases = 0
    for direction in (Direction.SHORT_SPOT_LONG_SWAP, Direction.LONG_SPOT_SHORT_SWAP):
        for target in (Decimal("0.1"), Decimal("0.2"), Decimal("0.5")):
            for child_size in (Decimal("0.1"), Decimal("0.2")):
                if child_size > target:
                    continue
                for exposure_limit in (Decimal("0"), Decimal("0.01"), Decimal("0.05")):
                    cases += 1
                    exchange = ScenarioExchange()
                    executor = PairExecutor(exchange)
                    parent = await executor.submit(make_request(
                        f"matrix-{cases}",
                        direction=direction,
                        target_base_qty=target,
                        child_base_qty=child_size,
                        max_unhedged_base_qty=exposure_limit,
                    ))
                    for child in list(parent.children):
                        await executor.on_order_event(FillEvent(
                            child.perp_order_id,
                            child.perp_cl_ord_id,
                            "BTC-USDT-SWAP",
                            "filled",
                            child.perp_target_contracts,
                        ))
                    assert parent.state.value == "completed"
                    assert parent.exposure == Decimal("0")
    return f"{cases} combinations passed: 3 target sizes x 2 child sizes x 3 exposure limits x 2 directions"


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


async def duplicate_recovery_event() -> str:
    exchange = ScenarioExchange()
    exchange.fail_ioc = True
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("duplicate-recovery", max_hedge_retries=1))
    child = parent.children[0]
    event = FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("10"))
    await executor.on_order_event(event)
    await executor.on_order_event(event)
    assert parent.state.value == "recovery"
    assert child.state.value == "recovery"
    assert len(exchange.requests) == 2
    return "duplicate terminal recovery event was ignored"


async def out_of_order_fill() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("out-of-order"))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "partially_filled", Decimal("6")))
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "partially_filled", Decimal("4")))
    assert child.perp_filled_contracts == Decimal("6")
    assert child.spot_filled_base_qty == Decimal("0.06")
    return "out-of-order lower cumulative fill was ignored"


async def unknown_order_event() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    await executor.submit(make_request("unknown-event"))
    await executor.on_order_event(FillEvent("unknown", "unknown", "BTC-USDT-SWAP", "filled", Decimal("10")))
    assert executor.parents["unknown-event"].filled_base_qty == Decimal("0")
    return "unknown order event was ignored safely"


async def invalid_duplicate_request() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    order = make_request("duplicate-request")
    await executor.submit(order)
    try:
        await executor.submit(order)
    except ValueError as exc:
        assert "duplicate" in str(exc)
        return "duplicate request id was rejected"
    raise AssertionError("duplicate request was accepted")

async def run() -> list[ScenarioResult]:
    scenarios = [
        ("full_fill", full_fill),
        ("partial_maker", partial_maker),
        ("partial_ioc", partial_ioc),
        ("exposure_limit", exposure_limit),
        ("reprice_and_duplicate", reprice_and_duplicate),
        ("persistence", persistence),
        ("bbo_reprice_debounce", bbo_reprice_debounce),
        ("parameter_matrix", parameter_matrix),
        ("duplicate_recovery_event", duplicate_recovery_event),
        ("out_of_order_fill", out_of_order_fill),
        ("unknown_order_event", unknown_order_event),
        ("invalid_duplicate_request", invalid_duplicate_request),
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
    lines = [f"# Local Scenario Report ({stamp})", "", f"Overall: {'PASS' if payload['passed'] else 'FAIL'}", "", "| Scenario | Result | Details |", "|---|---|---|"]
    for item in results:
        result = "PASS" if item.passed else "FAIL"
        lines.append(f"| `{item.name}` | **{result}** | {item.details} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))
    print(f"\nReports: {json_path} and {md_path}")
    raise SystemExit(0 if payload["passed"] else 1)


if __name__ == "__main__":
    main()
