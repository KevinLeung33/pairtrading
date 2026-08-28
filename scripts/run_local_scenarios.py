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
from okx_pair_executor.basis_strategy import BasisArbStrategy, BasisStrategyConfig
from okx_pair_executor.models import Direction, FillEvent, InstrumentRules, OrderAck, OrderAction, OrderRequest, ParentOrderRequest
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
        self.cancel_race_fill = False
        self.get_order_count = 0
        self.reconcile_count = 0

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
        if self.cancel_race_fill:
            self.orders[ord_id].state = "filled"
            self.orders[ord_id].acc_fill_sz = Decimal("10")
            raise RuntimeError("simulated cancel raced with fill")
        if ord_id in self.orders:
            self.orders[ord_id].state = "canceled"

    async def get_order(self, inst_id, ord_id, cl_ord_id):
        self.get_order_count += 1
        return self.orders[ord_id]

    async def subscribe_orders(self, handler):
        return None

    async def reconcile(self, inst_ids):
        self.reconcile_count += 1
        return list(self.orders.values())


class AmendScenarioExchange(ScenarioExchange):
    def __init__(self):
        super().__init__()
        self.amend_count = 0

    async def amend_order(self, inst_id, ord_id, cl_ord_id, new_price):
        self.amend_count += 1
        self.maker_bid = new_price
        return OrderAck(ord_id, cl_ord_id, "live")

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
        hedge_tolerance_base_qty=Decimal("0.001"),
        max_hedge_retries=10,
    ))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("10")))
    assert parent.exposure.copy_abs() <= Decimal("0.001")
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


async def basis_pause_resume() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    request = make_request(
        "basis-pause-resume",
        direction=Direction.LONG_SPOT_SHORT_SWAP,
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
        lark_report=False,
    )
    strategy = BasisArbStrategy(
        executor,
        request,
        BasisStrategyConfig(
            entry_threshold_bp=Decimal("10"),
            pause_threshold_bp=Decimal("5"),
            resume_threshold_bp=Decimal("8"),
            signal_interval_ms=0,
        ),
    )

    async def feed(inst_id, bid, ask):
        await strategy.on_book(inst_id, bid, ask)
        await executor.on_book(inst_id, bid, ask)

    await feed("BTC-USDT", Decimal("64990"), Decimal("65000"))
    await feed("BTC-USDT-SWAP", Decimal("65100"), Decimal("65110"))
    assert strategy.parent is not None
    assert strategy.state.value == "running"
    first_order = strategy.parent.children[0].perp_order_id
    await feed("BTC-USDT-SWAP", Decimal("65000"), Decimal("65010"))
    assert strategy.parent.state.value == "paused"
    assert exchange.cancel_count == 1
    await feed("BTC-USDT-SWAP", Decimal("65100"), Decimal("65110"))
    assert strategy.parent.state.value == "running"
    assert strategy.parent.children[0].perp_order_id != first_order
    child = strategy.parent.children[0]
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("10"),
    ))
    await strategy.refresh()
    assert strategy.state.value == "completed"
    assert strategy.parent.exposure == Decimal("0")
    return "basis entry paused on signal loss, resumed with a new Maker attempt, and completed"


async def basis_direction_matrix() -> str:
    values = []
    for direction, spot, swap in [
        (
            Direction.LONG_SPOT_SHORT_SWAP,
            (Decimal("64990"), Decimal("65000")),
            (Decimal("65100"), Decimal("65110")),
        ),
        (
            Direction.SHORT_SPOT_LONG_SWAP,
            (Decimal("65100"), Decimal("65110")),
            (Decimal("65000"), Decimal("65010")),
        ),
    ]:
        exchange = ScenarioExchange()
        executor = PairExecutor(exchange)
        strategy = BasisArbStrategy(
            executor,
            make_request(f"basis-{direction.value}", direction=direction, lark_report=False),
            BasisStrategyConfig(signal_interval_ms=0),
        )
        strategy.update_book("BTC-USDT", *spot)
        strategy.update_book("BTC-USDT-SWAP", *swap)
        value = strategy.current_basis_bp()
        assert value is not None and value > Decimal("10")
        values.append(value)
    return "both opening directions produced positive executable basis signals: " + ", ".join(str(value) for value in values)


async def atomic_amend() -> str:
    exchange = AmendScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("atomic-amend"))
    child = parent.children[0]
    old_order = child.perp_order_id
    exchange.maker_bid = Decimal("64999.9")
    await executor.reprice_child(child.child_id)
    assert exchange.amend_count == 1
    assert exchange.cancel_count == 0
    assert child.perp_order_id == old_order
    assert child.maker_price == Decimal("64999.9")
    return "Maker repriced through atomic amend without cancel-and-replace"

async def amend_failure_recovery() -> str:
    exchange = AmendScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("amend-failure"))
    child = parent.children[0]
    exchange.maker_bid = Decimal("64999.9")
    await executor.reprice_child(child.child_id)
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "live",
        Decimal("0"), order_price=Decimal("65000"), amend_result="51400",
    ))
    assert child.state.value == "maker_working"
    assert child.maker_price == Decimal("65000")
    exchange.maker_bid = Decimal("64999.8")
    await executor.reprice_child(child.child_id)
    assert exchange.amend_count == 2
    return "failed amend restored the actual quote and allowed a retry"


async def reconcile_active_order() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    await executor.submit(make_request("reconcile-active"))
    events = await executor.reconcile()
    assert len(events) == 1
    assert exchange.get_order_count == 1
    assert exchange.reconcile_count == 0
    return "REST safety check queried the known active order, not orders-history"


async def reprice_cancel_race_fill() -> str:
    exchange = ScenarioExchange()
    exchange.cancel_race_fill = True
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("reprice-race"))
    child = parent.children[0]
    await executor.reprice_child(child.child_id)
    assert parent.state.value == "completed"
    assert child.perp_filled_base_qty == Decimal("0.1")
    assert child.spot_filled_base_qty == Decimal("0.1")
    return "Maker fill racing with cancel was recovered from the order report"


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

async def close_short_spot_long_swap() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "close-short",
        action=OrderAction.CLOSE,
        direction=Direction.SHORT_SPOT_LONG_SWAP,
    ))
    child = parent.children[0]
    maker = exchange.requests[0]
    assert maker.side == "sell"
    assert maker.reduce_only is True
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
        "filled", Decimal("10"),
    ))
    hedge = [item for item in exchange.requests if item.ord_type == "ioc"][0]
    assert hedge.side == "buy"
    assert parent.state.value == "completed"
    return "close short-spot/long-swap used sell reduce-only Maker and buy spot IOC"


async def close_long_spot_short_swap() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "close-long",
        action=OrderAction.CLOSE,
        direction=Direction.LONG_SPOT_SHORT_SWAP,
    ))
    child = parent.children[0]
    maker = exchange.requests[0]
    assert maker.side == "buy"
    assert maker.reduce_only is True
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
        "filled", Decimal("10"),
    ))
    hedge = [item for item in exchange.requests if item.ord_type == "ioc"][0]
    assert hedge.side == "sell"
    assert parent.state.value == "completed"
    return "close long-spot/short-swap used buy reduce-only Maker and sell spot IOC"


async def close_split_position() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "close-split",
        action=OrderAction.CLOSE,
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        target_base_qty=Decimal("0.2"),
        child_base_qty=Decimal("0.1"),
    ))
    for child in list(parent.children):
        await executor.on_order_event(FillEvent(
            child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
            "filled", child.perp_target_contracts,
        ))
    makers = [item for item in exchange.requests if item.ord_type == "post_only"]
    assert len(makers) == 2
    assert all(item.reduce_only for item in makers)
    assert parent.state.value == "completed"
    return "close position split into two reduce-only children with independent spot hedges"


async def close_partial_ioc() -> str:
    exchange = ScenarioExchange()
    exchange.ioc_fill_ratio = Decimal("0.5")
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "close-partial-ioc",
        action=OrderAction.CLOSE,
        direction=Direction.LONG_SPOT_SHORT_SWAP,
        max_unhedged_base_qty=Decimal("0.2"),
        hedge_tolerance_base_qty=Decimal("0.001"),
        max_hedge_retries=10,
    ))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
        "filled", Decimal("10"),
    ))
    assert parent.exposure.copy_abs() <= Decimal("0.001")
    assert parent.state.value == "completed"
    return "close partial IOC retried and finished within tolerance"


async def one_btc_split_short() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "one-btc-short",
        target_base_qty=Decimal("1"),
        child_base_qty=Decimal("0.25"),
    ))
    for child in list(parent.children):
        await executor.on_order_event(FillEvent(
            child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
            "filled", child.perp_target_contracts,
        ))
    assert len(parent.children) == 4
    assert parent.state.value == "completed"
    assert parent.exposure == Decimal("0")
    return "1 BTC opened as 4 x 0.25 BTC short-spot/long-swap children"


async def one_btc_split_long() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "one-btc-long",
        direction=Direction.LONG_SPOT_SHORT_SWAP,
        target_base_qty=Decimal("1"),
        child_base_qty=Decimal("0.1"),
    ))
    for child in list(parent.children):
        await executor.on_order_event(FillEvent(
            child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
            "filled", child.perp_target_contracts,
        ))
    assert len(parent.children) == 10
    assert parent.state.value == "completed"
    assert parent.exposure == Decimal("0")
    return "1 BTC opened as 10 x 0.1 BTC long-spot/short-swap children"


async def maker_three_incremental_fills() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("maker-three-increments"))
    child = parent.children[0]
    for state, contracts in (("partially_filled", "2"), ("partially_filled", "5"), ("filled", "10")):
        await executor.on_order_event(FillEvent(
            child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
            state, Decimal(contracts),
        ))
    assert child.perp_filled_base_qty == Decimal("0.1")
    assert child.spot_filled_base_qty == Decimal("0.1")
    assert parent.state.value == "completed"
    return "Maker cumulative fills 2 -> 5 -> 10 contracts were hedged by deltas"


async def maker_cancel_after_partial_fill() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("maker-cancel-partial"))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
        "partially_filled", Decimal("4"),
    ))
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
        "canceled", Decimal("4"),
    ))
    assert child.state.value == "maker_working"
    assert parent.state.value == "running"
    await executor.on_order_event(FillEvent(
        child.perp_order_id,
        child.perp_cl_ord_id,
        "BTC-USDT-SWAP",
        "filled",
        child.perp_target_contracts,
    ))
    assert parent.state.value == "completed"
    assert parent.exposure == Decimal("0")
    assert parent.filled_base_qty == Decimal("0.1")
    return "partially filled Maker cancellation requeued the residual and completed the child"


async def zero_fill_cancel() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("zero-fill-cancel"))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
        "canceled", Decimal("0"),
    ))
    assert child.state.value == "maker_working"
    assert parent.state.value == "running"
    assert not [item for item in exchange.requests if item.ord_type == "ioc"]
    await executor.on_order_event(FillEvent(
        child.perp_order_id,
        child.perp_cl_ord_id,
        "BTC-USDT-SWAP",
        "filled",
        child.perp_target_contracts,
    ))
    assert parent.state.value == "completed"
    return "zero-fill Maker cancellation requeued the child instead of falsely completing the parent"


async def sell_side_bbo_reprice() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "sell-bbo",
        direction=Direction.LONG_SPOT_SHORT_SWAP,
        maker_reprice_interval_ms=20,
    ))
    child = parent.children[0]
    exchange.maker_ask = Decimal("65010")
    await executor.on_book("BTC-USDT-SWAP", Decimal("64990"), Decimal("65010"))
    await asyncio.sleep(0.05)
    makers = [item for item in exchange.requests if item.ord_type == "post_only"]
    assert len(makers) == 2
    assert makers[-1].side == "sell"
    assert makers[-1].price == Decimal("65010")
    await executor.stop_repricing()
    return "sell Maker followed best ask with debounce"


async def multiple_bbo_moves() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("multiple-bbo", maker_reprice_interval_ms=15))
    child = parent.children[0]
    for bid in ("64999.9", "64999.8", "64999.7"):
        exchange.maker_bid = Decimal(bid)
        await executor.on_book("BTC-USDT-SWAP", Decimal(bid), Decimal("65000.1"))
        await asyncio.sleep(0.03)
    makers = [item for item in exchange.requests if item.ord_type == "post_only"]
    assert len(makers) == 4
    assert child.state.value == "maker_working"
    await executor.stop_repricing()
    return "three separated BBO moves produced three controlled reprices"


async def sub_minimum_residual_enters_recovery() -> str:
    class DustSpotExchange(ScenarioExchange):
        async def instrument_rules(self, inst_id):
            if inst_id.endswith("SWAP"):
                return InstrumentRules(Decimal("0.1"), Decimal("1"), Decimal("1"), Decimal("0.01"))
            return InstrumentRules(Decimal("0.01"), Decimal("0.001"), Decimal("0.01"))

    exchange = DustSpotExchange()
    exchange.ioc_fill_ratio = Decimal("0.75")
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "sub-minimum-residual",
        target_base_qty=Decimal("0.02"),
        child_base_qty=Decimal("0.02"),
        max_hedge_retries=5,
    ))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
        "filled", Decimal("2"),
    ))
    assert parent.state.value == "recovery"
    assert child.state.value == "recovery"
    assert child.unhedged_base_qty == Decimal("0.005")
    return "IOC residual below spot minimum entered recovery instead of being silently deferred"


async def partial_hedge_retry_exhaustion() -> str:
    exchange = ScenarioExchange()
    exchange.ioc_fill_ratio = Decimal("0.75")
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request("retry-exhaustion", max_hedge_retries=2))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
        "filled", Decimal("10"),
    ))
    assert parent.state.value == "recovery"
    assert child.hedge_attempts == 2
    assert child.unhedged_base_qty > 0
    return "partial IOC stopped after retry limit and preserved recovery exposure"


async def merge_small_tail_child() -> str:
    class TailMinExchange(ScenarioExchange):
        async def instrument_rules(self, inst_id):
            if inst_id.endswith("SWAP"):
                return InstrumentRules(Decimal("0.1"), Decimal("1"), Decimal("10"), Decimal("0.01"))
            return InstrumentRules(Decimal("0.01"), Decimal("0.001"), Decimal("0.1"))

    exchange = TailMinExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "merge-tail",
        target_base_qty=Decimal("0.15"),
        child_base_qty=Decimal("0.1"),
    ))
    assert len(parent.children) == 1
    assert parent.children[0].target_base_qty == Decimal("0.15")
    await executor.on_order_event(FillEvent(
        parent.children[0].perp_order_id,
        parent.children[0].perp_cl_ord_id,
        "BTC-USDT-SWAP",
        "filled",
        Decimal("15"),
    ))
    assert parent.state.value == "completed"
    return "tail below effective minimum was merged into the previous batch"


async def invalid_contract_quantity() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    try:
        await executor.submit(make_request(
            "invalid-contract-qty",
            target_base_qty=Decimal("0.105"),
            child_base_qty=Decimal("0.105"),
        ))
    except ValueError as exc:
        assert "aligned" in str(exc) or "representable" in str(exc)
        return "0.105 BTC was rejected because swap contract size cannot represent it exactly"
    raise AssertionError("unrepresentable contract quantity was accepted")


async def one_btc_many_children() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "one-btc-many",
        target_base_qty=Decimal("1"),
        child_base_qty=Decimal("0.05"),
        max_unhedged_base_qty=Decimal("0.01"),
    ))
    assert len(parent.children) == 20
    for child in list(parent.children):
        await executor.on_order_event(FillEvent(
            child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP",
            "filled", child.perp_target_contracts,
        ))
    assert parent.state.value == "completed"
    assert parent.filled_base_qty == Decimal("1")
    assert parent.hedged_base_qty == Decimal("1")
    return "1 BTC opened through 20 small children with zero final exposure"


async def spot_margin_mode() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "spot-margin-mode",
        spot_td_mode="cross",
    ))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(
        child.perp_order_id,
        child.perp_cl_ord_id,
        "BTC-USDT-SWAP",
        "filled",
        child.perp_target_contracts,
    ))
    spot_requests = [request for request in exchange.requests if request.inst_id == "BTC-USDT"]
    assert spot_requests and all(request.td_mode == "cross" for request in spot_requests)
    return "Spot IOC hedge carried tdMode=cross for Cross Margin Buy"


async def incomplete_final_child() -> str:
    exchange = ScenarioExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(make_request(
        "incomplete-final",
        target_base_qty=Decimal("1"),
        child_base_qty=Decimal("0.1"),
        max_maker_attempts=1,
    ))
    for child in parent.children[:-1]:
        await executor.on_order_event(FillEvent(
            child.perp_order_id,
            child.perp_cl_ord_id,
            "BTC-USDT-SWAP",
            "filled",
            child.perp_target_contracts,
        ))
    final_child = parent.children[-1]
    await executor.on_order_event(FillEvent(
        final_child.perp_order_id,
        final_child.perp_cl_ord_id,
        "BTC-USDT-SWAP",
        "canceled",
        Decimal("0"),
    ))
    assert parent.filled_base_qty == Decimal("0.9")
    assert parent.state.value == "recovery"
    assert parent.error and "Maker retry limit reached" in parent.error
    return "0.9/1 BTC entered recovery after the configured Maker retry limit instead of false completion"


async def run() -> list[ScenarioResult]:
    scenarios = [
        ("full_fill", full_fill),
        ("partial_maker", partial_maker),
        ("partial_ioc", partial_ioc),
        ("exposure_limit", exposure_limit),
        ("reprice_and_duplicate", reprice_and_duplicate),
        ("atomic_amend", atomic_amend),
        ("basis_pause_resume", basis_pause_resume),
        ("basis_direction_matrix", basis_direction_matrix),
        ("persistence", persistence),
        ("bbo_reprice_debounce", bbo_reprice_debounce),
        ("reprice_cancel_race_fill", reprice_cancel_race_fill),
        ("reconcile_active_order", reconcile_active_order),
        ("amend_failure_recovery", amend_failure_recovery),
        ("parameter_matrix", parameter_matrix),
        ("duplicate_recovery_event", duplicate_recovery_event),
        ("out_of_order_fill", out_of_order_fill),
        ("unknown_order_event", unknown_order_event),
        ("invalid_duplicate_request", invalid_duplicate_request),
        ("close_short_spot_long_swap", close_short_spot_long_swap),
        ("close_long_spot_short_swap", close_long_spot_short_swap),
        ("close_split_position", close_split_position),
        ("close_partial_ioc", close_partial_ioc),
        ("one_btc_split_short", one_btc_split_short),
        ("one_btc_split_long", one_btc_split_long),
        ("maker_three_incremental_fills", maker_three_incremental_fills),
        ("maker_cancel_after_partial_fill", maker_cancel_after_partial_fill),
        ("zero_fill_cancel", zero_fill_cancel),
        ("sell_side_bbo_reprice", sell_side_bbo_reprice),
        ("multiple_bbo_moves", multiple_bbo_moves),
        ("partial_hedge_retry_exhaustion", partial_hedge_retry_exhaustion),
        ("sub_minimum_residual_enters_recovery", sub_minimum_residual_enters_recovery),
        ("merge_small_tail_child", merge_small_tail_child),
        ("invalid_contract_quantity", invalid_contract_quantity),
        ("one_btc_many_children", one_btc_many_children),
        ("spot_margin_mode", spot_margin_mode),
        ("incomplete_final_child", incomplete_final_child),
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
