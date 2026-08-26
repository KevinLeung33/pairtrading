from decimal import Decimal

import pytest

from okx_pair_executor.executor import PairExecutor
from okx_pair_executor.persistence import JsonStateStore
from okx_pair_executor.models import (
    ChildState,
    Direction,
    FillEvent,
    InstrumentRules,
    OrderAck,
    OrderRequest,
    ParentOrderRequest,
)


class FakeExchange:
    def __init__(self):
        self.orders = {}
        self.next_id = 1
        self.maker_bid = Decimal("65000")
        self.maker_ask = Decimal("65000")
        self.cancel_count = 0
        self.rules = {
            "BTC-USDT": InstrumentRules(Decimal("0.01"), Decimal("0.001"), Decimal("0.001")),
            "BTC-USDT-SWAP": InstrumentRules(Decimal("0.1"), Decimal("1"), Decimal("1"), Decimal("0.01")),
        }

    async def instrument_rules(self, inst_id):
        return self.rules[inst_id]

    async def maker_price(self, inst_id, side, offset_ticks=0):
        return self.maker_bid if side == "buy" else self.maker_ask

    async def ioc_price(self, inst_id, side, slippage_bps):
        return Decimal("65000")

    async def place_order(self, request: OrderRequest):
        ord_id = str(self.next_id)
        self.next_id += 1
        self.orders[ord_id] = FillEvent(ord_id, request.cl_ord_id, request.inst_id, "live", Decimal("0"))
        if request.ord_type == "ioc":
            # Test IOC fills whatever is available, configured by the test.
            fill = min(request.size, getattr(self, "ioc_fill", request.size))
            self.orders[ord_id] = FillEvent(ord_id, request.cl_ord_id, request.inst_id, "canceled", fill)
        return OrderAck(ord_id, request.cl_ord_id, self.orders[ord_id].state)

    async def cancel_order(self, inst_id, ord_id, cl_ord_id):
        self.cancel_count += 1
        self.orders[ord_id].state = "canceled"

    async def get_order(self, inst_id, ord_id, cl_ord_id):
        return self.orders[ord_id]

    async def subscribe_orders(self, handler):
        return None

    async def reconcile(self, inst_ids):
        return list(self.orders.values())


@pytest.mark.asyncio
async def test_partial_perp_fill_is_hedged_by_delta():
    exchange = FakeExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(ParentOrderRequest(
        request_id="P1",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
    ))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "partially_filled", Decimal("10")))
    assert child.spot_filled_base_qty == Decimal("0.1")
    assert child.unhedged_base_qty == Decimal("0")


@pytest.mark.asyncio
async def test_partial_ioc_leaves_exposure_and_retries():
    exchange = FakeExchange()
    exchange.ioc_fill = Decimal("0.05")
    executor = PairExecutor(exchange)
    parent = await executor.submit(ParentOrderRequest(
        request_id="P2",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
        max_hedge_retries=2,
        max_unhedged_base_qty=Decimal("0.2"),
    ))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("10")))
    assert child.spot_filled_base_qty == Decimal("0.1")
    assert child.unhedged_base_qty == Decimal("0")
    assert len(child.spot_order_ids) == 2


@pytest.mark.asyncio
async def test_rejects_duplicate_parent_request():
    exchange = FakeExchange()
    executor = PairExecutor(exchange)
    request = ParentOrderRequest("P3", Direction.SHORT_SPOT_LONG_SWAP, "BTC-USDT", "BTC-USDT-SWAP", Decimal("0.1"), Decimal("0.1"))
    await executor.submit(request)
    with pytest.raises(ValueError, match="duplicate"):
        await executor.submit(request)


@pytest.mark.asyncio
async def test_state_store_round_trip(tmp_path):
    exchange = FakeExchange()
    store = JsonStateStore(tmp_path / "state.json")
    executor = PairExecutor(exchange, store=store)
    request = ParentOrderRequest("P4", Direction.SHORT_SPOT_LONG_SWAP, "BTC-USDT", "BTC-USDT-SWAP", Decimal("0.1"), Decimal("0.1"))
    parent = await executor.submit(request)
    store.save({request.request_id: parent})
    restored = JsonStateStore(tmp_path / "state.json").load()
    assert restored["P4"].children[0].perp_order_id == parent.children[0].perp_order_id


@pytest.mark.asyncio
async def test_reprice_resets_active_order_fill_counter():
    exchange = FakeExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(ParentOrderRequest("P5", Direction.SHORT_SPOT_LONG_SWAP, "BTC-USDT", "BTC-USDT-SWAP", Decimal("0.1"), Decimal("0.1")))
    child = parent.children[0]
    old_order = child.perp_order_id
    await executor.on_order_event(FillEvent(old_order, child.perp_cl_ord_id, "BTC-USDT-SWAP", "partially_filled", Decimal("4")))
    await executor.reprice_child(child.child_id)
    assert child.perp_order_id != old_order
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("6")))
    assert child.perp_filled_contracts == Decimal("10")


@pytest.mark.asyncio
async def test_bbo_reprice_is_debounced():
    import asyncio
    exchange = FakeExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(ParentOrderRequest(
        request_id="P6",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
        maker_reprice_interval_ms=30,
    ))
    exchange.maker_bid = Decimal("64999.9")
    child = parent.children[0]
    await executor.on_book("BTC-USDT-SWAP", Decimal("64999.9"), Decimal("65000.1"))
    await executor.on_book("BTC-USDT-SWAP", Decimal("64999.8"), Decimal("65000.2"))
    await asyncio.sleep(0.06)
    assert exchange.cancel_count == 1
    assert child.maker_price == Decimal("64999.9")
    await executor.stop_repricing()

class AmendExchange(FakeExchange):
    def __init__(self):
        super().__init__()
        self.amend_count = 0

    async def amend_order(self, inst_id, ord_id, cl_ord_id, new_price):
        self.amend_count += 1
        self.maker_bid = new_price
        return OrderAck(ord_id, cl_ord_id, "live")


@pytest.mark.asyncio
async def test_reprice_uses_atomic_amend_when_supported():
    exchange = AmendExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(ParentOrderRequest(
        request_id="P-AMEND",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
    ))
    child = parent.children[0]
    old_order = child.perp_order_id
    exchange.maker_bid = Decimal("64999.9")
    await executor.reprice_child(child.child_id)
    assert exchange.amend_count == 1
    assert exchange.cancel_count == 0
    assert child.perp_order_id == old_order
    assert child.maker_price == Decimal("64999.9")


@pytest.mark.asyncio
async def test_failed_amend_result_restores_quote_for_retry():
    exchange = AmendExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(ParentOrderRequest(
        request_id="P-AMEND-FAIL",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
    ))
    child = parent.children[0]
    exchange.maker_bid = Decimal("64999.9")
    await executor.reprice_child(child.child_id)
    await executor.on_order_event(FillEvent(
        child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "live",
        Decimal("0"), order_price=Decimal("65000"), amend_result="51400",
    ))
    assert child.maker_price == Decimal("65000")
    assert child.state.value == "maker_working"
    exchange.maker_bid = Decimal("64999.8")
    await executor.reprice_child(child.child_id)
    assert exchange.amend_count == 2

class CaptureNotifier:
    def __init__(self):
        self.reports = []

    async def send_report(self, reason, payload):
        self.reports.append((reason, payload))


@pytest.mark.asyncio
async def test_task_notifications_hide_child_events_and_final_is_once():
    exchange = FakeExchange()
    notifier = CaptureNotifier()
    executor = PairExecutor(exchange, notifier=notifier)
    request = ParentOrderRequest(
        request_id="P-NOTIFY",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
    )

    parent = await executor.submit(request)
    assert [reason for reason, _ in notifier.reports] == ["ORDER_STARTED"]

    await executor.notify_status(request.request_id)
    assert [reason for reason, _ in notifier.reports] == ["ORDER_STARTED", "EXECUTION_STATUS"]

    child = parent.children[0]
    await executor.on_order_event(FillEvent(
        child.perp_order_id,
        child.perp_cl_ord_id,
        "BTC-USDT-SWAP",
        "filled",
        Decimal("10"),
    ))
    reasons = [reason for reason, _ in notifier.reports]
    assert "CHILD_STARTED" not in reasons
    assert "CHILD_TERMINAL" not in reasons
    assert reasons.count("PARENT_COMPLETED") == 1

    await executor.notify_terminal(request.request_id)
    assert [reason for reason, _ in notifier.reports].count("PARENT_COMPLETED") == 1


class RejectSecondMakerExchange(FakeExchange):
    def __init__(self):
        super().__init__()
        self.maker_submissions = 0

    async def place_order(self, request: OrderRequest):
        if request.ord_type == "post_only":
            self.maker_submissions += 1
            if self.maker_submissions == 2:
                raise RuntimeError("max leverage reached")
        return await super().place_order(request)


@pytest.mark.asyncio
async def test_next_maker_rejection_enters_recovery_and_is_reported():
    exchange = RejectSecondMakerExchange()
    notifier = CaptureNotifier()
    executor = PairExecutor(exchange, notifier=notifier)
    request = ParentOrderRequest(
        request_id="P-MAKER-REJECT",
        direction=Direction.LONG_SPOT_SHORT_SWAP,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.2"),
        child_base_qty=Decimal("0.1"),
    )

    parent = await executor.submit(request)
    first = parent.children[0]
    await executor.on_order_event(FillEvent(
        first.perp_order_id,
        first.perp_cl_ord_id,
        "BTC-USDT-SWAP",
        "filled",
        Decimal("10"),
    ))

    assert parent.state.value == "recovery"
    assert "max leverage reached" in (parent.error or "")
    assert [reason for reason, _ in notifier.reports].count("EXECUTION_RISK") == 1
@pytest.mark.asyncio
async def test_restored_running_parent_without_active_order_enters_recovery():
    exchange = FakeExchange()
    notifier = CaptureNotifier()
    executor = PairExecutor(exchange, notifier=notifier)
    request = ParentOrderRequest(
        request_id="P-ORPHAN",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
    )
    parent = await executor.submit(request)
    child = parent.children[0]
    executor._children_by_order.clear()
    child.perp_order_id = None
    child.state = ChildState.CREATED

    assert await executor.fail_orphaned_parent(request.request_id) is True
    assert parent.state.value == "recovery"
    assert "manual exchange reconciliation required" in (parent.error or "")
    assert [reason for reason, _ in notifier.reports].count("EXECUTION_RISK") == 1
