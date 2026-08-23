from decimal import Decimal

import pytest

from okx_pair_executor.executor import PairExecutor
from okx_pair_executor.persistence import JsonStateStore
from okx_pair_executor.models import (
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
        self.rules = {
            "BTC-USDT": InstrumentRules(Decimal("0.01"), Decimal("0.001"), Decimal("0.001")),
            "BTC-USDT-SWAP": InstrumentRules(Decimal("0.1"), Decimal("1"), Decimal("1"), Decimal("0.01")),
        }

    async def instrument_rules(self, inst_id):
        return self.rules[inst_id]

    async def maker_price(self, inst_id, side, offset_ticks=0):
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
    assert child.spot_filled_base_qty == Decimal("10")
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
