import asyncio
from decimal import Decimal

from okx_pair_executor.executor import PairExecutor
from okx_pair_executor.models import Direction, FillEvent, InstrumentRules, OrderAck, OrderRequest, ParentOrderRequest


class FakeExchange:
    def __init__(self):
        self.orders = {}
        self.counter = 0

    async def instrument_rules(self, inst_id):
        if inst_id.endswith("SWAP"):
            return InstrumentRules(Decimal("0.1"), Decimal("1"), Decimal("1"), Decimal("0.01"))
        return InstrumentRules(Decimal("0.01"), Decimal("0.001"), Decimal("0.001"))

    async def maker_price(self, inst_id, side, offset_ticks=0):
        return Decimal("65000")

    async def place_order(self, request: OrderRequest):
        self.counter += 1
        oid = str(self.counter)
        fill = request.size if request.ord_type == "ioc" else Decimal("0")
        state = "canceled" if request.ord_type == "ioc" else "live"
        self.orders[oid] = FillEvent(oid, request.cl_ord_id, request.inst_id, state, fill)
        return OrderAck(oid, request.cl_ord_id, state)

    async def cancel_order(self, inst_id, ord_id, cl_ord_id):
        return None

    async def get_order(self, inst_id, ord_id, cl_ord_id):
        return self.orders[ord_id]

    async def subscribe_orders(self, handler):
        return None

    async def reconcile(self, inst_ids):
        return list(self.orders.values())


async def main():
    exchange = FakeExchange()
    executor = PairExecutor(exchange)
    parent = await executor.submit(ParentOrderRequest(
        request_id="SMOKE-1",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
    ))
    child = parent.children[0]
    await executor.on_order_event(FillEvent(child.perp_order_id, child.perp_cl_ord_id, "BTC-USDT-SWAP", "filled", Decimal("10")))
    assert parent.state.value == "completed"
    assert parent.exposure == Decimal("0")
    print("smoke ok")


if __name__ == "__main__":
    asyncio.run(main())
