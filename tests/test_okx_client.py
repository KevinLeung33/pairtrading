from decimal import Decimal

import pytest

from okx_pair_executor.models import (
    ChildOrder,
    Direction,
    OrderAction,
    OrderRequest,
    ParentOrder,
    ParentOrderRequest,
)
from okx_pair_executor.okx_client import OkxV5Client


@pytest.mark.asyncio
async def test_trade_ws_uses_inst_id_code_for_order_amend_and_cancel():
    client = OkxV5Client("key", "secret", "passphrase", demo=True)
    client._inst_id_codes["BTC-USDT-SWAP"] = 123456
    calls = []

    async def fake_trade_ws_request(op, args):
        calls.append((op, args))
        return {
            "code": "0",
            "data": [{"sCode": "0", "ordId": "order-1", "clOrdId": "client-1"}],
        }

    client._trade_ws_request = fake_trade_ws_request
    request = OrderRequest(
        inst_id="BTC-USDT-SWAP",
        side="sell",
        ord_type="post_only",
        size=Decimal("10"),
        price=Decimal("79000"),
        cl_ord_id="client-1",
        reduce_only=True,
    )

    await client._place_order_ws(request)
    await client._amend_order_ws("BTC-USDT-SWAP", "order-1", "client-1", Decimal("78999"))
    await client._cancel_order_ws("BTC-USDT-SWAP", "order-1", "client-1")

    assert [op for op, _ in calls] == ["order", "amend-order", "cancel-order"]
    assert all(args[0]["instIdCode"] == 123456 for _, args in calls)
    assert all("instId" not in args[0] for _, args in calls)
@pytest.mark.asyncio
async def test_execution_details_includes_all_repriced_maker_orders():
    client = OkxV5Client("key", "secret", "passphrase", demo=True)
    request = ParentOrderRequest(
        request_id="P-FILLS",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        action=OrderAction.OPEN,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.2"),
        child_base_qty=Decimal("0.1"),
    )
    child = ChildOrder(
        child_id="P-FILLS-C0001",
        target_base_qty=Decimal("0.2"),
        perp_target_contracts=Decimal("20"),
        contract_value=Decimal("0.01"),
        perp_order_id="maker-new",
        perp_order_ids=["maker-old", "maker-new"],
        spot_order_ids=["spot-1"],
    )
    parent = ParentOrder(request=request, children=[child])
    calls = []

    async def fake_trade_fills(inst_id, ord_id):
        calls.append((inst_id, ord_id))
        if ord_id.startswith("maker"):
            return [{"fillSz": "10", "fillPx": "79000", "fee": "-1", "feeCcy": "USDT", "side": "buy"}]
        return [{"fillSz": "0.2", "fillPx": "79010", "fee": "-0.0002", "feeCcy": "BTC", "side": "sell"}]

    client._trade_fills = fake_trade_fills
    details = await client.execution_details(parent, include_account=False)

    assert calls == [
        ("BTC-USDT-SWAP", "maker-old"),
        ("BTC-USDT-SWAP", "maker-new"),
        ("BTC-USDT", "spot-1"),
    ]
    assert details["legs"]["perp"]["filled_base_qty"] == "0.20"
    assert details["legs"]["spot"]["filled_base_qty"] == "0.2"
    assert details["legs"]["perp"]["avg_price"] == "79000"
    assert details["legs"]["spot"]["avg_price"] == "79010"
    assert details["fill_data_available"] is True

@pytest.mark.asyncio
async def test_order_payload_includes_pos_side_for_ws_and_rest():
    client = OkxV5Client("key", "secret", "passphrase", demo=True)
    client._inst_id_codes["BTC-USDT-SWAP"] = 123456
    ws_calls = []

    async def fake_trade_ws_request(op, args):
        ws_calls.append((op, args))
        return {"code": "0", "data": [{"sCode": "0", "ordId": "order-2", "clOrdId": "dual-1"}]}

    client._trade_ws_request = fake_trade_ws_request
    request = OrderRequest(
        inst_id="BTC-USDT-SWAP",
        side="sell",
        ord_type="post_only",
        size=Decimal("10"),
        price=Decimal("79000"),
        cl_ord_id="dual-1",
        pos_side="short",
    )
    await client._place_order_ws(request)
    assert ws_calls[0][1][0]["posSide"] == "short"

    rest_calls = []

    async def fake_request(method, path, payload=None):
        rest_calls.append((method, path, payload))
        return {"code": "0", "data": [{"sCode": "0", "ordId": "order-3", "clOrdId": "dual-1"}]}

    client._request = fake_request
    await client._place_order_rest(request)
    assert rest_calls[0][2]["posSide"] == "short"