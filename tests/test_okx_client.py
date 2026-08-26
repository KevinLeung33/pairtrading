from decimal import Decimal

import pytest

from okx_pair_executor.models import OrderRequest
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