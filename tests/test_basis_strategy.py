import asyncio
import json
from decimal import Decimal

import pytest

from okx_pair_executor.basis_strategy import BasisArbStrategy, BasisStrategyConfig
from okx_pair_executor.models import (
    Direction,
    OrderAction,
    ParentOrder,
    ParentOrderRequest,
    ParentOrderState,
)


class FakeExecutor:
    def __init__(self):
        self.parents = {}
        self.submitted = []

    async def submit(self, request):
        self.submitted.append(request)
        parent = ParentOrder(request=request, state=ParentOrderState.RUNNING)
        self.parents[request.request_id] = parent
        return parent


@pytest.mark.asyncio
async def test_basis_entry_threshold_can_be_hot_reloaded(tmp_path):
    control_path = tmp_path / "basis-control.json"
    request = ParentOrderRequest(
        request_id="B-HOT-1",
        direction=Direction.SHORT_SPOT_LONG_SWAP,
        action=OrderAction.OPEN,
        spot_inst_id="BTC-USDT",
        swap_inst_id="BTC-USDT-SWAP",
        target_base_qty=Decimal("0.1"),
        child_base_qty=Decimal("0.1"),
    )
    executor = FakeExecutor()
    strategy = BasisArbStrategy(
        executor,
        request,
        BasisStrategyConfig(
            entry_threshold_bp=Decimal("10"),
            pause_threshold_bp=Decimal("5"),
            resume_threshold_bp=Decimal("8"),
            signal_interval_ms=0,
            control_path=str(control_path),
        ),
    )

    await strategy.on_book("BTC-USDT", Decimal("100"), Decimal("101"))
    await strategy.on_book("BTC-USDT-SWAP", Decimal("99.9"), Decimal("99.95"))
    assert executor.submitted == []

    control_path.write_text(
        json.dumps({"basis_entry_threshold_bp": "5"}),
        encoding="utf-8",
    )
    await asyncio.sleep(0.3)
    await strategy.refresh()
    assert strategy.config.entry_threshold_bp == Decimal("5")

    await strategy.on_book("BTC-USDT-SWAP", Decimal("99.9"), Decimal("99.95"))
    assert len(executor.submitted) == 1
    assert strategy.entry_basis_bp is not None