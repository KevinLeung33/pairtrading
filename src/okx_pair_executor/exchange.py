from __future__ import annotations

from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Protocol

from .models import FillEvent, InstrumentRules, OrderAck, OrderRequest


FillHandler = Callable[[FillEvent], Awaitable[None]]


class ExchangeAdapter(Protocol):
    async def instrument_rules(self, inst_id: str) -> InstrumentRules: ...
    async def maker_price(self, inst_id: str, side: str, offset_ticks: int = 0) -> Decimal: ...
    async def ioc_price(self, inst_id: str, side: str, slippage_bps: Decimal) -> Decimal: ...
    async def place_order(self, request: OrderRequest) -> OrderAck: ...
    async def amend_order(self, inst_id: str, ord_id: str, cl_ord_id: str, new_price: Decimal) -> OrderAck: ...
    async def cancel_order(self, inst_id: str, ord_id: str, cl_ord_id: str) -> None: ...
    async def get_order(self, inst_id: str, ord_id: str, cl_ord_id: str) -> FillEvent: ...
    async def subscribe_orders(self, handler: FillHandler) -> None: ...


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        raise ValueError("step must be positive")
    return (value // step) * step


def validate_size(value: Decimal, rules: InstrumentRules) -> None:
    if value < rules.min_size:
        raise ValueError(f"size {value} below minimum {rules.min_size}")
    if floor_to_step(value, rules.lot_size) != value:
        raise ValueError(f"size {value} does not align with lot size {rules.lot_size}")
