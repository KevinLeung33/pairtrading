from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any


class Direction(str, Enum):
    SHORT_SPOT_LONG_SWAP = "short_spot_long_swap"
    LONG_SPOT_SHORT_SWAP = "long_spot_short_swap"


class OrderAction(str, Enum):
    OPEN = "open"
    CLOSE = "close"


class ParentOrderState(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    RECOVERY = "recovery"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class ChildState(str, Enum):
    CREATED = "created"
    MAKER_WORKING = "maker_working"
    REPRICING = "repricing"
    HEDGE_PENDING = "hedge_pending"
    HEDGE_EXECUTING = "hedge_executing"
    RECOVERY = "recovery"
    COMPLETED = "completed"
    PARTIAL_COMPLETED = "partial_completed"
    FAILED = "failed"


@dataclass(frozen=True)
class InstrumentRules:
    tick_size: Decimal
    lot_size: Decimal
    min_size: Decimal
    contract_value: Decimal = Decimal("1")
    inst_id_code: int | None = None


@dataclass(frozen=True)
class ParentOrderRequest:
    request_id: str
    direction: Direction
    spot_inst_id: str
    swap_inst_id: str
    target_base_qty: Decimal
    child_base_qty: Decimal
    action: OrderAction = OrderAction.OPEN
    spot_td_mode: str = "cross"
    max_spot_slippage_bps: Decimal = Decimal("10")
    max_unhedged_base_qty: Decimal = Decimal("0.01")
    hedge_tolerance_base_qty: Decimal = Decimal("0")
    max_hedge_retries: int = 3
    max_maker_attempts: int = 50
    maker_reprice_interval_ms: int = 150
    lark_report: bool = True
    account_before: dict[str, Any] = field(default_factory=dict)


@dataclass
class FillEvent:
    ord_id: str
    cl_ord_id: str
    inst_id: str
    state: str
    acc_fill_sz: Decimal
    fill_px: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    trade_id: str = ""
    order_price: Decimal = Decimal("0")
    amend_result: str = ""


@dataclass
class ChildOrder:
    child_id: str
    target_base_qty: Decimal
    perp_target_contracts: Decimal
    contract_value: Decimal = Decimal("1")
    state: ChildState = ChildState.CREATED
    perp_order_id: str | None = None
    perp_cl_ord_id: str | None = None
    perp_filled_contracts: Decimal = Decimal("0")
    active_order_filled_contracts: Decimal = Decimal("0")
    spot_target_base_qty: Decimal = Decimal("0")
    spot_filled_base_qty: Decimal = Decimal("0")
    pending_hedge_base_qty: Decimal = Decimal("0")
    hedge_attempts: int = 0
    spot_order_ids: list[str] = field(default_factory=list)
    last_perp_fill_px: Decimal = Decimal("0")
    last_spot_fill_px: Decimal = Decimal("0")
    maker_price: Decimal = Decimal("0")
    maker_attempts: int = 0

    @property
    def perp_filled_base_qty(self) -> Decimal:
        return self.perp_filled_contracts * self.contract_value

    @property
    def unhedged_base_qty(self) -> Decimal:
        return self.perp_filled_base_qty - self.spot_filled_base_qty


@dataclass
class ParentOrder:
    request: ParentOrderRequest
    state: ParentOrderState = ParentOrderState.CREATED
    children: list[ChildOrder] = field(default_factory=list)
    error: str | None = None

    @property
    def filled_base_qty(self) -> Decimal:
        return sum((c.perp_filled_base_qty for c in self.children), Decimal("0"))

    @property
    def hedged_base_qty(self) -> Decimal:
        return sum((c.spot_filled_base_qty for c in self.children), Decimal("0"))

    @property
    def exposure(self) -> Decimal:
        return self.filled_base_qty - self.hedged_base_qty


@dataclass(frozen=True)
class OrderRequest:
    inst_id: str
    side: str
    ord_type: str
    size: Decimal
    price: Decimal | None
    cl_ord_id: str
    reduce_only: bool = False
    slippage_bps: Decimal | None = None
    td_mode: str = "cross"


@dataclass(frozen=True)
class OrderAck:
    ord_id: str
    cl_ord_id: str
    state: str


def report_payload(
    parent: ParentOrder,
    child: ChildOrder | None = None,
    execution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        "request_id": parent.request.request_id,
        "parent_state": parent.state.value,
        "direction": parent.request.direction.value,
        "action": parent.request.action.value,
        "filled_base_qty": str(parent.filled_base_qty),
        "hedged_base_qty": str(parent.hedged_base_qty),
        "exposure": str(parent.exposure),
        "parameters": {
            "target_base_qty": str(parent.request.target_base_qty),
            "child_base_qty": str(parent.request.child_base_qty),
            "spot_td_mode": parent.request.spot_td_mode,
            "max_maker_attempts": parent.request.max_maker_attempts,
            "max_unhedged_base_qty": str(parent.request.max_unhedged_base_qty),
            "maker_reprice_interval_ms": parent.request.maker_reprice_interval_ms,
        },
    }
    active_states = {
        ChildState.CREATED,
        ChildState.MAKER_WORKING,
        ChildState.REPRICING,
        ChildState.HEDGE_PENDING,
        ChildState.HEDGE_EXECUTING,
    }
    active_child = next((item for item in parent.children if item.state in active_states), None)
    data["execution_state"] = {
        "phase": active_child.state.value if active_child is not None else parent.state.value,
        "remaining_base_qty": str(max(parent.request.target_base_qty - parent.filled_base_qty, Decimal("0"))),
        "maker_order_id": active_child.perp_order_id if active_child is not None else None,
        "maker_price": str(active_child.maker_price) if active_child is not None else None,
        "maker_attempts": active_child.maker_attempts if active_child is not None else 0,
    }
    if parent.error:
        data["error"] = parent.error
    if execution:
        data["execution"] = execution
    if child is not None:
        data["child"] = {
            "child_id": child.child_id,
            "state": child.state.value,
            "target_base_qty": str(child.target_base_qty),
            "perp_target_contracts": str(child.perp_target_contracts),
            "perp_order_id": child.perp_order_id,
            "perp_filled_base_qty": str(child.perp_filled_base_qty),
            "spot_filled_base_qty": str(child.spot_filled_base_qty),
            "maker_price": str(child.maker_price),
            "exposure": str(child.unhedged_base_qty),
            "hedge_attempts": child.hedge_attempts,
        }
    return data
