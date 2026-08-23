from __future__ import annotations

import asyncio
import re
from decimal import Decimal
from typing import Any

from .exchange import ExchangeAdapter, floor_to_step, validate_size
from .models import (
    ChildOrder,
    ChildState,
    Direction,
    FillEvent,
    InstrumentRules,
    OrderRequest,
    ParentOrder,
    ParentOrderRequest,
    ParentOrderState,
    report_payload,
)
from .persistence import JsonStateStore


def compact_client_id(value: str) -> str:
    result = re.sub(r'[^A-Za-z0-9]', '', value)
    if not result:
        raise ValueError('client order id must contain an alphanumeric character')
    return result[:32]


class PairExecutor:
    """Fill-driven executor. The adapter is intentionally injected for safe testing."""

    def __init__(self, exchange: ExchangeAdapter, notifier: Any | None = None, store: JsonStateStore | None = None):
        self.exchange = exchange
        self.notifier = notifier
        self.store = store
        self.parents: dict[str, ParentOrder] = {}
        self._children_by_order: dict[str, ChildOrder] = {}
        self._hedge_locks: dict[str, asyncio.Lock] = {}
        self._event_lock = asyncio.Lock()

    def restore(self) -> dict[str, ParentOrder]:
        if not self.store:
            return self.parents
        self.parents = self.store.load()
        self._children_by_order.clear()
        self._hedge_locks.clear()
        for parent in self.parents.values():
            for child in parent.children:
                self._hedge_locks[child.child_id] = asyncio.Lock()
                if child.perp_order_id:
                    self._children_by_order[child.perp_order_id] = child
        return self.parents

    def _persist(self) -> None:
        if self.store:
            self.store.save(self.parents)

    async def submit(self, request: ParentOrderRequest) -> ParentOrder:
        if request.request_id in self.parents:
            raise ValueError(f"duplicate request_id: {request.request_id}")
        if request.target_base_qty <= 0 or request.child_base_qty <= 0:
            raise ValueError("target and child quantities must be positive")

        spot_rules = await self.exchange.instrument_rules(request.spot_inst_id)
        swap_rules = await self.exchange.instrument_rules(request.swap_inst_id)
        if request.child_base_qty < spot_rules.min_size:
            raise ValueError("child quantity below spot minimum")

        parent = ParentOrder(request=request, state=ParentOrderState.RUNNING)
        remaining = request.target_base_qty
        index = 1
        while remaining > 0:
            child_base = min(remaining, request.child_base_qty)
            child_base = floor_to_step(child_base, spot_rules.lot_size)
            if child_base <= 0:
                break
            perp_contracts = floor_to_step(child_base / swap_rules.contract_value, swap_rules.lot_size)
            validate_size(perp_contracts, swap_rules)
            child = ChildOrder(
                child_id=f"{request.request_id}-C{index:04d}",
                target_base_qty=child_base,
                perp_target_contracts=perp_contracts,
                contract_value=swap_rules.contract_value,
            )
            parent.children.append(child)
            self._hedge_locks[child.child_id] = asyncio.Lock()
            remaining -= child_base
            index += 1

        if remaining > 0:
            raise ValueError(f"target quantity leaves unrepresentable remainder: {remaining}")
        self.parents[request.request_id] = parent
        await self._place_maker(parent, parent.children[0], swap_rules)
        self._persist()
        return parent

    async def _place_maker(self, parent: ParentOrder, child: ChildOrder, rules: InstrumentRules) -> None:
        buy = parent.request.direction is Direction.SHORT_SPOT_LONG_SWAP
        cl_ord_id = compact_client_id(f"{child.child_id}M")
        request = OrderRequest(
            inst_id=parent.request.swap_inst_id,
            side="buy" if buy else "sell",
            ord_type="post_only",
            size=child.perp_target_contracts,
            price=await self.exchange.maker_price(parent.request.swap_inst_id, "buy" if buy else "sell"),
            cl_ord_id=cl_ord_id,
        )
        ack = await self.exchange.place_order(request)
        child.perp_order_id = ack.ord_id
        child.perp_cl_ord_id = ack.cl_ord_id
        child.state = ChildState.MAKER_WORKING
        self._children_by_order[ack.ord_id] = child
        self._persist()

    async def reprice_child(self, child_id: str) -> None:
        child = next(child for parent in self.parents.values() for child in parent.children if child.child_id == child_id)
        parent = self.parents_for_child(child)
        if child.state in {ChildState.RECOVERY, ChildState.COMPLETED, ChildState.PARTIAL_COMPLETED, ChildState.FAILED}:
            return
        if not child.perp_order_id or child.state not in {ChildState.MAKER_WORKING, ChildState.REPRICING}:
            return
        child.state = ChildState.REPRICING
        old_order_id = child.perp_order_id
        self._children_by_order.pop(old_order_id, None)
        await self.exchange.cancel_order(parent.request.swap_inst_id, child.perp_order_id, child.perp_cl_ord_id or "")
        if child.perp_filled_contracts < child.perp_target_contracts:
            rules = await self.exchange.instrument_rules(parent.request.swap_inst_id)
            remaining = child.perp_target_contracts - child.perp_filled_contracts
            child.perp_target_contracts = remaining
            child.active_order_filled_contracts = Decimal("0")
            await self._place_maker(parent, child, rules)
        self._persist()

    async def on_order_event(self, event: FillEvent) -> None:
        async with self._event_lock:
            child = self._children_by_order.get(event.ord_id)
            if child is None:
                return
            if event.inst_id == self.parents_for_child(child).request.swap_inst_id:
                await self._on_perp_event(child, event)

    def parents_for_child(self, child: ChildOrder) -> ParentOrder:
        for parent in self.parents.values():
            if child in parent.children:
                return parent
        raise KeyError(child.child_id)

    async def _on_perp_event(self, child: ChildOrder, event: FillEvent) -> None:
        parent = self.parents_for_child(child)
        if child.state in {ChildState.RECOVERY, ChildState.COMPLETED, ChildState.PARTIAL_COMPLETED, ChildState.FAILED}:
            return
        previous = child.active_order_filled_contracts
        if event.acc_fill_sz < previous:
            return
        child.active_order_filled_contracts = event.acc_fill_sz
        child.last_perp_fill_px = event.fill_px
        delta = event.acc_fill_sz - previous
        if delta > 0:
            child.perp_filled_contracts += delta
            delta_base = delta * child.contract_value
            child.pending_hedge_base_qty += delta_base
            child.spot_target_base_qty += delta_base
            child.state = ChildState.HEDGE_PENDING
            await self._hedge_pending(parent, child)
            if child.state is ChildState.RECOVERY:
                self._persist()
                return

        if event.state in {"filled", "canceled"} and child.state is not ChildState.REPRICING:
            if child.unhedged_base_qty.copy_abs() > parent.request.max_unhedged_base_qty:
                child.state = ChildState.RECOVERY
                parent.state = ParentOrderState.RECOVERY
                await self._notify(parent, child, "EXPOSURE_LIMIT")
            elif child.unhedged_base_qty.copy_abs() <= parent.request.hedge_tolerance_base_qty:
                child.state = ChildState.COMPLETED if event.state == "filled" else ChildState.PARTIAL_COMPLETED
                await self._notify(parent, child, "CHILD_TERMINAL")
                await self._advance_parent(parent, child)
        self._persist()

    async def _advance_parent(self, parent: ParentOrder, child: ChildOrder) -> None:
        index = parent.children.index(child)
        if index + 1 < len(parent.children):
            rules = await self.exchange.instrument_rules(parent.request.swap_inst_id)
            await self._place_maker(parent, parent.children[index + 1], rules)
        elif parent.exposure.copy_abs() <= parent.request.hedge_tolerance_base_qty:
            parent.state = ParentOrderState.COMPLETED
            await self._notify(parent, child, "PARENT_COMPLETED")
        self._persist()

    async def _hedge_pending(self, parent: ParentOrder, child: ChildOrder) -> None:
        async with self._hedge_locks[child.child_id]:
            while child.pending_hedge_base_qty > parent.request.hedge_tolerance_base_qty:
                qty = child.pending_hedge_base_qty
                child.state = ChildState.HEDGE_EXECUTING
                child.hedge_attempts += 1
                buy = parent.request.direction is Direction.LONG_SPOT_SHORT_SWAP
                request = OrderRequest(
                    inst_id=parent.request.spot_inst_id,
                    side="buy" if buy else "sell",
                    ord_type="ioc",
                    size=qty,
                    price=None,
                    cl_ord_id=compact_client_id(f"{child.child_id}H{child.hedge_attempts:03d}"),
                    slippage_bps=parent.request.max_spot_slippage_bps,
                )
                try:
                    ack = await self.exchange.place_order(request)
                    child.spot_order_ids.append(ack.ord_id)
                    result = await self.exchange.get_order(request.inst_id, ack.ord_id, ack.cl_ord_id)
                    filled = result.acc_fill_sz
                    child.spot_filled_base_qty += filled
                    child.pending_hedge_base_qty -= filled
                    child.last_spot_fill_px = result.fill_px
                except Exception as exc:
                    if child.hedge_attempts >= parent.request.max_hedge_retries:
                        child.state = ChildState.RECOVERY
                        parent.state = ParentOrderState.RECOVERY
                        parent.error = str(exc)
                        await self._notify(parent, child, "HEDGE_FAILED")
                        self._persist()
                        return
                if child.pending_hedge_base_qty <= parent.request.hedge_tolerance_base_qty:
                    child.pending_hedge_base_qty = Decimal("0")
                    child.state = ChildState.MAKER_WORKING
                elif child.hedge_attempts >= parent.request.max_hedge_retries:
                    child.state = ChildState.RECOVERY
                    parent.state = ParentOrderState.RECOVERY
                    await self._notify(parent, child, "HEDGE_RETRY_EXHAUSTED")
                    self._persist()
                    return

    async def _notify(self, parent: ParentOrder, child: ChildOrder, reason: str) -> None:
        if not self.notifier or not parent.request.lark_report:
            return
        payload = report_payload(parent, child)
        await self.notifier.send(f"{reason}\n{payload}")

    async def reconcile(self) -> list[FillEvent]:
        events = await self.exchange.reconcile(
            list({self.parents_for_child(c).request.swap_inst_id for c in self._children_by_order.values()})
        )
        for event in events:
            await self.on_order_event(event)
        self._persist()
        return events
