from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from .efficiency import ExecutionEfficiency
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
from .spread import MarketSpreadTracker


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
        self._latest_bbo: dict[str, tuple[Decimal, Decimal]] = {}
        self._bbo_updated_at: dict[str, float] = {}
        self._efficiency: dict[str, ExecutionEfficiency] = {}
        self._reprice_tasks: dict[str, asyncio.Task[None]] = {}
        self._market_spreads: dict[str, MarketSpreadTracker] = {}
        self._terminal_report_sent: set[str] = set()
        self._runtime_warnings: dict[str, str] = {}

    def restore(self) -> dict[str, ParentOrder]:
        if not self.store:
            return self.parents
        self.parents = self.store.load()
        self._children_by_order.clear()
        self._hedge_locks.clear()
        for parent in self.parents.values():
            for child in parent.children:
                self._hedge_locks[child.child_id] = asyncio.Lock()
                if child.perp_order_id and child.state in {
                    ChildState.MAKER_WORKING,
                    ChildState.REPRICING,
                    ChildState.HEDGE_PENDING,
                    ChildState.HEDGE_EXECUTING,
                }:
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
        swap_base_step = swap_rules.lot_size * swap_rules.contract_value
        base_step = max(spot_rules.lot_size, swap_base_step)
        min_base_qty = max(spot_rules.min_size, swap_rules.min_size * swap_rules.contract_value)
        if request.target_base_qty < min_base_qty:
            raise ValueError("target quantity below effective minimum")
        if floor_to_step(request.target_base_qty, base_step) != request.target_base_qty:
            raise ValueError("target quantity is not aligned to spot/swap executable step")

        parent = ParentOrder(request=request, state=ParentOrderState.RUNNING)
        tracker = MarketSpreadTracker(request.direction, request.action)
        tracker.seed(self._latest_bbo)
        self._market_spreads[request.request_id] = tracker
        self._efficiency[request.request_id] = ExecutionEfficiency()
        if hasattr(self.exchange, "account_snapshot"):
            try:
                parent.request.account_before.update(
                    await self.exchange.account_snapshot(
                        [request.spot_inst_id, request.swap_inst_id]
                    )
                )
            except Exception:
                # Account reporting must not prevent a protected Demo order from being submitted.
                pass
        remaining = request.target_base_qty
        index = 1
        while remaining > 0:
            child_base = min(remaining, request.child_base_qty)
            child_base = floor_to_step(child_base, base_step)
            residual = remaining - child_base
            if residual > 0 and residual < min_base_qty:
                # Do not create an unexecutable tail child. Merge the dust
                # into the current batch; child_base is allowed to exceed
                # the preferred child size by this small tail.
                child_base = remaining
            if child_base <= 0:
                break
            perp_contracts = floor_to_step(child_base / swap_rules.contract_value, swap_rules.lot_size)
            validate_size(perp_contracts, swap_rules)
            if perp_contracts * swap_rules.contract_value != child_base:
                raise ValueError("child quantity is not exactly representable by swap contracts")
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
        try:
            await self._place_maker(parent, parent.children[0], swap_rules, reprice=False)
        except Exception as exc:
            parent.state = ParentOrderState.RECOVERY
            parent.error = f"Maker submission failed: {exc}"
            self._persist()
            await self._notify(parent, parent.children[0], "EXECUTION_RISK")
            return parent
        self._persist()
        await self._notify(parent, parent.children[0], "ORDER_STARTED")
        return parent

    async def _place_maker(self, parent: ParentOrder, child: ChildOrder, rules: InstrumentRules, *, reprice: bool = False) -> None:
        buy = parent.request.direction is Direction.SHORT_SPOT_LONG_SWAP
        if parent.request.action.value == "close":
            buy = not buy
        child.maker_attempts += 1
        cl_ord_id = compact_client_id(f"{child.child_id}M{child.maker_attempts:03d}")
        started = time.perf_counter()
        quote_age_ms = max(0.0, (started - self._bbo_updated_at.get(parent.request.swap_inst_id, started)) * 1000)
        request = OrderRequest(
            inst_id=parent.request.swap_inst_id,
            side="buy" if buy else "sell",
            ord_type="post_only",
            size=child.perp_target_contracts,
            price=await self.exchange.maker_price(parent.request.swap_inst_id, "buy" if buy else "sell"),
            cl_ord_id=cl_ord_id,
            reduce_only=parent.request.action.value == "close",
            td_mode="cross",
        )
        ack = await self.exchange.place_order(request)
        metrics = self._efficiency.get(parent.request.request_id)
        if metrics is not None:
            metrics.maker_submitted(child.child_id, (time.perf_counter() - started) * 1000, quote_age_ms, reprice)
        child.perp_order_id = ack.ord_id
        if ack.ord_id not in child.perp_order_ids:
            child.perp_order_ids.append(ack.ord_id)
        child.perp_cl_ord_id = ack.cl_ord_id
        child.state = ChildState.MAKER_WORKING
        child.maker_price = request.price or Decimal("0")
        self._children_by_order[ack.ord_id] = child
        self._persist()

    def record_book(self, inst_id: str, best_bid: Decimal, best_ask: Decimal) -> None:
        self._latest_bbo[inst_id] = (best_bid, best_ask)
        self._bbo_updated_at[inst_id] = time.perf_counter()
        for metrics in self._efficiency.values():
            metrics.record_bbo()
        for tracker in self._market_spreads.values():
            tracker.update(inst_id, best_bid, best_ask)

    def record_bbo_queue(self, coalesced: bool) -> None:
        for metrics in self._efficiency.values():
            metrics.record_bbo_queue(coalesced)

    async def process_book(self, inst_id: str, received_at: float | None = None) -> None:
        if received_at is not None:
            queue_age_ms = max(0.0, (time.perf_counter() - received_at) * 1000)
            for metrics in self._efficiency.values():
                metrics.book_dispatched(queue_age_ms)
        for parent in self.parents.values():
            if parent.request.swap_inst_id != inst_id:
                continue
            buy = parent.request.direction is Direction.SHORT_SPOT_LONG_SWAP
            if parent.request.action.value == "close":
                buy = not buy
            bbo = self._latest_bbo.get(inst_id)
            if not bbo:
                continue
            target = bbo[0] if buy else bbo[1]
            for child in parent.children:
                if child.state is not ChildState.MAKER_WORKING or not child.perp_order_id:
                    continue
                if child.maker_price == target or child.child_id in self._reprice_tasks:
                    continue
                self._reprice_tasks[child.child_id] = asyncio.create_task(
                    self._debounced_reprice(child.child_id),
                    name=f"reprice-{child.child_id}",
                )

    async def on_book(self, inst_id: str, best_bid: Decimal, best_ask: Decimal) -> None:
        self.record_book(inst_id, best_bid, best_ask)
        await self.process_book(inst_id)

    async def _debounced_reprice(self, child_id: str) -> None:
        child = next((child for parent in self.parents.values() for child in parent.children
                      if child.child_id == child_id), None)
        was_cancelled = False
        try:
            if child is None:
                return
            parent = self.parents_for_child(child)
            await asyncio.sleep(max(parent.request.maker_reprice_interval_ms, 0) / 1000)
            if child.state is not ChildState.MAKER_WORKING or not child.perp_order_id:
                return
            bbo = self._latest_bbo.get(parent.request.swap_inst_id)
            if not bbo:
                return
            buy = parent.request.direction is Direction.SHORT_SPOT_LONG_SWAP
            if parent.request.action.value == "close":
                buy = not buy
            target = bbo[0] if buy else bbo[1]
            if target != child.maker_price:
                await self.reprice_child(child_id)
        except asyncio.CancelledError:
            was_cancelled = True
            raise
        except Exception as exc:
            if child is not None:
                parent = self.parents_for_child(child)
                child.state = ChildState.RECOVERY
                parent.state = ParentOrderState.RECOVERY
                parent.error = f"Maker reprice failed: {exc}"
                await self._notify(parent, child, "REPRICE_FAILED")
                self._persist()
        finally:
            self._reprice_tasks.pop(child_id, None)
            if was_cancelled:
                return
            if child is not None and child.state is ChildState.MAKER_WORKING and child.perp_order_id:
                parent = self.parents_for_child(child)
                bbo = self._latest_bbo.get(parent.request.swap_inst_id)
                if bbo:
                    buy = parent.request.direction is Direction.SHORT_SPOT_LONG_SWAP
                    if parent.request.action.value == "close":
                        buy = not buy
                    target = bbo[0] if buy else bbo[1]
                    if target != child.maker_price and child.child_id not in self._reprice_tasks:
                        self._reprice_tasks[child.child_id] = asyncio.create_task(
                            self._debounced_reprice(child.child_id),
                            name=f"reprice-{child.child_id}",
                        )

    async def stop_repricing(self) -> None:
        tasks = list(self._reprice_tasks.values())
        self._reprice_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def cancel_active_makers(self) -> None:
        for parent in self.parents.values():
            for child in parent.children:
                if not child.perp_order_id or child.state not in {ChildState.MAKER_WORKING, ChildState.REPRICING}:
                    continue
                await self.exchange.cancel_order(
                    parent.request.swap_inst_id,
                    child.perp_order_id,
                    child.perp_cl_ord_id or "",
                )
                self._children_by_order.pop(child.perp_order_id, None)
                child.state = ChildState.RECOVERY
                parent.state = ParentOrderState.RECOVERY
                parent.error = "controlled shutdown canceled active Maker"
        self._persist()

    async def pause_parent(self, request_id: str, reason: str = "paused") -> ParentOrder:
        parent = self.parents[request_id]
        if parent.state in {
            ParentOrderState.COMPLETED,
            ParentOrderState.FAILED,
            ParentOrderState.RECOVERY,
            ParentOrderState.PAUSED,
        }:
            return parent
        await self.stop_repricing()
        parent.state = ParentOrderState.PAUSED
        for child in list(parent.children):
            if child.state not in {ChildState.MAKER_WORKING, ChildState.REPRICING} or not child.perp_order_id:
                continue
            order_id = child.perp_order_id
            try:
                await self.exchange.cancel_order(
                    parent.request.swap_inst_id, order_id, child.perp_cl_ord_id or "",
                )
            except Exception:
                # The order may have filled during the cancel request. The
                # subsequent order query decides whether there is residual work.
                pass
            try:
                resolved = await self.exchange.get_order(
                    parent.request.swap_inst_id, order_id, child.perp_cl_ord_id or "",
                )
            except Exception as exc:
                parent.state = ParentOrderState.RECOVERY
                parent.error = f"unable to resolve paused Maker: {exc}"
                await self._notify(parent, child, "REPRICE_FAILED")
                continue
            resolved_state = (
                "filled"
                if resolved.acc_fill_sz >= child.perp_target_contracts
                else "canceled"
            )
            await self.on_order_event(FillEvent(
                ord_id=resolved.ord_id,
                cl_ord_id=resolved.cl_ord_id,
                inst_id=resolved.inst_id,
                state=resolved_state,
                acc_fill_sz=resolved.acc_fill_sz,
                fill_px=resolved.fill_px,
                fee=resolved.fee,
                trade_id=resolved.trade_id,
                order_price=resolved.order_price,
                amend_result=resolved.amend_result,
            ))
        if parent.state is ParentOrderState.PAUSED and (
            parent.filled_base_qty >= parent.request.target_base_qty
            and parent.exposure.copy_abs() <= parent.request.hedge_tolerance_base_qty
        ):
            parent.state = ParentOrderState.COMPLETED
            if parent.children:
                await self._notify(parent, parent.children[-1], "PARENT_COMPLETED")
        self._persist()
        return parent

    async def resume_parent(self, request_id: str) -> ParentOrder:
        parent = self.parents[request_id]
        if parent.state is not ParentOrderState.PAUSED:
            return parent
        if parent.filled_base_qty >= parent.request.target_base_qty and (
            parent.exposure.copy_abs() <= parent.request.hedge_tolerance_base_qty
        ):
            parent.state = ParentOrderState.COMPLETED
            if parent.children:
                await self._notify(parent, parent.children[-1], "PARENT_COMPLETED")
            self._persist()
            return parent
        child = next(
            (item for item in parent.children if item.state in {ChildState.CREATED, ChildState.PARTIAL_COMPLETED}),
            None,
        )
        if child is None:
            parent.state = ParentOrderState.RECOVERY
            parent.error = "paused parent has no resumable child"
            self._persist()
            return parent
        total_contracts = child.target_base_qty / child.contract_value
        remaining_contracts = total_contracts - child.perp_filled_contracts
        if remaining_contracts <= 0:
            parent.state = ParentOrderState.RECOVERY
            parent.error = "paused child has no executable residual"
            self._persist()
            return parent
        child.perp_target_contracts = remaining_contracts
        child.active_order_filled_contracts = Decimal("0")
        child.pending_hedge_base_qty = Decimal("0")
        child.state = ChildState.CREATED
        parent.state = ParentOrderState.RUNNING
        parent.error = None
        rules = await self.exchange.instrument_rules(parent.request.swap_inst_id)
        await self._place_maker(parent, child, rules, reprice=False)
        await self._notify(parent, child, "CHILD_STARTED")
        self._persist()
        return parent

    async def reprice_child(self, child_id: str) -> None:
        child = next(child for parent in self.parents.values() for child in parent.children if child.child_id == child_id)
        parent = self.parents_for_child(child)
        if child.state in {ChildState.RECOVERY, ChildState.COMPLETED, ChildState.PARTIAL_COMPLETED, ChildState.FAILED}:
            return
        if not child.perp_order_id or child.state not in {ChildState.MAKER_WORKING, ChildState.REPRICING}:
            return
        child.state = ChildState.REPRICING
        old_order_id = child.perp_order_id
        if hasattr(self.exchange, "amend_order"):
            amend_started = time.perf_counter()
            try:
                target = await self.exchange.maker_price(
                    parent.request.swap_inst_id,
                    "buy" if (
                        parent.request.direction is Direction.SHORT_SPOT_LONG_SWAP
                        and parent.request.action.value != "close"
                    ) or (
                        parent.request.direction is Direction.LONG_SPOT_SHORT_SWAP
                        and parent.request.action.value == "close"
                    ) else "sell",
                )
                quote_age_ms = max(
                    0.0,
                    (amend_started - self._bbo_updated_at.get(
                        parent.request.swap_inst_id,
                        amend_started,
                    )) * 1000,
                )
                await self.exchange.amend_order(
                    parent.request.swap_inst_id,
                    old_order_id,
                    child.perp_cl_ord_id or "",
                    target,
                )
                metrics = self._efficiency.get(parent.request.request_id)
                if metrics is not None:
                    metrics.maker_amended(
                        (time.perf_counter() - amend_started) * 1000,
                        quote_age_ms,
                    )
                # A fill can arrive while amend-order is in flight. In that
                # case the order event owns the state transition; do not
                # overwrite HEDGE_* or terminal state with MAKER_WORKING.
                if child.state is ChildState.REPRICING:
                    child.maker_price = target
                    child.state = ChildState.MAKER_WORKING
                self._persist()
                return
            except Exception:
                # An amend can race with a fill or cancellation. Fall back to
                # the proven cancel-and-replace path after resolving the order.
                pass
        try:
            await self.exchange.cancel_order(
                parent.request.swap_inst_id,
                old_order_id,
                child.perp_cl_ord_id or "",
            )
        except Exception:
            # The order may have filled or been canceled between the BBO event
            # and cancel-order. Resolve its final cumulative fill before
            # deciding whether a residual Maker must be recreated.
            resolved = await self.exchange.get_order(
                parent.request.swap_inst_id,
                old_order_id,
                child.perp_cl_ord_id or "",
            )
            resolved_state = (
                "filled"
                if resolved.acc_fill_sz >= child.perp_target_contracts
                else "partially_filled"
            )
            await self.on_order_event(FillEvent(
                ord_id=resolved.ord_id,
                cl_ord_id=resolved.cl_ord_id,
                inst_id=resolved.inst_id,
                state=resolved_state,
                acc_fill_sz=resolved.acc_fill_sz,
                fill_px=resolved.fill_px,
                fee=resolved.fee,
                trade_id=resolved.trade_id,
            ))
        self._children_by_order.pop(old_order_id, None)
        if child.state in {ChildState.COMPLETED, ChildState.PARTIAL_COMPLETED, ChildState.RECOVERY, ChildState.FAILED}:
            self._persist()
            return
        if child.perp_filled_contracts < child.perp_target_contracts:
            rules = await self.exchange.instrument_rules(parent.request.swap_inst_id)
            remaining = child.perp_target_contracts - child.perp_filled_contracts
            child.perp_target_contracts = remaining
            child.active_order_filled_contracts = Decimal("0")
            await self._place_maker(parent, child, rules, reprice=True)
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
            metrics = self._efficiency.get(parent.request.request_id)
            if metrics is not None:
                metrics.maker_filled(child.child_id)
            delta_base = delta * child.contract_value
            child.pending_hedge_base_qty += delta_base
            child.spot_target_base_qty += delta_base
            child.state = ChildState.HEDGE_PENDING
            await self._hedge_pending(parent, child)
            if child.state is ChildState.RECOVERY:
                self._persist()
                return

        if event.amend_result:
            if event.order_price:
                child.maker_price = event.order_price
            elif event.amend_result != "0":
                # A failed amendment leaves the original order unchanged.
                # Make the next BBO event eligible to retry the amendment.
                child.maker_price = Decimal("0")
            if child.state is ChildState.REPRICING:
                child.state = ChildState.MAKER_WORKING
        if event.state in {"filled", "canceled"} and child.state is not ChildState.REPRICING:
            if child.unhedged_base_qty.copy_abs() > parent.request.max_unhedged_base_qty:
                child.state = ChildState.RECOVERY
                parent.state = ParentOrderState.RECOVERY
                await self._notify(parent, child, "EXPOSURE_LIMIT")
            elif child.unhedged_base_qty.copy_abs() <= parent.request.hedge_tolerance_base_qty:
                child.state = ChildState.COMPLETED if event.state == "filled" else ChildState.PARTIAL_COMPLETED
                await self._notify(parent, child, "CHILD_TERMINAL")
                if parent.state is not ParentOrderState.PAUSED:
                    await self._advance_parent(parent, child)
        if child.state in {
            ChildState.COMPLETED,
            ChildState.PARTIAL_COMPLETED,
            ChildState.RECOVERY,
            ChildState.FAILED,
        }:
            self._children_by_order.pop(event.ord_id, None)
        self._persist()

    async def _advance_parent(self, parent: ParentOrder, child: ChildOrder) -> None:
        index = parent.children.index(child)
        if child.perp_filled_contracts < child.perp_target_contracts:
            if child.maker_attempts >= parent.request.max_maker_attempts:
                parent.state = ParentOrderState.RECOVERY
                parent.error = (
                    f"Maker retry limit reached for {child.child_id}; "
                    f"filled {child.perp_filled_contracts} of {child.perp_target_contracts} contracts"
                )
                await self._notify(parent, child, "MAKER_RETRY_EXHAUSTED")
                self._persist()
                return
            remaining = child.perp_target_contracts - child.perp_filled_contracts
            child.perp_target_contracts = remaining
            child.active_order_filled_contracts = Decimal("0")
            child.pending_hedge_base_qty = Decimal("0")
            child.state = ChildState.CREATED
            try:
                rules = await self.exchange.instrument_rules(parent.request.swap_inst_id)
                await self._place_maker(parent, child, rules, reprice=True)
            except Exception as exc:
                parent.state = ParentOrderState.RECOVERY
                parent.error = f"Maker submission failed while requeueing {child.child_id}: {exc}"
                await self._notify(parent, child, "EXECUTION_RISK")
                self._persist()
                return
            await self._notify(parent, child, "CHILD_STARTED")
            self._persist()
            return
        if index + 1 < len(parent.children):
            next_child = parent.children[index + 1]
            try:
                rules = await self.exchange.instrument_rules(parent.request.swap_inst_id)
                await self._place_maker(parent, next_child, rules, reprice=False)
            except Exception as exc:
                parent.state = ParentOrderState.RECOVERY
                parent.error = f"Maker submission failed for {next_child.child_id}: {exc}"
                await self._notify(parent, next_child, "EXECUTION_RISK")
                self._persist()
                return
            await self._notify(parent, next_child, "CHILD_STARTED")
        elif parent.exposure.copy_abs() <= parent.request.hedge_tolerance_base_qty:
            if parent.filled_base_qty >= parent.request.target_base_qty:
                parent.state = ParentOrderState.COMPLETED
                await self._notify(parent, child, "PARENT_COMPLETED")
            else:
                parent.state = ParentOrderState.RECOVERY
                parent.error = (
                    f"target incomplete: filled {parent.filled_base_qty} "
                    f"of {parent.request.target_base_qty}"
                )
                await self._notify(parent, child, "TARGET_INCOMPLETE")
        self._persist()

    async def _hedge_pending(self, parent: ParentOrder, child: ChildOrder) -> None:
        spot_rules = await self.exchange.instrument_rules(parent.request.spot_inst_id)
        async with self._hedge_locks[child.child_id]:
            while child.pending_hedge_base_qty > parent.request.hedge_tolerance_base_qty:
                qty = child.pending_hedge_base_qty
                if qty < spot_rules.min_size:
                    if qty <= parent.request.hedge_tolerance_base_qty:
                        child.pending_hedge_base_qty = Decimal("0")
                        child.state = ChildState.MAKER_WORKING
                        return
                    child.state = ChildState.RECOVERY
                    parent.state = ParentOrderState.RECOVERY
                    parent.error = (
                        f"residual hedge {qty} below spot minimum {spot_rules.min_size}"
                    )
                    await self._notify(parent, child, "HEDGE_FAILED")
                    self._persist()
                    return
                child.state = ChildState.HEDGE_EXECUTING
                child.hedge_attempts += 1
                buy = parent.request.direction is Direction.LONG_SPOT_SHORT_SWAP
                if parent.request.action.value == "close":
                    buy = not buy
                request = OrderRequest(
                    inst_id=parent.request.spot_inst_id,
                    side="buy" if buy else "sell",
                    ord_type="ioc",
                    size=qty,
                    price=await self.exchange.ioc_price(parent.request.spot_inst_id, "buy" if buy else "sell", parent.request.max_spot_slippage_bps),
                    cl_ord_id=compact_client_id(f"{child.child_id}H{child.hedge_attempts:03d}"),
                    slippage_bps=parent.request.max_spot_slippage_bps,
                    td_mode=parent.request.spot_td_mode,
                )
                hedge_started = time.perf_counter()
                try:
                    ack_started = time.perf_counter()
                    ack = await self.exchange.place_order(request)
                    ack_ms = (time.perf_counter() - ack_started) * 1000
                    child.spot_order_ids.append(ack.ord_id)
                    result = await self.exchange.get_order(request.inst_id, ack.ord_id, ack.cl_ord_id)
                    filled = result.acc_fill_sz
                    child.spot_filled_base_qty += filled
                    child.pending_hedge_base_qty -= filled
                    child.last_spot_fill_px = result.fill_px
                    self.clear_runtime_warning(parent.request.request_id)
                    metrics = self._efficiency.get(parent.request.request_id)
                    if metrics is not None:
                        metrics.hedge_submitted(float(qty), ack_ms, (time.perf_counter() - hedge_started) * 1000, float(filled))
                except Exception as exc:
                    await self.notify_runtime_warning(
                        parent.request.request_id,
                        f"Spot hedge order error (attempt {child.hedge_attempts}): {exc}",
                    )
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

    async def notify_runtime_warning(self, request_id: str, message: str) -> None:
        """Report a recoverable transport/order issue without stopping execution."""
        parent = self.parents.get(request_id)
        if parent is None or parent.state in {
            ParentOrderState.COMPLETED,
            ParentOrderState.FAILED,
            ParentOrderState.CANCELED,
            ParentOrderState.RECOVERY,
        }:
            return
        if self._runtime_warnings.get(request_id) == message:
            return
        self._runtime_warnings[request_id] = message
        parent.error = message
        self._persist()
        await self._notify(parent, None, "EXECUTION_WARNING")

    def clear_runtime_warning(self, request_id: str) -> None:
        self._runtime_warnings.pop(request_id, None)
        parent = self.parents.get(request_id)
        if parent is not None and parent.state is ParentOrderState.RUNNING:
            parent.error = None
    async def fail_orphaned_parent(self, request_id: str) -> bool:
        """Stop a restored RUNNING parent that has no known active order.

        Re-submitting here would be unsafe because the exchange may have an
        order that was not persisted locally. Manual reconciliation is safer.
        """
        parent = self.parents.get(request_id)
        if parent is None or parent.state is not ParentOrderState.RUNNING:
            return False
        active_states = {
            ChildState.MAKER_WORKING,
            ChildState.REPRICING,
            ChildState.HEDGE_PENDING,
            ChildState.HEDGE_EXECUTING,
        }
        if any(
            child.state in active_states and child.perp_order_id
            for child in parent.children
        ):
            return False
        parent.state = ParentOrderState.RECOVERY
        parent.error = (
            "restored running parent has no known active order; "
            "manual exchange reconciliation required"
        )
        self._persist()
        await self._notify(parent, None, "EXECUTION_RISK")
        return True
    async def notify_status(self, request_id: str) -> None:
        """Send a task-level progress snapshot for a non-terminal parent."""
        parent = self.parents.get(request_id)
        if parent is None or parent.state in {
            ParentOrderState.COMPLETED,
            ParentOrderState.FAILED,
            ParentOrderState.CANCELED,
            ParentOrderState.RECOVERY,
        }:
            return
        await self._notify(parent, None, "EXECUTION_STATUS")

    async def notify_terminal(self, request_id: str) -> None:
        """Retry the terminal report from the live state during shutdown."""
        parent = self.parents.get(request_id)
        if parent is None or not self.notifier or not parent.request.lark_report:
            return
        if parent.state is ParentOrderState.COMPLETED:
            if request_id not in self._terminal_report_sent:
                await self._notify(parent, parent.children[-1] if parent.children else None, "PARENT_COMPLETED")
        elif parent.state in {ParentOrderState.RECOVERY, ParentOrderState.FAILED}:
            if request_id not in self._terminal_report_sent:
                await self._notify(parent, parent.children[-1] if parent.children else None, "EXECUTION_RISK")

    async def _notify(self, parent: ParentOrder, child: ChildOrder | None, reason: str) -> bool:
        if reason in {"CHILD_STARTED", "CHILD_TERMINAL"}:
            # Child execution remains internal; only task-level notifications
            # are emitted to Lark.
            return True
        request_id = parent.request.request_id
        if reason == "PARENT_COMPLETED" and request_id in self._terminal_report_sent:
            return True
        if not self.notifier or not parent.request.lark_report:
            return True

        execution = None
        if reason in {"PARENT_COMPLETED", "EXECUTION_STATUS"} and hasattr(self.exchange, "execution_details"):
            try:
                execution = await self.exchange.execution_details(
                    parent,
                    child=None,
                    include_account=reason == "PARENT_COMPLETED",
                )
            except Exception as exc:
                execution = {"report_error": str(exc)}

        if execution is not None and reason in {"PARENT_COMPLETED", "EXECUTION_STATUS"}:
            # The order event/state is authoritative for cumulative quantity.
            # REST fill aggregation can briefly lag behind the final IOC.
            legs = execution.setdefault("legs", {})
            perp_leg = legs.setdefault("perp", {})
            spot_leg = legs.setdefault("spot", {})
            fill_data_available = execution.get("fill_data_available")
            perp_leg["filled_base_qty"] = str(parent.filled_base_qty)
            spot_leg["filled_base_qty"] = str(parent.hedged_base_qty)
            execution["state_progress"] = {
                "filled_base_qty": str(parent.filled_base_qty),
                "hedged_base_qty": str(parent.hedged_base_qty),
                "exposure": str(parent.exposure),
            }
            tracker = self._market_spreads.get(request_id)
            if tracker is not None:
                market = tracker.snapshot()
                execution["market_spread"] = market
                if market.get("observations", 0) and fill_data_available is True:
                    actual = Decimal(execution.get("effective_spread_rate_pct", "0"))
                    market_exec = Decimal(market.get("executable_twap_rate_pct", "0"))
                    execution["execution_vs_market_executable_rate_pct"] = str(actual - market_exec)
                elif market.get("observations", 0):
                    execution["execution_vs_market_executable_rate_pct"] = "N/A"
            metrics = self._efficiency.get(request_id)
            if metrics is not None:
                execution["efficiency"] = metrics.snapshot()
                if reason == "PARENT_COMPLETED":
                    report_dir = Path(os.getenv("EFFICIENCY_REPORT_DIR", "runtime/reports"))
                    report_dir.mkdir(parents=True, exist_ok=True)
                    report_path = report_dir / f"execution-efficiency-{request_id}.json"
                    efficiency_report = {
                        "request_id": request_id,
                        "direction": parent.request.direction.value,
                        "action": parent.request.action.value,
                        "efficiency": execution.get("efficiency", {}),
                        "market_spread": execution.get("market_spread", {}),
                        "execution_vs_market_executable_rate_pct": execution.get("execution_vs_market_executable_rate_pct", "N/A"),
                        "legs": execution.get("legs", {}),
                        "unhedged_base_qty": execution.get("unhedged_base_qty", "0"),
                        "children": [item.child_id for item in parent.children],
                    }
                    report_path.write_text(json.dumps(efficiency_report, indent=2, ensure_ascii=False), encoding="utf-8")
                    execution["efficiency_report_path"] = str(report_path)

        payload = report_payload(parent, child, execution)
        try:
            if hasattr(self.notifier, "send_report"):
                await self.notifier.send_report(reason, payload)
            else:
                await self.notifier.send(f"{reason}\n{payload}")
        except Exception:
            # Notification failure must not terminate order management. A
            # completed parent is retried by notify_terminal in main finally.
            logging.exception("failed to send %s report for %s", reason, request_id)
            return False
        if reason in {"PARENT_COMPLETED", "EXECUTION_RISK"}:
            self._terminal_report_sent.add(request_id)
        return True

    async def reconcile(self) -> list[FillEvent]:
        # The private WebSocket orders channel is the primary source of order
        # state. REST is only a safety check for currently active known orders;
        # querying orders-history on every loop is both redundant and rate-limit
        # intensive.
        events: list[FillEvent] = []
        for order_id, child in list(self._children_by_order.items()):
            if child.state not in {
                ChildState.MAKER_WORKING,
                ChildState.REPRICING,
                ChildState.HEDGE_PENDING,
                ChildState.HEDGE_EXECUTING,
            }:
                self._children_by_order.pop(order_id, None)
                continue
            parent = self.parents_for_child(child)
            try:
                event = await self.exchange.get_order(
                    parent.request.swap_inst_id,
                    order_id,
                    child.perp_cl_ord_id or "",
                )
            except Exception as exc:
                await self.notify_runtime_warning(
                    parent.request.request_id,
                    f"Maker REST核对失败: {exc}",
                )
                continue
            events.append(event)
        for event in events:
            await self.on_order_event(event)
        self._persist()
        return events
