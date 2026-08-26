from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

from .models import (
    Direction,
    OrderAction,
    ParentOrder,
    ParentOrderRequest,
    ParentOrderState,
    report_payload,
)


class BasisStrategyState(str, Enum):
    WAITING_BASIS = "waiting_basis"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    RECOVERY = "recovery"
    STOPPED = "stopped"


@dataclass(frozen=True)
class BasisStrategyConfig:
    entry_threshold_bp: Decimal = Decimal("10")
    pause_threshold_bp: Decimal = Decimal("5")
    resume_threshold_bp: Decimal = Decimal("8")
    exit_threshold_bp: Decimal = Decimal("0")
    resume_exposure_base_qty: Decimal = Decimal("0.005")
    signal_interval_ms: int = 50


class BasisArbStrategy:
    """Basis signal coordinator built on top of the fill-driven PairExecutor.

    The coordinator owns only signal gating and lifecycle decisions. Order
    placement, child splitting, hedging, reconciliation, and recovery remain
    in PairExecutor so this class can later be connected to OMSConsumer.
    """

    def __init__(
        self,
        executor: Any,
        request: ParentOrderRequest,
        config: BasisStrategyConfig,
        notifier: Any | None = None,
    ):
        self.executor = executor
        self.request = request
        self.config = config
        self.notifier = notifier
        self.parent: ParentOrder | None = getattr(executor, "parents", {}).get(request.request_id)
        self.state = self._state_from_parent(self.parent)
        self.books: dict[str, tuple[Decimal, Decimal]] = {}
        self.last_basis_bp: Decimal | None = None
        self.entry_basis_bp: Decimal | None = None
        self.last_pause_reason: str | None = None
        self.last_decision_at = 0.0
        self._lock = asyncio.Lock()
        self._stopping = False

    @staticmethod
    def _state_from_parent(parent: ParentOrder | None) -> BasisStrategyState:
        if parent is None:
            return BasisStrategyState.WAITING_BASIS
        if parent.state is ParentOrderState.RUNNING:
            return BasisStrategyState.RUNNING
        if parent.state is ParentOrderState.PAUSED:
            return BasisStrategyState.PAUSED
        if parent.state is ParentOrderState.COMPLETED:
            return BasisStrategyState.COMPLETED
        if parent.state is ParentOrderState.RECOVERY:
            return BasisStrategyState.RECOVERY
        return BasisStrategyState.STOPPED

    @property
    def request_id(self) -> str:
        return self.request.request_id

    @property
    def terminal(self) -> bool:
        return self.state in {
            BasisStrategyState.COMPLETED,
            BasisStrategyState.RECOVERY,
            BasisStrategyState.STOPPED,
        }

    @property
    def exposure(self) -> Decimal:
        return self.parent.exposure if self.parent is not None else Decimal("0")

    def update_book(self, inst_id: str, best_bid: Decimal, best_ask: Decimal) -> None:
        self.books[inst_id] = (best_bid, best_ask)

    def current_basis_bp(self) -> Decimal | None:
        spot = self.books.get(self.request.spot_inst_id)
        swap = self.books.get(self.request.swap_inst_id)
        if spot is None or swap is None:
            return None
        spot_bid, spot_ask = spot
        perp_bid, perp_ask = swap
        perp_buy = self.request.direction is Direction.SHORT_SPOT_LONG_SWAP
        spot_buy = self.request.direction is Direction.LONG_SPOT_SHORT_SWAP
        if self.request.action is OrderAction.CLOSE:
            perp_buy = not perp_buy
            spot_buy = not spot_buy
        if perp_buy and not spot_buy:
            # Buy perp and sell spot: conservative executable reference.
            numerator = spot_bid - perp_ask
            denominator = perp_ask
        elif not perp_buy and spot_buy:
            # Sell perp and buy spot: conservative executable reference.
            numerator = perp_bid - spot_ask
            denominator = spot_ask
        else:
            raise ValueError("basis strategy requires opposite spot/perp legs")
        if denominator <= 0:
            return None
        return numerator / denominator * Decimal("10000")

    def _start_threshold(self) -> Decimal:
        return (
            self.config.entry_threshold_bp
            if self.request.action is OrderAction.OPEN
            else self.config.exit_threshold_bp
        )

    def _maintain_threshold(self) -> Decimal:
        return (
            self.config.pause_threshold_bp
            if self.request.action is OrderAction.OPEN
            else self.config.exit_threshold_bp
        )

    def _resume_threshold(self) -> Decimal:
        return (
            self.config.resume_threshold_bp
            if self.request.action is OrderAction.OPEN
            else self.config.exit_threshold_bp
        )

    async def on_book(self, inst_id: str, best_bid: Decimal, best_ask: Decimal) -> None:
        self.update_book(inst_id, best_bid, best_ask)
        basis = self.current_basis_bp()
        if basis is None:
            return
        self.last_basis_bp = basis
        now = time.perf_counter()
        if (now - self.last_decision_at) * 1000 < self.config.signal_interval_ms:
            return
        self.last_decision_at = now
        async with self._lock:
            await self._decide(basis)

    async def refresh(self) -> None:
        async with self._lock:
            if self.parent is None:
                return
            if self.parent.state is ParentOrderState.COMPLETED:
                self.state = BasisStrategyState.COMPLETED
            elif self.parent.state is ParentOrderState.RECOVERY:
                self.state = BasisStrategyState.RECOVERY

    async def _decide(self, basis: Decimal) -> None:
        if self._stopping or self.terminal:
            return
        if self.parent is None:
            if basis >= self._start_threshold():
                await self._start_parent(basis)
            return

        if self.parent.state is ParentOrderState.COMPLETED:
            self.state = BasisStrategyState.COMPLETED
            return
        if self.parent.state is ParentOrderState.RECOVERY:
            self.state = BasisStrategyState.RECOVERY
            return
        if self.parent.state is ParentOrderState.PAUSED:
            if (
                basis >= self._resume_threshold()
                and self.exposure.copy_abs() <= self.config.resume_exposure_base_qty
            ):
                await self.executor.resume_parent(self.request_id)
                if self.parent.state is ParentOrderState.RUNNING:
                    self.state = BasisStrategyState.RUNNING
                    await self._notify("BASIS_RESUMED")
            return
        if self.parent.state is not ParentOrderState.RUNNING:
            return

        basis_lost = basis < self._maintain_threshold()
        exposure_limit = self.exposure.copy_abs() > self.request.max_unhedged_base_qty
        if basis_lost or exposure_limit:
            reason = "exposure_limit" if exposure_limit else "basis_lost"
            self.last_pause_reason = reason
            await self.executor.pause_parent(self.request_id, reason)
            if self.parent.state is ParentOrderState.PAUSED:
                self.state = BasisStrategyState.PAUSED
                await self._notify("BASIS_PAUSED")
            elif self.parent.state is ParentOrderState.RECOVERY:
                self.state = BasisStrategyState.RECOVERY

    async def _start_parent(self, basis: Decimal) -> None:
        self.parent = await self.executor.submit(self.request)
        self.entry_basis_bp = basis
        self.state = BasisStrategyState.RUNNING
        await self._notify("BASIS_STARTED")

    async def shutdown(self) -> None:
        async with self._lock:
            if self.terminal:
                return
            self._stopping = True
            if self.parent is not None and self.parent.state is ParentOrderState.RUNNING:
                await self.executor.pause_parent(self.request_id, "shutdown")
            self.state = BasisStrategyState.STOPPED

    async def _notify(self, reason: str) -> None:
        if self.notifier is None or not self.request.lark_report or self.parent is None:
            return
        payload = report_payload(
            self.parent,
            self.parent.children[0] if self.parent.children else None,
        )
        payload["strategy"] = {
            "state": self.state.value,
            "basis_bp": str(self.last_basis_bp) if self.last_basis_bp is not None else "0",
            "entry_basis_bp": str(self.entry_basis_bp) if self.entry_basis_bp is not None else "0",
            "entry_threshold_bp": str(self._start_threshold()),
            "pause_threshold_bp": str(self._maintain_threshold()),
            "resume_threshold_bp": str(self._resume_threshold()),
            "reason": self.last_pause_reason or "",
        }
        try:
            if hasattr(self.notifier, "send_report"):
                await self.notifier.send_report(reason, payload)
            else:
                await self.notifier.send(f"{reason}\n{payload}")
        except Exception:
            logging.exception("basis strategy notification failed: %s", reason)
