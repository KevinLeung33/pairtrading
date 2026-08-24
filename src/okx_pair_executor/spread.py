from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .models import Direction, OrderAction


@dataclass
class MarketSpreadTracker:
    direction: Direction
    action: OrderAction
    _bbo: dict[str, tuple[Decimal, Decimal]]
    _last_ts: float | None = None
    _quote_area: Decimal = Decimal("0")
    _executable_area: Decimal = Decimal("0")
    _elapsed: float = 0.0
    observations: int = 0

    def __init__(self, direction: Direction, action: OrderAction):
        self.direction = direction
        self.action = action
        self._bbo = {}
        self._last_ts = None
        self._quote_area = Decimal("0")
        self._executable_area = Decimal("0")
        self._elapsed = 0.0
        self.observations = 0

    def update(self, inst_id: str, bid: Decimal, ask: Decimal, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        if self._last_ts is not None and self._has_both_books():
            dt = max(0.0, now - self._last_ts)
            quote, executable = self._current_spreads()
            self._quote_area += quote * Decimal(str(dt))
            self._executable_area += executable * Decimal(str(dt))
            self._elapsed += dt
        self._bbo[inst_id] = (bid, ask)
        self._last_ts = now
        if self._has_both_books():
            self.observations += 1

    def seed(self, books: dict[str, tuple[Decimal, Decimal]], now: float | None = None) -> None:
        for inst_id, (bid, ask) in books.items():
            self.update(inst_id, bid, ask, now)

    def _has_both_books(self) -> bool:
        return len(self._bbo) >= 2

    def _sides(self) -> tuple[bool, bool]:
        perp_buy = self.direction is Direction.SHORT_SPOT_LONG_SWAP
        spot_buy = self.direction is Direction.LONG_SPOT_SHORT_SWAP
        if self.action is OrderAction.CLOSE:
            perp_buy = not perp_buy
            spot_buy = not spot_buy
        return perp_buy, spot_buy

    def _current_spreads(self) -> tuple[Decimal, Decimal]:
        spot_bid, spot_ask = self._bbo[next(k for k in self._bbo if not k.endswith("-SWAP"))]
        perp_bid, perp_ask = self._bbo[next(k for k in self._bbo if k.endswith("-SWAP"))]
        perp_buy, spot_buy = self._sides()
        quote_sell = perp_bid if not perp_buy else spot_bid
        quote_buy = spot_bid if not spot_buy else perp_bid
        executable_sell = perp_bid if not perp_buy else spot_bid
        executable_buy = spot_ask if spot_buy else perp_ask
        quote = (quote_sell / quote_buy - Decimal("1")) * Decimal("100") if quote_buy else Decimal("0")
        executable = (executable_sell / executable_buy - Decimal("1")) * Decimal("100") if executable_buy else Decimal("0")
        return quote, executable

    def snapshot(self, now: float | None = None) -> dict[str, Any]:
        now = time.monotonic() if now is None else now
        elapsed = self._elapsed
        quote_area = self._quote_area
        executable_area = self._executable_area
        if self._last_ts is not None and self._has_both_books():
            dt = max(0.0, now - self._last_ts)
            quote, executable = self._current_spreads()
            quote_area += quote * Decimal(str(dt))
            executable_area += executable * Decimal(str(dt))
            elapsed += dt
        quote, executable = self._current_spreads() if self._has_both_books() else (Decimal("0"), Decimal("0"))
        divisor = Decimal(str(elapsed)) if elapsed > 0 else Decimal("0")
        return {
            "quote_twap_rate_pct": str(quote_area / divisor if divisor else quote),
            "executable_twap_rate_pct": str(executable_area / divisor if divisor else executable),
            "last_quote_rate_pct": str(quote),
            "last_executable_rate_pct": str(executable),
            "observations": self.observations,
            "duration_ms": round(elapsed * 1000, 3),
        }