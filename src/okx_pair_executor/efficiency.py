from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


def _summary(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "avg_ms": 0, "p95_ms": 0, "max_ms": 0}
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
    return {
        "count": len(values),
        "avg_ms": round(sum(values) / len(values), 3),
        "p95_ms": round(p95, 3),
        "max_ms": round(max(values), 3),
    }


@dataclass
class ExecutionEfficiency:
    started_at: float = field(default_factory=time.perf_counter)
    bbo_events: int = 0
    maker_reprices: int = 0
    maker_ack_latencies: list[float] = field(default_factory=list)
    maker_quote_ages: list[float] = field(default_factory=list)
    maker_waits: list[float] = field(default_factory=list)
    hedge_ack_latencies: list[float] = field(default_factory=list)
    hedge_roundtrips: list[float] = field(default_factory=list)
    hedge_requested_qty: float = 0.0
    hedge_filled_qty: float = 0.0
    maker_started_at: dict[str, float] = field(default_factory=dict)

    def record_bbo(self) -> None:
        self.bbo_events += 1

    def maker_submitted(self, child_id: str, latency_ms: float, quote_age_ms: float, reprice: bool) -> None:
        self.maker_ack_latencies.append(latency_ms)
        self.maker_quote_ages.append(quote_age_ms)
        self.maker_started_at[child_id] = time.perf_counter()
        if reprice:
            self.maker_reprices += 1

    def maker_filled(self, child_id: str) -> None:
        started = self.maker_started_at.pop(child_id, None)
        if started is not None:
            self.maker_waits.append((time.perf_counter() - started) * 1000)

    def hedge_submitted(self, requested_qty: float, ack_ms: float, roundtrip_ms: float, filled_qty: float) -> None:
        self.hedge_requested_qty += requested_qty
        self.hedge_filled_qty += filled_qty
        self.hedge_ack_latencies.append(ack_ms)
        self.hedge_roundtrips.append(roundtrip_ms)

    def snapshot(self) -> dict[str, Any]:
        maker_ack = _summary(self.maker_ack_latencies)
        maker_quote_age = _summary(self.maker_quote_ages)
        hedge_ack = _summary(self.hedge_ack_latencies)
        warnings: list[str] = []
        if maker_ack["p95_ms"] > 500:
            warnings.append("maker_ack_slow")
        if maker_quote_age["p95_ms"] > 500:
            warnings.append("maker_quote_stale")
        if hedge_ack["p95_ms"] > 500:
            warnings.append("hedge_ack_slow")
        if self.hedge_requested_qty and self.hedge_filled_qty < self.hedge_requested_qty:
            warnings.append("hedge_partial_or_unfilled")
        return {
            "status": "WARN" if warnings else "OK",
            "warnings": warnings,
            "bbo_events": self.bbo_events,
            "maker_reprices": self.maker_reprices,
            "maker_ack_latency": maker_ack,
            "maker_quote_age": maker_quote_age,
            "maker_time_to_fill": _summary(self.maker_waits),
            "hedge_ack_latency": hedge_ack,
            "hedge_roundtrip": _summary(self.hedge_roundtrips),
            "hedge_fill_rate_pct": round(
                self.hedge_filled_qty / self.hedge_requested_qty * 100
                if self.hedge_requested_qty else 0,
                4,
            ),
            "duration_ms": round((time.perf_counter() - self.started_at) * 1000, 3),
        }