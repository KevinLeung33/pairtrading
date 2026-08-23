from __future__ import annotations

import json
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from .models import ChildOrder, ChildState, Direction, ParentOrder, ParentOrderRequest, ParentOrderState


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


class JsonStateStore:
    """Small atomic JSON store for recovery metadata, not a trade ledger."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def save(self, parents: dict[str, ParentOrder]) -> None:
        payload = _json_value({"parents": [asdict(parent) for parent in parents.values()]})
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def load(self) -> dict[str, ParentOrder]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        result: dict[str, ParentOrder] = {}
        for item in raw.get("parents", []):
            request_raw = item["request"]
            request = ParentOrderRequest(
                request_id=request_raw["request_id"],
                direction=Direction(request_raw["direction"]),
                spot_inst_id=request_raw["spot_inst_id"],
                swap_inst_id=request_raw["swap_inst_id"],
                target_base_qty=Decimal(request_raw["target_base_qty"]),
                child_base_qty=Decimal(request_raw["child_base_qty"]),
                max_spot_slippage_bps=Decimal(request_raw["max_spot_slippage_bps"]),
                max_unhedged_base_qty=Decimal(request_raw["max_unhedged_base_qty"]),
                hedge_tolerance_base_qty=Decimal(request_raw["hedge_tolerance_base_qty"]),
                max_hedge_retries=int(request_raw["max_hedge_retries"]),
                maker_reprice_interval_ms=int(request_raw.get("maker_reprice_interval_ms", 150)),
                lark_report=bool(request_raw["lark_report"]),
            )
            children = []
            for child_raw in item.get("children", []):
                child = ChildOrder(
                    child_id=child_raw["child_id"],
                    target_base_qty=Decimal(child_raw["target_base_qty"]),
                    perp_target_contracts=Decimal(child_raw["perp_target_contracts"]),
                    contract_value=Decimal(child_raw.get("contract_value", "1")),
                    state=ChildState(child_raw["state"]),
                    perp_order_id=child_raw.get("perp_order_id"),
                    perp_cl_ord_id=child_raw.get("perp_cl_ord_id"),
                    perp_filled_contracts=Decimal(child_raw.get("perp_filled_contracts", "0")),
                    active_order_filled_contracts=Decimal(child_raw.get("active_order_filled_contracts", "0")),
                    spot_target_base_qty=Decimal(child_raw.get("spot_target_base_qty", "0")),
                    spot_filled_base_qty=Decimal(child_raw.get("spot_filled_base_qty", "0")),
                    pending_hedge_base_qty=Decimal(child_raw.get("pending_hedge_base_qty", "0")),
                    hedge_attempts=int(child_raw.get("hedge_attempts", 0)),
                    spot_order_ids=child_raw.get("spot_order_ids", []),
                    last_perp_fill_px=Decimal(child_raw.get("last_perp_fill_px", "0")),
                    last_spot_fill_px=Decimal(child_raw.get("last_spot_fill_px", "0")),
                    maker_price=Decimal(child_raw.get("maker_price", "0")),
                )
                children.append(child)
            parent = ParentOrder(
                request=request,
                state=ParentOrderState(item["state"]),
                children=children,
                error=item.get("error"),
            )
            result[request.request_id] = parent
        return result
