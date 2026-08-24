from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal

from dotenv import load_dotenv

from .models import Direction, OrderAction, ParentOrderRequest

load_dotenv()


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"missing environment variable: {name}")
    return value


@dataclass(frozen=True)
class AppConfig:
    api_key: str
    secret_key: str
    passphrase: str
    demo: bool
    spot_inst_id: str
    swap_inst_id: str
    direction: Direction
    action: OrderAction
    target_base_qty: Decimal
    child_base_qty: Decimal
    max_spot_slippage_bps: Decimal
    max_unhedged_base_qty: Decimal
    max_hedge_retries: int
    maker_reprice_interval_ms: int
    state_path: str
    lark_webhook_url: str | None = None
    lark_secret: str | None = None

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            api_key=env("OKX_API_KEY"),
            secret_key=env("OKX_SECRET_KEY"),
            passphrase=env("OKX_PASSPHRASE"),
            demo=os.getenv("OKX_DEMO", "1").lower() in {"1", "true", "yes"},
            spot_inst_id=os.getenv("SPOT_INST_ID", "BTC-USDT"),
            swap_inst_id=os.getenv("SWAP_INST_ID", "BTC-USDT-SWAP"),
            direction=Direction(os.getenv("ARB_DIRECTION", Direction.SHORT_SPOT_LONG_SWAP.value)),
            action=OrderAction(os.getenv("ARB_ACTION", OrderAction.OPEN.value)),
            target_base_qty=Decimal(env("TARGET_BASE_QTY")),
            child_base_qty=Decimal(env("CHILD_BASE_QTY")),
            max_spot_slippage_bps=Decimal(os.getenv("MAX_SPOT_SLIPPAGE_BPS", "10")),
            max_unhedged_base_qty=Decimal(os.getenv("MAX_UNHEDGED_BASE_QTY", "0.01")),
            max_hedge_retries=int(os.getenv("MAX_HEDGE_RETRIES", "3")),
            maker_reprice_interval_ms=int(os.getenv("MAKER_REPRICE_INTERVAL_MS", "150")),
            state_path=os.getenv("STATE_PATH", "runtime/state.json"),
            lark_webhook_url=os.getenv("LARK_WEBHOOK_URL"),
            lark_secret=os.getenv("LARK_SECRET"),
        )

    def request(self, request_id: str) -> ParentOrderRequest:
        return ParentOrderRequest(
            request_id=request_id,
            direction=self.direction,
            action=self.action,
            spot_inst_id=self.spot_inst_id,
            swap_inst_id=self.swap_inst_id,
            target_base_qty=self.target_base_qty,
            child_base_qty=self.child_base_qty,
            max_spot_slippage_bps=self.max_spot_slippage_bps,
            max_unhedged_base_qty=self.max_unhedged_base_qty,
            max_hedge_retries=self.max_hedge_retries,
            maker_reprice_interval_ms=self.maker_reprice_interval_ms,
            lark_report=bool(self.lark_webhook_url),
        )
