from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal

from dotenv import load_dotenv

from .basis_strategy import BasisStrategyConfig
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
    strategy_mode: str
    spot_td_mode: str
    direction: Direction
    action: OrderAction
    target_base_qty: Decimal
    child_base_qty: Decimal
    max_spot_slippage_bps: Decimal
    max_unhedged_base_qty: Decimal
    max_hedge_retries: int
    max_maker_attempts: int
    maker_reprice_interval_ms: int
    status_report_interval_seconds: int
    basis_entry_threshold_bp: Decimal
    basis_pause_threshold_bp: Decimal
    basis_resume_threshold_bp: Decimal
    basis_exit_threshold_bp: Decimal
    basis_resume_exposure_base_qty: Decimal
    basis_signal_interval_ms: int
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
            strategy_mode=os.getenv("STRATEGY_MODE", "pair"),
            spot_td_mode=os.getenv("SPOT_TD_MODE", "cross"),
            direction=Direction(os.getenv("ARB_DIRECTION", Direction.SHORT_SPOT_LONG_SWAP.value)),
            action=OrderAction(os.getenv("ARB_ACTION", OrderAction.OPEN.value)),
            target_base_qty=Decimal(env("TARGET_BASE_QTY")),
            child_base_qty=Decimal(env("CHILD_BASE_QTY")),
            max_spot_slippage_bps=Decimal(os.getenv("MAX_SPOT_SLIPPAGE_BPS", "10")),
            max_unhedged_base_qty=Decimal(os.getenv("MAX_UNHEDGED_BASE_QTY", "0.01")),
            max_hedge_retries=int(os.getenv("MAX_HEDGE_RETRIES", "3")),
            max_maker_attempts=int(os.getenv("MAX_MAKER_ATTEMPTS", "50")),
            maker_reprice_interval_ms=int(os.getenv("MAKER_REPRICE_INTERVAL_MS", "150")),
            status_report_interval_seconds=int(os.getenv("STATUS_REPORT_INTERVAL_SECONDS", "30")),
            basis_entry_threshold_bp=Decimal(os.getenv("BASIS_ENTRY_THRESHOLD_BP", "10")),
            basis_pause_threshold_bp=Decimal(os.getenv("BASIS_PAUSE_THRESHOLD_BP", "5")),
            basis_resume_threshold_bp=Decimal(os.getenv("BASIS_RESUME_THRESHOLD_BP", "8")),
            basis_exit_threshold_bp=Decimal(os.getenv("BASIS_EXIT_THRESHOLD_BP", "0")),
            basis_resume_exposure_base_qty=Decimal(os.getenv("BASIS_RESUME_EXPOSURE_BASE_QTY", "0.005")),
            basis_signal_interval_ms=int(os.getenv("BASIS_SIGNAL_INTERVAL_MS", "50")),
            state_path=os.getenv("STATE_PATH", "runtime/state.json"),
            lark_webhook_url=os.getenv("LARK_WEBHOOK_URL"),
            lark_secret=os.getenv("LARK_SECRET"),
        )

    def basis_config(self) -> BasisStrategyConfig:
        return BasisStrategyConfig(
            entry_threshold_bp=self.basis_entry_threshold_bp,
            pause_threshold_bp=self.basis_pause_threshold_bp,
            resume_threshold_bp=self.basis_resume_threshold_bp,
            exit_threshold_bp=self.basis_exit_threshold_bp,
            resume_exposure_base_qty=self.basis_resume_exposure_base_qty,
            signal_interval_ms=self.basis_signal_interval_ms,
        )

    def request(self, request_id: str) -> ParentOrderRequest:
        return ParentOrderRequest(
            request_id=request_id,
            direction=self.direction,
            action=self.action,
            spot_td_mode=self.spot_td_mode,
            spot_inst_id=self.spot_inst_id,
            swap_inst_id=self.swap_inst_id,
            target_base_qty=self.target_base_qty,
            child_base_qty=self.child_base_qty,
            max_spot_slippage_bps=self.max_spot_slippage_bps,
            max_unhedged_base_qty=self.max_unhedged_base_qty,
            max_hedge_retries=self.max_hedge_retries,
            max_maker_attempts=self.max_maker_attempts,
            maker_reprice_interval_ms=self.maker_reprice_interval_ms,
            lark_report=bool(self.lark_webhook_url),
        )
