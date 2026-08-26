from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any

import httpx


def _short_decimal(value: Any, places: int = 6) -> str:
    try:
        number = float(value)
        return f"{number:.{places}f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(value)


def _format_map(values: dict[str, Any], places: int = 6) -> str:
    if not values:
        return "-"
    return "；".join(f"{key} {_short_decimal(value, places)}" for key, value in values.items())


class LarkNotifier:
    def __init__(self, webhook_url: str, secret: str | None = None, timeout: float = 5.0):
        self.webhook_url = webhook_url
        self.secret = secret
        self.timeout = timeout

    def _signed_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.secret:
            timestamp = str(int(time.time()))
            string_to_sign = f"{timestamp}\n{self.secret}"
            digest = hmac.new(self.secret.encode(), string_to_sign.encode(), hashlib.sha256).digest()
            payload.update({"timestamp": timestamp, "sign": base64.b64encode(digest).decode()})
        return payload

    async def _post(self, payload: dict[str, Any]) -> None:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(self.webhook_url, json=self._signed_payload(payload))
            response.raise_for_status()
            body = response.json()
            if body.get("code", 0) != 0:
                raise RuntimeError(f"Lark webhook failed: {body}")

    async def send(self, text: str) -> None:
        await self._post({"msg_type": "text", "content": {"text": text}})

    async def send_report(self, reason: str, payload: dict[str, Any]) -> None:
        execution = payload.get("execution", {}) or {}
        legs = execution.get("legs", {}) or {}
        perp = legs.get("perp", {}) or {}
        spot = legs.get("spot", {}) or {}
        progress = execution.get("state_progress", {}) or {}

        def value(*values: Any, default: str = "0") -> Any:
            for item in values:
                if item is not None and item != "":
                    return item
            return default

        perp_qty = value(perp.get("filled_base_qty"), progress.get("filled_base_qty"), payload.get("filled_base_qty"))
        spot_qty = value(spot.get("filled_base_qty"), progress.get("hedged_base_qty"), payload.get("hedged_base_qty"))
        exposure = value(execution.get("unhedged_base_qty"), progress.get("exposure"), payload.get("exposure"))

        def avg_price(leg: dict[str, Any], qty: Any) -> str:
            try:
                if float(qty) == 0:
                    return "N/A"
            except (TypeError, ValueError):
                return "N/A"
            raw = leg.get("avg_price")
            return "N/A" if raw in (None, "", "0", "0.0") else _short_decimal(raw)

        market = execution.get("market_spread", {}) or {}
        has_market_sample = bool(market.get("observations", 0))
        market_quote = _short_decimal(market.get("quote_twap_rate_pct")) if has_market_sample else "N/A"
        market_exec = _short_decimal(market.get("executable_twap_rate_pct")) if has_market_sample else "N/A"
        relative = execution.get("execution_vs_market_executable_rate_pct")
        relative_display = _short_decimal(relative, 4) if has_market_sample and relative not in (None, "") else "N/A"
        actual_spread = execution.get("effective_spread_rate_pct")
        actual_display = _short_decimal(actual_spread, 4) if execution.get("fill_data_available") is True and actual_spread not in (None, "") else "N/A"
        efficiency = execution.get("efficiency", {}) or {}
        maker_ack = efficiency.get("maker_ack_latency", {}) or {}
        amend = efficiency.get("maker_amend_latency", {}) or {}
        maker_fill = efficiency.get("maker_time_to_fill", {}) or {}
        hedge_ack = efficiency.get("hedge_ack_latency", {}) or {}
        fees = f"合约 {_format_map(perp.get('fees', {}))}；现货 {_format_map(spot.get('fees', {}))}"
        context = payload.get("execution_state", {}) or {}
        common = [
            f"**任务**：{payload.get('request_id', '-')}\n",
            f"**状态**：{payload.get('parent_state', '-')}\n",
            f"**动作/方向**：{payload.get('action', '-')} / {payload.get('direction', '-')}\n",
            f"**成交数量**：合约 {perp_qty} / 现货 {spot_qty}\n",
            f"**成交均价**：合约 {avg_price(perp, perp_qty)} / 现货 {avg_price(spot, spot_qty)}\n",
            f"**成交价差率**：{actual_display}%\n",
            f"**市场报价 TWAP**：{market_quote}%\n",
            f"**市场可执行 TWAP**：{market_exec}%\n",
            f"**相对市场可执行价差**：{relative_display}%\n",
            f"**未对冲敞口**：{_short_decimal(exposure, 8)}\n",
        ]

        if reason == "PARENT_COMPLETED":
            template, icon, title = "green", "✅", "EXECUTION_COMPLETED"
            reconciliation = execution.get("account_reconciliation", {}) or {}
            common.extend([
                f"**已实现盈亏**：合约 {_short_decimal(perp.get('realized_pnl', '0'))} / 现货 {_short_decimal(spot.get('realized_pnl', '0'))}\n",
                f"**执行效率**：Maker ACK {_short_decimal(maker_ack.get('avg_ms', 0), 2)}ms；改单 ACK {_short_decimal(amend.get('avg_ms', 0), 2)}ms；改单 {efficiency.get('maker_reprices', 0)} 次；Maker 等待 {_short_decimal(maker_fill.get('avg_ms', 0), 2)}ms；对冲 ACK {_short_decimal(hedge_ack.get('avg_ms', 0), 2)}ms；IOC 成交率 {_short_decimal(efficiency.get('hedge_fill_rate_pct', 0), 2)}%\n",
                f"**手续费**：{fees}\n",
                f"**资产对账**：{reconciliation.get('status', 'UNAVAILABLE')}\n",
                f"**余额变化**：{_format_map(reconciliation.get('balance_delta', {}))}\n",
                f"**仓位变化**：{_format_map(reconciliation.get('position_delta_contracts', {}))}",
            ])
            if reconciliation.get("status") == "CHECK_REQUIRED":
                common.append(f"**差额**：余额 {reconciliation.get('balance_difference', {})}；仓位 {reconciliation.get('position_difference', {})}")
            if execution.get("fill_data_available") is False:
                common.append("**成交明细**：交易所成交明细暂未返回，价差/手续费待核对")
        elif reason == "EXECUTION_STATUS":
            template, icon, title = "blue", "⏳", "EXECUTION_STATUS"
            common.extend([
                f"**执行阶段**：{context.get('phase', payload.get('parent_state', '-'))}",
                f"**剩余目标**：{context.get('remaining_base_qty', '-')}",
                f"**当前 Maker**：{context.get('maker_order_id') or '无'} @ {context.get('maker_price') or 'N/A'}",
                f"**Maker 尝试**：{context.get('maker_attempts', '-')}",
                f"**手续费**：{fees}",
            ])
            if payload.get("error"):
                common.append(f"**最近异常**：{payload['error']}")
            if execution.get("report_error"):
                common.append(f"**状态数据**：{execution['report_error']}")
        elif reason == "EXECUTION_WARNING":
            template, icon, title = "yellow", "⚠️", "EXECUTION_WARNING"
            common.extend([
                f"**执行阶段**：{context.get('phase', payload.get('parent_state', '-'))}",
                f"**剩余目标**：{context.get('remaining_base_qty', '-')}",
                f"**当前 Maker**：{context.get('maker_order_id') or '无'} @ {context.get('maker_price') or 'N/A'}",
                f"**告警**：{payload.get('error', reason)}",
                f"**手续费**：{fees}",
            ])
        elif reason == "ORDER_STARTED":
            template, icon, title = "blue", "▶️", "EXECUTION_STARTED"
            params = payload.get("parameters", {})
            common = [
                f"**任务**：{payload.get('request_id', '-')}\n",
                f"**状态**：{payload.get('parent_state', '-')}\n",
                f"**动作/方向**：{payload.get('action', '-')} / {payload.get('direction', '-')}\n",
                f"**目标数量**：{params.get('target_base_qty', '-')}\n",
                f"**拆单粒度**：{params.get('child_base_qty', '-')}\n",
                f"**现货交易模式**：{params.get('spot_td_mode', 'cross')}\n",
                f"**最大敞口**：{params.get('max_unhedged_base_qty', '-')}\n",
                f"**改单间隔**：{params.get('maker_reprice_interval_ms', '-')}ms",
            ]
        elif reason in {"HEDGE_FAILED", "EXPOSURE_LIMIT", "HEDGE_RETRY_EXHAUSTED", "REPRICE_FAILED", "TARGET_INCOMPLETE", "MAKER_RETRY_EXHAUSTED", "EXECUTION_RISK"}:
            template, icon, title = "red", "🚨", "EXECUTION_RISK"
            common.extend([
                f"**原因**：{payload.get('error', reason)}\n",
                f"**手续费**：{fees}",
            ])
        elif reason in {"BASIS_STARTED", "BASIS_PAUSED", "BASIS_RESUMED"}:
            strategy = payload.get("strategy", {})
            template = "yellow" if reason == "BASIS_PAUSED" else "blue"
            title = reason
            icon = "⏸️" if reason == "BASIS_PAUSED" else "📈"
            common = [
                f"**任务**：{payload.get('request_id', '-')}\n",
                f"**策略状态**：{reason}\n",
                f"**原因**：{strategy.get('reason', '-') or '-'}\n",
                f"**方向/动作**：{payload.get('direction', '-')} / {payload.get('action', '-')}\n",
                f"**当前 Basis**：{_short_decimal(strategy.get('basis_bp', '0'), 2)} bp\n",
                f"**成交/对冲**：{payload.get('filled_base_qty', '0')} / {payload.get('hedged_base_qty', '0')}\n",
                f"**当前敞口**：{payload.get('exposure', '0')}",
            ]
        else:
            template, icon, title = "blue", "ℹ️", "EXECUTION_STATUS"

        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": template,
                "title": {"tag": "plain_text", "content": f"{icon} {title}"},
            },
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(common)}}],
        }
        await self._post({"msg_type": "interactive", "card": card})


class NullNotifier:
    async def send(self, text: str) -> None:
        return None

    async def send_report(self, reason: str, payload: dict[str, Any]) -> None:
        return None
