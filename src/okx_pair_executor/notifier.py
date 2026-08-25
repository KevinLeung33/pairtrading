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
        child = payload.get("child", {})
        execution = payload.get("execution", {})
        if reason == "PARENT_COMPLETED":
            template, icon, title = "green", "✅", "EXECUTION_COMPLETED"
            legs = execution.get("legs", {})
            perp = legs.get("perp", {})
            spot = legs.get("spot", {})
            reconciliation = execution.get("account_reconciliation", {})
            fields = [
                f"**任务**：{payload.get('request_id', '-')}",
                f"**方向**：{payload.get('direction', '-')}",
                f"**成交数量**：合约 {perp.get('filled_base_qty', '0')} / 现货 {spot.get('filled_base_qty', '0')}",
                f"**成交均价**：合约 {_short_decimal(perp.get('avg_price', '0'))} / 现货 {_short_decimal(spot.get('avg_price', '0'))}",
                f"**成交价差率**：{_short_decimal(execution.get('effective_spread_rate_pct', execution.get('spread_rate_pct', '0')), 4)}%",
                f"**市场报价 TWAP**：{_short_decimal(execution.get('market_spread', {}).get('quote_twap_rate_pct', '0'), 4)}%",
                f"**市场可执行 TWAP**：{_short_decimal(execution.get('market_spread', {}).get('executable_twap_rate_pct', '0'), 4)}%",
                f"**相对市场可执行价差**：{_short_decimal(execution.get('execution_vs_market_executable_rate_pct', '0'), 4)}%",
                f"**执行效率**：Maker ACK {_short_decimal(execution.get('efficiency', {}).get('maker_ack_latency', {}).get('avg_ms', 0), 2)}ms；改单 {execution.get('efficiency', {}).get('maker_reprices', 0)} 次；Maker 等待 {_short_decimal(execution.get('efficiency', {}).get('maker_time_to_fill', {}).get('avg_ms', 0), 2)}ms；对冲 ACK {_short_decimal(execution.get('efficiency', {}).get('hedge_ack_latency', {}).get('avg_ms', 0), 2)}ms；IOC 成交率 {_short_decimal(execution.get('efficiency', {}).get('hedge_fill_rate_pct', 0), 2)}%",
                f"**手续费**：合约 {_format_map(perp.get('fees', {}))}；现货 {_format_map(spot.get('fees', {}))}",
                f"**未对冲敞口**：{_short_decimal(execution.get('unhedged_base_qty', payload.get('exposure', '0')), 8)}",
                f"**资产对账**：{reconciliation.get('status', 'UNAVAILABLE')}",
                f"**余额变化**：{_format_map(reconciliation.get('balance_delta', {}))}",
                f"**仓位变化**：{_format_map(reconciliation.get('position_delta_contracts', {}))}",
            ]
            if reconciliation.get("status") == "CHECK_REQUIRED":
                fields.append(f"**差额**：余额 {reconciliation.get('balance_difference', {})}；仓位 {reconciliation.get('position_difference', {})}")
        elif reason == "CHILD_TERMINAL":
            template, icon, title = "green", "✅", "CHILD_COMPLETED"
            legs = execution.get("legs", {})
            perp = legs.get("perp", {})
            spot = legs.get("spot", {})
            fields = [
                f"**任务**：{payload.get('request_id', '-')}",
                f"**子单**：{child.get('child_id', '-')}",
                f"**成交数量**：合约 {perp.get('filled_base_qty', child.get('perp_filled_base_qty', '0'))} / 现货 {spot.get('filled_base_qty', child.get('spot_filled_base_qty', '0'))}",
                f"**成交均价**：合约 {_short_decimal(perp.get('avg_price', '0'))} / 现货 {_short_decimal(spot.get('avg_price', '0'))}",
                f"**成交价差率**：{_short_decimal(execution.get('effective_spread_rate_pct', execution.get('spread_rate_pct', '0')), 4)}%",
                f"**市场报价 TWAP**：{_short_decimal(execution.get('market_spread', {}).get('quote_twap_rate_pct', '0'), 4)}%",
                f"**市场可执行 TWAP**：{_short_decimal(execution.get('market_spread', {}).get('executable_twap_rate_pct', '0'), 4)}%",
                f"**相对市场可执行价差**：{_short_decimal(execution.get('execution_vs_market_executable_rate_pct', '0'), 4)}%",

                f"**手续费**：合约 {_format_map(perp.get('fees', {}))}；现货 {_format_map(spot.get('fees', {}))}",
                f"**未对冲敞口**：{_short_decimal(execution.get('unhedged_base_qty', child.get('exposure', '0')), 8)}",
                f"**对冲次数**：{child.get('hedge_attempts', 0)}",
            ]
        elif reason in {"ORDER_STARTED", "CHILD_STARTED"}:
            template, icon, title = "blue", "▶️", reason
            params = payload.get("parameters", {})
            fields = [
                f"**任务**：{payload.get('request_id', '-')}",
                f"**动作/方向**：{payload.get('action', '-')} / {payload.get('direction', '-')}",
                f"**目标数量**：{params.get('target_base_qty', '-')}",
                f"**单批数量**：{params.get('child_base_qty', '-')}",
                f"**当前子单**：{child.get('child_id', '-')}，目标 {child.get('target_base_qty', '-')}",
                f"**合约张数**：{child.get('perp_target_contracts', '-')}",
                f"**Maker 价格**：{child.get('maker_price', '-')}",
                f"**最大敞口**：{params.get('max_unhedged_base_qty', '-')}",
                f"**改单间隔**：{params.get('maker_reprice_interval_ms', '-')}ms",
            ]
        elif reason in {"HEDGE_FAILED", "EXPOSURE_LIMIT", "HEDGE_RETRY_EXHAUSTED", "REPRICE_FAILED"}:
            template, icon, title = "red", "🚨", "RISK_ALERT"
            fields = [
                f"**任务**：{payload.get('request_id', '-')}",
                f"**子单**：{child.get('child_id', '-')}",
                f"**状态**：{child.get('state', payload.get('parent_state', '-'))}",
                f"**当前敞口**：{payload.get('exposure', '0')}",
                f"**对冲次数**：{child.get('hedge_attempts', 0)}",
                f"**原因**：{payload.get('error', reason)}",
            ]
        else:
            template, icon, title = "blue", "ℹ️", "CHILD_PROGRESS"
            fields = [
                f"**任务**：{payload.get('request_id', '-')}",
                f"**子单**：{child.get('child_id', '-')}",
                f"**状态**：{child.get('state', payload.get('parent_state', '-'))}",
                f"**进度**：合约 {child.get('perp_filled_base_qty', '0')} / 现货 {child.get('spot_filled_base_qty', '0')}",
                f"**当前敞口**：{payload.get('exposure', '0')}",
                f"**对冲次数**：{child.get('hedge_attempts', 0)}",
            ]
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": template,
                "title": {"tag": "plain_text", "content": f"{icon} {title}"},
            },
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(fields)}}],
        }
        await self._post({"msg_type": "interactive", "card": card})



class NullNotifier:
    async def send(self, text: str) -> None:
        return None

    async def send_report(self, reason: str, payload: dict[str, Any]) -> None:
        return None