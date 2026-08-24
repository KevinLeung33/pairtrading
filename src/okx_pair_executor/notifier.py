from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any

import httpx


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
                f"**成交均价**：合约 {perp.get('avg_price', '0')} / 现货 {spot.get('avg_price', '0')}",
                f"**价差率**：{execution.get('spread_rate_pct', '0')}%",
                f"**手续费**：合约 {perp.get('fees', {})}；现货 {spot.get('fees', {})}",
                f"**未对冲敞口**：{execution.get('unhedged_base_qty', payload.get('exposure', '0'))}",
                f"**资产对账**：{reconciliation.get('status', 'UNAVAILABLE')}",
            ]
            if reconciliation.get("status") == "CHECK_REQUIRED":
                fields.append(f"**差额**：余额 {reconciliation.get('balance_difference', {})}；仓位 {reconciliation.get('position_difference', {})}")
        elif reason in {"HEDGE_FAILED", "EXPOSURE_LIMIT", "HEDGE_RETRY_EXHAUSTED"}:
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