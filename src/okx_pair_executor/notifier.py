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
        status = child.get("state", payload.get("parent_state", "unknown"))
        if reason in {"HEDGE_FAILED", "EXPOSURE_LIMIT", "HEDGE_RETRY_EXHAUSTED"}:
            template, icon = "red", "🚨"
        elif reason in {"PARENT_COMPLETED", "CHILD_TERMINAL"}:
            template, icon = "green", "✅"
        else:
            template, icon = "orange", "⚠️"
        fields = [
            f"**任务**：`{payload.get('request_id', '-')}`",
            f"**方向**：`{payload.get('direction', '-')}`",
            f"**状态**：`{status}`",
            f"**合约成交**：`{payload.get('filled_base_qty', '0')}",
            f"**现货对冲**：`{payload.get('hedged_base_qty', '0')}",
            f"**当前敞口**：`{payload.get('exposure', '0')}",
        ]
        if child:
            fields.extend([
                f"**子单**：`{child.get('child_id', '-')}`",
                f"**子单目标**：`{child.get('target_base_qty', '0')}`",
                f"**子单合约成交**：`{child.get('perp_filled_base_qty', '0')}`",
                f"**子单现货成交**：`{child.get('spot_filled_base_qty', '0')}`",
                f"**对冲次数**：`{child.get('hedge_attempts', 0)}",
            ])
        if payload.get("error"):
            fields.append(f"**错误**：`{payload['error']}`")
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": template,
                "title": {"tag": "plain_text", "content": f"{icon} {reason}"},
            },
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(fields)}}],
        }
        await self._post({"msg_type": "interactive", "card": card})


class NullNotifier:
    async def send(self, text: str) -> None:
        return None

    async def send_report(self, reason: str, payload: dict[str, Any]) -> None:
        return None