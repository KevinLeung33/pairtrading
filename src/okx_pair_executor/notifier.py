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

    def _signed_payload(self, text: str) -> dict[str, Any]:
        payload: dict[str, Any] = {"msg_type": "text", "content": {"text": text}}
        if self.secret:
            timestamp = str(int(time.time()))
            string_to_sign = f"{timestamp}\n{self.secret}"
            digest = hmac.new(self.secret.encode(), string_to_sign.encode(), hashlib.sha256).digest()
            payload.update({"timestamp": timestamp, "sign": base64.b64encode(digest).decode()})
        return payload

    async def send(self, text: str) -> None:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(self.webhook_url, json=self._signed_payload(text))
            response.raise_for_status()
            body = response.json()
            if body.get("code", 0) != 0:
                raise RuntimeError(f"Lark webhook failed: {body}")


class NullNotifier:
    async def send(self, text: str) -> None:
        return None
