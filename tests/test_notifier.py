import pytest

from okx_pair_executor.notifier import LarkNotifier


@pytest.mark.asyncio
async def test_status_card_is_task_level_and_marks_missing_market_samples():
    notifier = LarkNotifier("https://example.invalid/hook")
    sent = []

    async def capture(payload):
        sent.append(payload)

    notifier._post = capture
    await notifier.send_report("EXECUTION_STATUS", {
        "request_id": "P-STATUS",
        "parent_state": "running",
        "direction": "long_spot_short_swap",
        "action": "open",
        "filled_base_qty": "0.1",
        "hedged_base_qty": "0.1",
        "exposure": "0",
        "execution": {
            "state_progress": {
                "filled_base_qty": "0.1",
                "hedged_base_qty": "0.1",
                "exposure": "0",
            },
            "legs": {
                "perp": {"filled_base_qty": "0.1", "avg_price": "79000"},
                "spot": {"filled_base_qty": "0.1", "avg_price": "79010"},
            },
            "market_spread": {"observations": 0},
        },
    })

    assert len(sent) == 1
    card = sent[0]["card"]
    assert card["header"]["title"]["content"] == "⏳ EXECUTION_STATUS"
    content = card["elements"][0]["text"]["content"]
    assert "现货 0.1" in content
    assert "**市场报价 TWAP**：N/A%" in content
    assert "CHILD_" not in content


@pytest.mark.asyncio
async def test_started_card_uses_execution_started_title():
    notifier = LarkNotifier("https://example.invalid/hook")
    sent = []

    async def capture(payload):
        sent.append(payload)

    notifier._post = capture
    await notifier.send_report("ORDER_STARTED", {
        "request_id": "P-START",
        "parent_state": "running",
        "direction": "short_spot_long_swap",
        "action": "open",
        "parameters": {
            "target_base_qty": "1",
            "child_base_qty": "0.1",
            "spot_td_mode": "cross",
            "max_unhedged_base_qty": "0.01",
            "maker_reprice_interval_ms": 150,
        },
    })

    assert sent[0]["card"]["header"]["title"]["content"] == "▶️ EXECUTION_STARTED"
@pytest.mark.asyncio
async def test_warning_card_contains_recent_error_and_execution_context():
    notifier = LarkNotifier("https://example.invalid/hook")
    sent = []

    async def capture(payload):
        sent.append(payload)

    notifier._post = capture
    await notifier.send_report("EXECUTION_WARNING", {
        "request_id": "P-WARN",
        "parent_state": "running",
        "direction": "long_spot_short_swap",
        "action": "open",
        "error": "max leverage reached",
        "execution_state": {
            "phase": "maker_working",
            "remaining_base_qty": "0.3",
            "maker_order_id": "123",
            "maker_price": "79000",
            "maker_attempts": 2,
        },
    })

    content = sent[0]["card"]["elements"][0]["text"]["content"]
    assert sent[0]["card"]["header"]["title"]["content"] == "⚠️ EXECUTION_WARNING"
    assert "max leverage reached" in content
    assert "剩余目标" in content
    assert "当前 Maker" in content
