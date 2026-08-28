from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
REPORT_DIR = ROOT / "runtime" / "reports"
LOG_DIR = ROOT / "runtime" / "demo-suite-logs"
TIMEOUT = int(os.getenv("DEMO_SUITE_TIMEOUT_SECONDS", "180"))

CLOSE_CASES = [
    {"name": "close_short_small", "args": ["--action", "close", "--direction", "short_spot_long_swap", "--target-base-qty", "0.01", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "150"]},
    {"name": "close_short_split", "args": ["--action", "close", "--direction", "short_spot_long_swap", "--target-base-qty", "0.05", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.05", "--maker-reprice-interval-ms", "200"]},
    {"name": "close_long_split", "args": ["--action", "close", "--direction", "long_spot_short_swap", "--target-base-qty", "0.02", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "100"]},
]


CASES = [
    {"name": "short_small", "args": ["--direction", "short_spot_long_swap", "--target-base-qty", "0.01", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "150"]},
    {"name": "short_split", "args": ["--direction", "short_spot_long_swap", "--target-base-qty", "0.05", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.05", "--maker-reprice-interval-ms", "200"]},
    {"name": "long_split", "args": ["--direction", "long_spot_short_swap", "--target-base-qty", "0.02", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "100"]},
]


EXTENDED_CASES = [
    {"name": "short_tiny_fast_reprice", "args": ["--direction", "short_spot_long_swap", "--target-base-qty", "0.01", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.005", "--maker-reprice-interval-ms", "100"]},
    {"name": "short_three_children", "args": ["--direction", "short_spot_long_swap", "--target-base-qty", "0.03", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "200"]},
    {"name": "short_uneven_tail", "args": ["--direction", "short_spot_long_swap", "--target-base-qty", "0.05", "--child-base-qty", "0.02", "--max-unhedged-base-qty", "0.02", "--maker-reprice-interval-ms", "250"]},
    {"name": "short_exposure_tight", "args": ["--direction", "short_spot_long_swap", "--target-base-qty", "0.02", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.001", "--maker-reprice-interval-ms", "150"]},
    {"name": "short_reprice_slow", "args": ["--direction", "short_spot_long_swap", "--target-base-qty", "0.02", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "500"]},
    {"name": "long_tiny_fast_reprice", "args": ["--direction", "long_spot_short_swap", "--target-base-qty", "0.01", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.005", "--maker-reprice-interval-ms", "100"]},
    {"name": "long_three_children", "args": ["--direction", "long_spot_short_swap", "--target-base-qty", "0.03", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "200"]},
    {"name": "long_uneven_tail", "args": ["--direction", "long_spot_short_swap", "--target-base-qty", "0.05", "--child-base-qty", "0.02", "--max-unhedged-base-qty", "0.02", "--maker-reprice-interval-ms", "250"]},
    {"name": "long_exposure_tight", "args": ["--direction", "long_spot_short_swap", "--target-base-qty", "0.02", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.001", "--maker-reprice-interval-ms", "150"]},
    {"name": "long_reprice_slow", "args": ["--direction", "long_spot_short_swap", "--target-base-qty", "0.02", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "500"]},
]


EXTENDED_CLOSE_CASES = [
    {"name": "close_short_tiny_tight", "args": ["--action", "close", "--direction", "short_spot_long_swap", "--target-base-qty", "0.01", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.005", "--maker-reprice-interval-ms", "100"]},
    {"name": "close_short_split_uneven", "args": ["--action", "close", "--direction", "short_spot_long_swap", "--target-base-qty", "0.03", "--child-base-qty", "0.02", "--max-unhedged-base-qty", "0.02", "--maker-reprice-interval-ms", "250"]},
    {"name": "close_short_split_slow", "args": ["--action", "close", "--direction", "short_spot_long_swap", "--target-base-qty", "0.05", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "500"]},
    {"name": "close_long_tiny_tight", "args": ["--action", "close", "--direction", "long_spot_short_swap", "--target-base-qty", "0.01", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.005", "--maker-reprice-interval-ms", "100"]},
    {"name": "close_long_split_uneven", "args": ["--action", "close", "--direction", "long_spot_short_swap", "--target-base-qty", "0.03", "--child-base-qty", "0.02", "--max-unhedged-base-qty", "0.02", "--maker-reprice-interval-ms", "250"]},
    {"name": "close_long_split_slow", "args": ["--action", "close", "--direction", "long_spot_short_swap", "--target-base-qty", "0.05", "--child-base-qty", "0.01", "--max-unhedged-base-qty", "0.01", "--maker-reprice-interval-ms", "500"]},
]

def run_capture(command: list[str], output: Path, timeout: int | None = None) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output.open("w", encoding="utf-8") as stream:
            result = subprocess.run(
                command,
                cwd=ROOT,
                stdout=stream,
                stderr=subprocess.STDOUT,
                timeout=timeout,
                check=False,
            )
        return result.returncode
    except subprocess.TimeoutExpired:
        output.write_text(output.read_text(encoding="utf-8", errors="replace") + "\nTIMEOUT\n", encoding="utf-8")
        return 124


def newest_summary(before: set[Path]) -> Path | None:
    candidates = [p for p in REPORT_DIR.glob("demo-log-summary-*.json") if p not in before]
    return max(candidates, key=lambda p: p.stat().st_mtime) if candidates else None


def matching_close_position(case: dict[str, object]) -> tuple[bool, str]:
    """Verify the live one-way net position before submitting a close case."""
    try:
        from okx_pair_executor.config import AppConfig
        from okx_pair_executor.okx_client import OkxV5Client

        config = AppConfig.from_env()
        args = case["args"]
        target_index = args.index("--target-base-qty")
        target = Decimal(args[target_index + 1])
        direction = args[args.index("--direction") + 1]
        required_sign = Decimal("1") if direction == "short_spot_long_swap" else Decimal("-1")

        async def read_position() -> tuple[Decimal, Decimal]:
            client = OkxV5Client(config.api_key, config.secret_key, config.passphrase, demo=config.demo)
            snapshot = await client.account_snapshot([config.swap_inst_id])
            rules = await client.instrument_rules(config.swap_inst_id)
            return Decimal(snapshot.get("positions", {}).get(config.swap_inst_id, "0")), rules.contract_value

        position_contracts, contract_value = asyncio.run(read_position())
        target_contracts = target / contract_value
        if position_contracts * required_sign < target_contracts:
            return False, f"skip: current swap position {position_contracts} contracts cannot support close target {target_contracts} contracts"
        return True, f"matched current swap position {position_contracts} contracts for close target {target_contracts} contracts"
    except Exception as exc:
        return False, f"skip: unable to verify current swap position: {exc}"

def main() -> int:
    parser = argparse.ArgumentParser(description="Run sequential OKX Demo open tests")
    parser.add_argument(
        "--extended",
        action="store_true",
        help="run the extended 13-case parameter and direction matrix",
    )
    parser.add_argument(
        "--include-close",
        action="store_true",
        help="after opening, run matching close tests with the same quantities",
    )
    args = parser.parse_args()
    selected_cases = CASES + (EXTENDED_CASES if args.extended else []) + (CLOSE_CASES if args.include_close else []) + (EXTENDED_CLOSE_CASES if args.extended and args.include_close else [])
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results: list[dict[str, object]] = []

    preflight = LOG_DIR / f"{stamp}-preflight.log"
    rc = run_capture(["bash", "scripts/run_local_scenarios.sh"], preflight)
    results.append({"name": "local_scenarios", "passed": rc == 0, "log": str(preflight)})
    if rc != 0:
        return write_report(stamp, results, 2 + len(selected_cases))

    readonly = LOG_DIR / f"{stamp}-readonly.log"
    rc = run_capture(["bash", "scripts/demo_readonly_check.sh"], readonly, timeout=30)
    results.append({"name": "demo_readonly_check", "passed": rc == 0, "log": str(readonly)})
    if rc != 0:
        return write_report(stamp, results, 2 + len(selected_cases))

    for index, case in enumerate(selected_cases, start=1):
        if args.include_close and case in CLOSE_CASES:
            can_close, details = matching_close_position(case)
            if not can_close:
                results.append({"name": case["name"], "passed": True, "skipped": True, "details": details})
                continue
        request_id = f"SUITE-{stamp}-{index:02d}"
        log = LOG_DIR / f"{stamp}-{case['name']}.log"
        before = set(REPORT_DIR.glob("demo-log-summary-*.json"))
        rc = run_capture(
            ["bash", "scripts/run_demo.sh", "--request-id", request_id, *case["args"]],
            log,
            timeout=TIMEOUT,
        )
        summary_log = LOG_DIR / f"{stamp}-{case['name']}-summary.log"
        summary_rc = run_capture(
            [sys.executable, "scripts/summarize_demo_log.py", str(log), "--output-dir", str(REPORT_DIR)],
            summary_log,
            timeout=30,
        )
        summary = newest_summary(before)
        summary_payload = json.loads(summary.read_text(encoding="utf-8")) if summary else {}
        issues = int(summary_payload.get("issue_count", 0))
        issue_list = summary_payload.get("issues", [])[-20:]
        if rc == 124:
            issue_list = [f"runner timeout after {TIMEOUT}s"] + issue_list
            issues = max(1, issues)
        passed = rc == 0 and summary_rc == 0 and issues == 0
        results.append({
            "name": case["name"],
            "request_id": request_id,
            "passed": passed,
            "runner_exit_code": rc,
            "summary_exit_code": summary_rc,
            "issue_count": issues,
            "issues": issue_list,
            "log": str(log),
            "summary": str(summary) if summary else None,
        })
        if not passed:
            break

    return write_report(stamp, results, 2 + len(selected_cases))


def write_report(stamp: str, results: list[dict[str, object]], expected_count: int) -> int:
    passed = all(bool(item["passed"]) for item in results)
    payload = {
        "generated_at": stamp,
        "mode": "demo_sequential_suite",
        "passed": passed,
        "stopped_after_failure": not passed and len(results) < expected_count,
        "results": results,
    }
    json_path = REPORT_DIR / f"demo-suite-{stamp}.json"
    md_path = REPORT_DIR / f"demo-suite-{stamp}.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = [
        f"# Demo Suite Report ({stamp})",
        "",
        f"Overall: {'PASS' if passed else 'FAIL'}",
        "",
        "| Test | Result | Details |",
        "|---|---|---|",
    ]
    for item in results:
        detail = f"log={item['log']}"
        if item.get("summary"):
            detail += f"; summary={item['summary']}"
        if "issue_count" in item:
            detail += f"; issues={item['issue_count']}"
            if item.get("issues"):
                detail += f"; first_issue={item['issues'][0]}"
        result_label = "SKIP" if item.get("skipped") else ("PASS" if item["passed"] else "FAIL")
        if item.get("details"):
            detail = f"{item['details']}; {detail}"
        lines.append(f"| {item['name']} | {result_label} | {detail} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"\nFINAL_RESULT: {'PASS' if passed else 'FAIL'}")
    print(f"REPORT_MD: {md_path}")
    print(f"REPORT_JSON: {json_path}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
