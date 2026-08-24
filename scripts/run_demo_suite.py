from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "runtime" / "reports"
LOG_DIR = ROOT / "runtime" / "demo-suite-logs"
TIMEOUT = int(os.getenv("DEMO_SUITE_TIMEOUT_SECONDS", "180"))

CASES = [
    {
        "name": "short_small",
        "args": [
            "--direction", "short_spot_long_swap",
            "--target-base-qty", "0.01",
            "--child-base-qty", "0.01",
            "--max-unhedged-base-qty", "0.01",
            "--maker-reprice-interval-ms", "150",
        ],
    },
    {
        "name": "short_split",
        "args": [
            "--direction", "short_spot_long_swap",
            "--target-base-qty", "0.05",
            "--child-base-qty", "0.01",
            "--max-unhedged-base-qty", "0.05",
            "--maker-reprice-interval-ms", "200",
        ],
    },
    {
        "name": "long_split",
        "args": [
            "--direction", "long_spot_short_swap",
            "--target-base-qty", "0.02",
            "--child-base-qty", "0.01",
            "--max-unhedged-base-qty", "0.01",
            "--maker-reprice-interval-ms", "100",
        ],
    },
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


def main() -> int:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results: list[dict[str, object]] = []

    preflight = LOG_DIR / f"{stamp}-preflight.log"
    rc = run_capture(["bash", "scripts/run_local_scenarios.sh"], preflight)
    results.append({"name": "local_scenarios", "passed": rc == 0, "log": str(preflight)})
    if rc != 0:
        return write_report(stamp, results)

    readonly = LOG_DIR / f"{stamp}-readonly.log"
    rc = run_capture(["bash", "scripts/demo_readonly_check.sh"], readonly, timeout=30)
    results.append({"name": "demo_readonly_check", "passed": rc == 0, "log": str(readonly)})
    if rc != 0:
        return write_report(stamp, results)

    for index, case in enumerate(CASES, start=1):
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
        passed = rc == 0 and summary_rc == 0 and issues == 0
        results.append({
            "name": case["name"],
            "request_id": request_id,
            "passed": passed,
            "runner_exit_code": rc,
            "summary_exit_code": summary_rc,
            "issue_count": issues,
            "log": str(log),
            "summary": str(summary) if summary else None,
        })
        if not passed:
            break

    return write_report(stamp, results)


def write_report(stamp: str, results: list[dict[str, object]]) -> int:
    passed = all(bool(item["passed"]) for item in results)
    payload = {
        "generated_at": stamp,
        "mode": "demo_sequential_suite",
        "passed": passed,
        "stopped_after_failure": not passed and len(results) < len(CASES) + 2,
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
        lines.append(f"| {item['name']} | {'PASS' if item['passed'] else 'FAIL'} | {detail} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print(f"\nFINAL_RESULT: {'PASS' if passed else 'FAIL'}")
    print(f"REPORT_MD: {md_path}")
    print(f"REPORT_JSON: {json_path}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
