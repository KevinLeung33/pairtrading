from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


HTTP_RE = re.compile(r'HTTP Request: (?P<method>[A-Z]+) https?://[^/]+(?P<path>/api/v5/[^ ]+) "(?P<status>\d{3})')


def summarize_log(path: Path) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    errors: list[str] = []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines() if path.exists() else []
    for line in lines:
        match = HTTP_RE.search(line)
        if match:
            endpoint = match.group("path").split("?", 1)[0]
            counts[f"{match.group('method')} {endpoint}"] += 1
            statuses[match.group("status")] += 1
        if "ERROR" in line or "Traceback" in line or "TIMEOUT" in line:
            errors.append(line.strip())
    return {
        "exists": path.exists(),
        "line_count": len(lines),
        "http_calls": sum(counts.values()),
        "http_by_endpoint": dict(counts),
        "http_statuses": dict(statuses),
        "errors": errors[-20:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export execution efficiency data for one Demo suite")
    parser.add_argument("stamp", help="suite timestamp, e.g. 20260825T083813Z")
    parser.add_argument("--reports-dir", default="runtime/reports")
    parser.add_argument("--logs-dir", default="runtime/demo-suite-logs")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    reports_dir = Path(args.reports_dir)
    logs_dir = Path(args.logs_dir)
    suite_path = reports_dir / f"demo-suite-{args.stamp}.json"
    if not suite_path.exists():
        raise SystemExit(f"suite report not found: {suite_path}")
    suite = json.loads(suite_path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    for item in suite.get("results", []):
        request_id = item.get("request_id")
        if not request_id:
            continue
        efficiency_path = reports_dir / f"execution-efficiency-{request_id}.json"
        efficiency = json.loads(efficiency_path.read_text(encoding="utf-8")) if efficiency_path.exists() else None
        if efficiency is None:
            missing.append(request_id)
        log_path = Path(item.get("log", ""))
        if not log_path.exists():
            log_path = logs_dir / f"{args.stamp}-{item['name']}.log"
        rows.append({
            "name": item["name"],
            "request_id": request_id,
            "passed": item.get("passed"),
            "efficiency": efficiency,
            "log_summary": summarize_log(log_path),
        })

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "suite_stamp": args.stamp,
        "suite_passed": suite.get("passed"),
        "case_count": len(rows),
        "efficiency_files_found": len(rows) - len(missing),
        "missing_efficiency_files": missing,
        "cases": rows,
    }
    output = Path(args.output) if args.output else reports_dir / f"efficiency-audit-{args.stamp}.json"
    output.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    md = output.with_suffix(".md")
    lines = [
        f"# Execution Efficiency Audit ({args.stamp})",
        "",
        f"Suite: {'PASS' if suite.get('passed') else 'FAIL'}",
        f"Cases: {len(rows)}; efficiency files: {len(rows) - len(missing)}; missing: {len(missing)}",
        "",
        "| Case | Result | Efficiency | BBO events | BBO coalesced | Book queue P95 ms | Maker ACK avg/P95 ms | Amend ACK P95 ms | Quote age P95 ms | Reprices | Hedge ACK P95 ms | IOC fill rate | Warnings |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        eff = row.get("efficiency") or {}
        metrics = eff.get("efficiency", {})
        maker_ack = metrics.get("maker_ack_latency", {})
        maker_amend = metrics.get("maker_amend_latency", {})
        quote_age = metrics.get("maker_quote_age", {})
        book_dispatch = metrics.get("book_dispatch_latency", {})
        hedge_ack = metrics.get("hedge_ack_latency", {})
        lines.append(
            f"| {row['name']} | {'PASS' if row['passed'] else 'FAIL'} | "
            f"{metrics.get('status', 'MISSING')} | {metrics.get('bbo_events', '-')}"
            f" | {metrics.get('bbo_coalesced', '-')} ({metrics.get('bbo_coalesce_rate_pct', '-')}%)"
            f" | {book_dispatch.get('p95_ms', '-')}"
            f" | {maker_ack.get('avg_ms', '-')}/{maker_ack.get('p95_ms', '-')}"
            f" | {maker_amend.get('p95_ms', '-')}"
            f" | {quote_age.get('p95_ms', '-')}"
            f" | {metrics.get('maker_reprices', '-')}"
            f" | {hedge_ack.get('p95_ms', '-')}"
            f" | {metrics.get('hedge_fill_rate_pct', '-')}"
            f" | {','.join(metrics.get('warnings', [])) or '-'} |"
        )
    if missing:
        lines.extend(["", "## Missing efficiency files", ""])
        lines.extend(f"- {request_id}" for request_id in missing)
        lines.append("")
        lines.append("These orders completed before efficiency persistence was added, or did not reach PARENT_COMPLETED.")
    md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({
        "audit_json": str(output),
        "audit_md": str(md),
        "cases": len(rows),
        "efficiency_files_found": len(rows) - len(missing),
        "missing": len(missing),
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
