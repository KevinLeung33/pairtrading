from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


PATTERNS = {
    "error": re.compile(r"ERROR|Traceback|RuntimeError|Exception|HTTP/1\.1 4|HTTP/1\.1 5|fatal", re.I),
    "risk": re.compile(r"HEDGE_FAILED|EXPOSURE_LIMIT|RECOVERY|MANUAL|unhedged|exposure", re.I),
    "order": re.compile(r"trade/order|ordId|clOrdId|CHILD_TERMINAL|PARENT_COMPLETED", re.I),
    "cancel": re.compile(r"cancel|canceled|cancelled", re.I),
    "warning": re.compile(r"WARNING|WARN|retry|timeout|reconnect", re.I),
}


def classify(line: str) -> str | None:
    for name, pattern in PATTERNS.items():
        if pattern.search(line):
            return name
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Condense OKX Demo runner logs")
    parser.add_argument("logfile")
    parser.add_argument("--output-dir", default="runtime/reports")
    args = parser.parse_args()
    path = Path(args.logfile)
    if not path.exists():
        raise SystemExit(f"log file not found: {path}")

    counts: Counter[str] = Counter()
    suspicious: list[str] = []
    selected: list[str] = []
    for raw in path.read_text(errors="replace").splitlines():
        line = raw.strip()
        category = classify(line)
        if category:
            counts[category] += 1
            selected.append(line)
        if PATTERNS["error"].search(line) or PATTERNS["risk"].search(line):
            suspicious.append(line)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": stamp,
        "source": str(path),
        "line_count": len(path.read_text(errors="replace").splitlines()),
        "category_counts": dict(counts),
        "issue_count": len(suspicious),
        "issues": suspicious,
        "selected_events": selected,
    }
    json_path = output_dir / f"demo-log-summary-{stamp}.json"
    md_path = output_dir / f"demo-log-summary-{stamp}.md"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = [f"# Demo Log Summary ({stamp})", "", f"Source: `{path}`", "", "## Counts", "", "| Category | Count |", "|---|---:|"]
    for key in ("error", "risk", "warning", "order", "cancel"):
        lines.append(f"| {key} | {counts.get(key, 0)} |")
    lines.extend(["", "## Issues", ""])
    if suspicious:
        lines.extend(f"- `{line}`" for line in suspicious)
    else:
        lines.append("No error or risk lines detected.")
    lines.extend(["", "## Selected events", ""])
    lines.extend(f"- `{line}`" for line in selected[-200:])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"summary: {md_path}")
    print(f"json: {json_path}")
    print(f"issues: {len(suspicious)}")
    raise SystemExit(1 if suspicious else 0)


if __name__ == "__main__":
    main()