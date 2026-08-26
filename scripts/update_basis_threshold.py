#!/usr/bin/env python3
"""Atomically update Basis controls for an already-running strategy."""

from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def non_negative_decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update controls used by a running Basis strategy"
    )
    parser.add_argument("--state-path", required=True)
    parser.add_argument("--control-path")
    parser.add_argument("--entry-threshold-bp", type=non_negative_decimal)
    parser.add_argument("--pause-threshold-bp", type=non_negative_decimal)
    parser.add_argument("--resume-threshold-bp", type=non_negative_decimal)
    parser.add_argument("--exit-threshold-bp", type=non_negative_decimal)
    parser.add_argument("--resume-exposure-base-qty", type=non_negative_decimal)
    args = parser.parse_args()

    updates: dict[str, Any] = {}
    for option, key in (
        ("entry_threshold_bp", "basis_entry_threshold_bp"),
        ("pause_threshold_bp", "basis_pause_threshold_bp"),
        ("resume_threshold_bp", "basis_resume_threshold_bp"),
        ("exit_threshold_bp", "basis_exit_threshold_bp"),
        ("resume_exposure_base_qty", "basis_resume_exposure_base_qty"),
    ):
        value = getattr(args, option)
        if value is not None:
            updates[key] = str(value)
    if not updates:
        parser.error("provide at least one threshold option")

    control_path = (
        Path(args.control_path)
        if args.control_path
        else Path(args.state_path).with_suffix(".basis-control.json")
    )
    payload: dict[str, Any] = {}
    if control_path.exists():
        try:
            existing = json.loads(control_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            parser.error(f"cannot read {control_path}: {exc}")
        if not isinstance(existing, dict):
            parser.error(f"{control_path} must contain a JSON object")
        payload.update(existing)
    payload.update(updates)

    control_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = control_path.with_name(
        f"{control_path.name}.tmp-{os.getpid()}"
    )
    temp_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temp_path, control_path)
    print(f"Updated {control_path}")
    for key in updates:
        print(f"{key}={payload[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())