#!/usr/bin/env bash
set -Eeuo pipefail
PROJECT_DIR="$(cd "$(dirname "__BASH_SOURCE__")/.." && pwd)"
cd "$PROJECT_DIR"
source .venv/bin/activate
exec python scripts/export_efficiency.py "$@"