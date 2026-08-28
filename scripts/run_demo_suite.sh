#!/usr/bin/env bash
set -Eeuo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
source .venv/bin/activate

if [ "${OKX_DEMO:-1}" != "1" ]; then
  echo "Refusing to run: OKX_DEMO must be 1 for this script."
  exit 1
fi

exec python scripts/run_demo_suite.py "$@"
