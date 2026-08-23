#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required. Install it with: sudo apt update && sudo apt install -y python3 python3-venv python3-pip"
  exit 1
fi

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[test]"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env. Edit it with: nano .env"
fi

mkdir -p runtime/reports
echo "WSL setup complete. Activate with: source .venv/bin/activate"
