#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
PYTHON_BIN="${PYTHON:-python3}"

"$PYTHON_BIN" -m compileall -q app tests
"$PYTHON_BIN" - <<'PY'
from pathlib import Path

compose = Path("docker-compose.yml").read_text(encoding="utf-8")
dockerfile = Path("Dockerfile").read_text(encoding="utf-8")

required_compose = (
    'restart: unless-stopped',
    'init: true',
    'user: "10001:10001"',
    '- "127.0.0.1:8000:8000"',
    'read_only: true',
    'cap_drop:',
    '- ALL',
    'no-new-privileges:true',
    '/etc/camel-gateway/gateway.env',
    '/var/lib/camel-gateway:/data',
    '/var/log/camel-gateway:/logs',
)
for item in required_compose:
    if item not in compose:
        raise SystemExit(f"missing required Compose setting: {item}")

if not dockerfile.startswith("FROM python:3.13-slim-trixie\n"):
    raise SystemExit("Dockerfile must use python:3.13-slim-trixie")
if '"--workers", "1"' not in dockerfile:
    raise SystemExit("Uvicorn must run with exactly one worker")
if "USER 10001:10001" not in dockerfile:
    raise SystemExit("Dockerfile must run as the unprivileged gateway user")

print("static deployment assertions: OK")
PY
"$PYTHON_BIN" -m pytest -q -W error

if command -v docker >/dev/null 2>&1; then
    docker compose config --quiet
    if [ "${RUN_DOCKER_BUILD:-0}" = "1" ]; then
        docker compose build --pull
    fi
fi
