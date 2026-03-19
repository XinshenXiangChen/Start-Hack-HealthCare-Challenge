#!/usr/bin/env bash
set -euo pipefail

# Build/run your Docker stack from a clean slate (no cache).
# This script is macOS-focused (bash).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[build] Stopping containers + removing volumes..."
docker compose down --volumes --remove-orphans || true

echo "[build] Removing runtime/ folder..."
rm -rf runtime || true

echo "[build] Building app image with --no-cache..."
docker compose build --no-cache app

echo "[build] Starting stack..."
docker compose up -d --force-recreate

echo "[build] Done. Dashboard should be on http://127.0.0.1:8000/"
