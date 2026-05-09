#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port "${APP_PORT:-7777}" --reload
