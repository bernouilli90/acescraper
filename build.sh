#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

APP_VERSION=$(git rev-parse --short HEAD 2>/dev/null || echo "dev")
export APP_VERSION

echo "Building acescraper v${APP_VERSION}..."
docker compose build "$@"
echo "Done: acescraper v${APP_VERSION}"
