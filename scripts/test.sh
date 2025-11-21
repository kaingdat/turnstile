#!/usr/bin/env bash
# Usage: ./scripts/test.sh [coverage]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

COVERAGE="${1:-}"

if [ "$COVERAGE" = "coverage" ]; then
    echo "Running unit tests with coverage..."
    go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
    go tool cover -html=coverage.out -o coverage.html
    echo "Coverage report: coverage.html"
    go tool cover -func=coverage.out | grep total:
else
    echo "Running unit tests..."
    go test -v -race ./...
fi
