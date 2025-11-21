#!/usr/bin/env bash
# Usage: ./scripts/build.sh [fmt|vet|lint|clean|ci|pre-commit]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

CMD="${1:-build}"

case "$CMD" in
    build)
        echo "Building library..."
        go build -v ./...
        ;;
    fmt)
        echo "Formatting code..."
        go fmt ./...
        ;;
    vet)
        echo "Running go vet..."
        go vet ./...
        ;;
    lint)
        echo "Running golangci-lint..."
        golangci-lint run ./...
        ;;
    clean)
        echo "Cleaning up..."
        rm -f coverage.out coverage.html coverage-integration.out coverage-integration.html
        go clean -testcache
        ;;
    ci)
        echo "Running CI checks (build + vet + unit tests)..."
        go build -v ./...
        go vet ./...
        go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
        ;;
    pre-commit)
        echo "Running pre-commit checks (fmt + vet + modernize + unit tests)..."
        go fmt ./...
        go vet ./...
        go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -test ./...
        go test -v -race ./...
        ;;
    *)
        echo "Usage: $0 [build|fmt|vet|lint|clean|ci|pre-commit]"
        exit 1
        ;;
esac
