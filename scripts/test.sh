#!/usr/bin/env bash
# Merges the unit and integration coverage profiles into one combined report.
#
# Usage: ./scripts/test.sh
#
# Both suites cover the same package, so a statement exercised by either counts as
# covered. Runs the unit suite, then the integration suite (which starts its own
# Kafka), and reports the union.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

"${SCRIPT_DIR}/test_unit.sh" coverage
"${SCRIPT_DIR}/test_integration.sh" coverage

echo "Merging coverage profiles..."
{
    echo "mode: atomic"
    tail -n +2 coverage.out
    tail -n +2 coverage_integration.out
} | awk '
    NR == 1 { print; next }
    {
        block = $1 " " $2
        count[block] += $3
        if (!(block in seen)) { seen[block] = 1; order[++n] = block }
    }
    END { for (i = 1; i <= n; i++) { split(order[i], f, " "); print f[1], f[2], count[order[i]] } }
' > coverage_combined.out

go tool cover -html=coverage_combined.out -o coverage_combined.html
echo "Combined coverage report: coverage_combined.html"
go tool cover -func=coverage_combined.out | grep total:
