#!/bin/bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
exec env \
    RS_CI_PROJECT_ROOT="$PROJECT_ROOT" \
    RUN_COVERAGE_CFG_CLIPPY="${RUN_COVERAGE_CFG_CLIPPY:-1}" \
    "$PROJECT_ROOT/.rs-ci/align-ci.sh" "$@"
