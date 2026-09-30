#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd -P)
cleanup_script="$project_root/.infra/tools/cleanup-build-artifacts.sh"
test_root=$(mktemp -d "${TMPDIR:-/tmp}/infra-cleanup-test.XXXXXX")
trap 'command rm -rf -- "$test_root"' EXIT

run_case() {
    local name="$1"
    local expected_status="$2"
    local case_root="$test_root/$name"
    local actual_status

    mkdir -p \
        "$case_root/target/debug" \
        "$case_root/target/release" \
        "$case_root/target/llvm-cov-target" \
        "$case_root/target/rs-ci-feature-matrix" \
        "$case_root/target/tmp" \
        "$case_root/target/llvm-cov/html" \
        "$case_root/target/doc" \
        "$case_root/fuzz/target"

    set +e
    (
        project_root="$case_root"
        source "$cleanup_script"
        exit "$expected_status"
    )
    actual_status=$?
    set -e

    if [ "$actual_status" -ne "$expected_status" ]; then
        echo "error: $name returned $actual_status, expected $expected_status" >&2
        return 1
    fi

    for directory in \
        "$case_root/target/debug" \
        "$case_root/target/release" \
        "$case_root/target/llvm-cov-target" \
        "$case_root/target/rs-ci-feature-matrix" \
        "$case_root/target/tmp" \
        "$case_root/fuzz/target"; do
        if [ -e "$directory" ]; then
            echo "error: transient directory remains after $name: $directory" >&2
            return 1
        fi
    done

    for directory in "$case_root/target/llvm-cov/html" "$case_root/target/doc"; do
        if [ ! -d "$directory" ]; then
            echo "error: deliverable directory was removed after $name: $directory" >&2
            return 1
        fi
    done
}

run_case success 0
run_case failure 23
echo "Build artifact cleanup tests passed."
