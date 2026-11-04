#!/usr/bin/env bash
set -euo pipefail

artifact_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
run_dir="${1:-$artifact_dir/paired-replay}"
mkdir -p "$run_dir"
for round in 1 2 3 4 5 6 7; do
  set +e
  "$artifact_dir/baseline/bench-binary" > "$run_dir/baseline-$round.stdout" 2> "$run_dir/baseline-$round.stderr"
  baseline_status=$?
  printf 'EXIT_CODE=%s\n' "$baseline_status" >> "$run_dir/baseline-$round.stderr"
  if (( baseline_status != 0 )); then exit "$baseline_status"; fi
  "$artifact_dir/candidate/bench-binary" > "$run_dir/candidate-$round.stdout" 2> "$run_dir/candidate-$round.stderr"
  candidate_status=$?
  printf 'EXIT_CODE=%s\n' "$candidate_status" >> "$run_dir/candidate-$round.stderr"
  if (( candidate_status != 0 )); then exit "$candidate_status"; fi
  set -e
done
