#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
python3 -B -m unittest discover -s "$project_root/scripts/tests" -p 'performance_evidence_tests.py'
python3 -B -m unittest discover -s "$project_root/scripts/tests" -p 'documentation_contract_tests.py'
package_list=$(mktemp)
trap 'rm -f "$package_list"' EXIT
cargo package --list --locked --allow-dirty --manifest-path "$project_root/Cargo.toml" > "$package_list"
python3 -B "$project_root/scripts/check_performance_evidence.py" --package-list "$package_list"
python3 -B "$project_root/scripts/check_documentation_contract.py" --package-list "$package_list"
rm -f "$package_list"
trap - EXIT
python3 -B -m unittest discover -s "$project_root/scripts/tests" -p '*graph_tests.py'
exec python3 -B "$project_root/scripts/check_event_bus_graph.py"
