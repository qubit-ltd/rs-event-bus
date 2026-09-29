#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
python3 -B -m unittest discover -s "$project_root/scripts/tests" -p '*graph_tests.py'
exec python3 -B "$project_root/scripts/check_event_bus_graph.py" "$@"
