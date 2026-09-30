#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
project_root=$(cd "$script_dir/../.." && pwd -P)
source "$project_root/.infra/tools/cleanup-build-artifacts.sh"
"$project_root/.infra/tools/prepare-local-path-dependencies.sh" "$@"
