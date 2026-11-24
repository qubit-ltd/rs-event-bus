#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
prepare=0
if [[ ${1:-} == --prepare ]]; then
    prepare=1
    shift
fi
mode=${1:-}
shift || true

usage() {
    printf 'usage: %s [--prepare] contract PACKAGE_LIST | [--prepare] examples [--fetch-snapshot]\n' "$0" >&2
    exit 2
}
case "$mode" in
    contract) [[ $# -eq 1 ]] || usage ;;
    examples) [[ $# -le 1 && ( $# -eq 0 || $1 == --fetch-snapshot ) ]] || usage ;;
    *) usage ;;
esac

python_bin=$(command -v python3)
if [[ $prepare -eq 1 ]]; then
    venv_root=$(mktemp -d "${TMPDIR:-/tmp}/rs-event-bus-doc-check.XXXXXX")
    trap 'rm -rf "$venv_root"' EXIT
    "$python_bin" -m venv "$venv_root/venv"
    python_bin="$venv_root/venv/bin/python"
    "$python_bin" -m pip install --disable-pip-version-check -r "$project_root/scripts/requirements-doc-check.txt"
else
    if ! "$python_bin" -c 'import importlib.metadata as m; assert m.version("markdown-it-py") == "3.0.0" and m.version("mdurl") == "0.1.2"' 2>/dev/null; then
        printf 'Documentation checks require markdown-it-py==3.0.0 and mdurl==0.1.2.\n' >&2
        printf 'Prepare and activate an environment with:\n  python3 -m venv "$HOME/.venvs/rs-event-bus-doc-check"\n  source "$HOME/.venvs/rs-event-bus-doc-check/bin/activate"\n  python -m pip install -r scripts/requirements-doc-check.txt\n' >&2
        exit 1
    fi
fi

if [[ $mode == contract ]]; then
    package_list=$1
    "$python_bin" -B -m unittest discover -s "$project_root/scripts/tests" -p 'documentation_contract_tests.py'
    "$python_bin" -B "$project_root/scripts/check_documentation_contract.py" --root "$project_root" --package-list "$package_list"
    exit $?
fi

fetch_args=()
if [[ ${1:-} == --fetch-snapshot ]]; then
    fetch_args+=(--fetch-snapshot)
fi
"$python_bin" -B "$project_root/scripts/check_documentation_examples.py" --check-snapshot-content "${fetch_args[@]}"
"$python_bin" -B "$project_root/scripts/test_documentation_examples.py"
