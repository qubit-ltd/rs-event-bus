#!/usr/bin/env python3
"""Select a unique Cargo executable by target name and target kind."""

import json
import os
import sys


def select_executable(messages, target_name, target_kind="test"):
    executables = []
    for line_number, line in enumerate(messages.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid Cargo JSON on line {line_number}: {error}") from error
        if message.get("reason") != "compiler-artifact":
            continue
        target = message.get("target", {})
        if target.get("name") != target_name or target_kind not in target.get("kind", []):
            continue
        executable = message.get("executable")
        if executable:
            executables.append(executable)

    if not executables:
        raise ValueError(f"Cargo produced no executable for {target_kind} target {target_name!r}")
    if len(executables) != 1:
        raise ValueError(
            f"Cargo produced {len(executables)} executables for test target {target_name!r}; expected exactly one"
        )
    executable = executables[0]
    if not os.path.isfile(executable):
        raise ValueError(f"Cargo test executable does not exist: {executable}")
    if not os.access(executable, os.X_OK):
        raise ValueError(f"Cargo test executable is not executable: {executable}")
    return executable


def main(argv):
    if len(argv) not in (2, 4) or (len(argv) == 4 and argv[2] != "--kind"):
        print(f"usage: {argv[0]} TARGET_NAME [--kind TARGET_KIND]", file=sys.stderr)
        return 2
    try:
        target_kind = argv[3] if len(argv) == 4 else "test"
        print(select_executable(sys.stdin.read(), argv[1], target_kind))
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
