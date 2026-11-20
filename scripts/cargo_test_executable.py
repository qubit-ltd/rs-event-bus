#!/usr/bin/env python3
"""Select the unique executable for a test target from Cargo JSON messages."""

import json
import os
import sys


def select_executable(messages, target_name):
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
        if target.get("name") != target_name or "test" not in target.get("kind", []):
            continue
        executable = message.get("executable")
        if executable:
            executables.append(executable)

    if not executables:
        raise ValueError(f"Cargo produced no executable for test target {target_name!r}")
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
    if len(argv) != 2:
        print(f"usage: {argv[0]} TARGET_NAME", file=sys.stderr)
        return 2
    try:
        print(select_executable(sys.stdin.read(), argv[1]))
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
