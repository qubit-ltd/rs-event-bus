"""Check resolved event-bus release dependency graphs."""

import json
from pathlib import Path
import subprocess
import sys

def validate_graph(graph, *, require_bus, expected_core=None):
    """Validate a resolved cargo metadata graph."""
    resolve = graph.get("resolve")
    if resolve is None:
        raise ValueError("cargo metadata must include the resolved dependency graph")
    edges = {node["id"]: node["dependencies"] for node in resolve["nodes"]}
    reachable = set()
    pending = list(graph["workspace_members"])
    while pending:
        identity = pending.pop()
        if identity not in reachable:
            reachable.add(identity)
            pending.extend(edges[identity])
    packages = [package for package in graph["packages"]
                if package["id"] in reachable
                and package["name"] == "qubit-event-bus"]
    if not packages:
        if require_bus:
            raise ValueError("qubit-event-bus is missing from the resolved graph")
        return
    if len(packages) != 1:
        identities = ", ".join(package["id"] for package in packages)
        raise ValueError(f"multiple qubit-event-bus packages: {identities}")
    package = packages[0]
    if package["version"].split(".")[:2] != ["0", "21"]:
        raise ValueError(f"expected qubit-event-bus 0.21.x, got {package['version']}")
    if expected_core is not None:
        expected = (expected_core / "Cargo.toml").resolve()
        actual = Path(package["manifest_path"]).resolve()
        if package["source"] is not None or actual != expected:
            raise ValueError(f"expected local core {expected}, got {package['id']} at {actual}")


def main():
    project = Path(__file__).absolute().parents[1]
    manifest = project / "Cargo.toml"
    print("event-bus graph gate: standalone core package", flush=True)
    command = ["cargo", "metadata", "--locked", "--all-features",
               "--format-version", "1", "--manifest-path", str(manifest)]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="")
    try:
        validate_graph(json.loads(result.stdout), require_bus=True,
                       expected_core=project)
    except ValueError as error:
        raise ValueError(f"{manifest}: {error}") from error
    print(f"event-bus graph gate: passed {manifest}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, subprocess.CalledProcessError) as error:
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        print(f"event-bus graph gate: {error}", file=sys.stderr)
        sys.exit(1)
