"""Check resolved event-bus release dependency graphs."""

import argparse
import json
from pathlib import Path
import subprocess
import sys

REPOSITORIES = ("rs-event-bus", "rs-event-bus-redis", "rs-task", "rs-ioc",
                "rs-execution-services")
FIXTURES = (
    "rs-event-bus/tests/fixtures/discovery_provider",
    "rs-event-bus/tests/fixtures/discovery_consumer",
    "rs-event-bus/tests/fixtures/documentation_consumer",
    "rs-event-bus-redis/tests/fixtures/business_consumer",
    "rs-task/tests/fixtures/doc-examples",
    "rs-ioc/tests/fixtures/application_consumer_current",
    "rs-execution-services/tests/fixtures/ioc_application_consumer",
    "rs-task/tests/fixtures/consumer",
    "rs-task/tests/fixtures/provider",
    "rs-ioc/tests/fixtures/macro_contracts",
    "rs-ioc/tests/fixtures/ioc_cross_crate",
    "rs-ioc/tests/fixtures/application_consumer_current",
    "rs-ioc/tests/fixtures/ioc_bench",
    "rs-execution-services/tests/fixtures/application_consumer",
    "rs-execution-services/tests/fixtures/documentation_consumer",
)
NON_BUS_CONSUMERS = frozenset((
    "rs-ioc",
    "rs-execution-services",
    "rs-task/tests/fixtures/consumer",
    "rs-task/tests/fixtures/provider",
    "rs-ioc/tests/fixtures/macro_contracts",
    "rs-ioc/tests/fixtures/ioc_cross_crate",
    "rs-ioc/tests/fixtures/ioc_bench",
    "rs-execution-services/tests/fixtures/application_consumer",
    "rs-execution-services/tests/fixtures/documentation_consumer",
))


def requires_bus(manifest, root):
    """Require core only for roots and fixtures that consume the event bus."""
    return manifest.parent.relative_to(root).as_posix() not in NON_BUS_CONSUMERS


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
    if package["version"].split(".")[:2] != ["0", "19"]:
        raise ValueError(f"expected qubit-event-bus 0.19.x, got {package['version']}")
    if expected_core is not None:
        expected = (expected_core / "Cargo.toml").resolve()
        actual = Path(package["manifest_path"]).resolve()
        if package["source"] is not None or actual != expected:
            raise ValueError(f"expected local core {expected}, got {package['id']} at {actual}")


def layout_manifests(root):
    """Return controlled-layout manifests after verifying repository presence."""
    paths = [root / name / "Cargo.toml" for name in REPOSITORIES]
    paths.extend(root / name / "Cargo.toml" for name in FIXTURES)
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise ValueError("controlled layout is missing manifests: " + ", ".join(missing))
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ecosystem-root", type=Path,
                        help="explicit five-repository layout; all roots and fixtures are required")
    args = parser.parse_args()
    project = Path(__file__).absolute().parents[1]
    if args.ecosystem_root is None:
        manifests = [project / "Cargo.toml"]
        expected_core = project
        print("event-bus graph gate: standalone core package", flush=True)
    else:
        # Preserve symlink layout spelling when handing manifests to Cargo.
        root = args.ecosystem_root.absolute()
        manifests = layout_manifests(root)
        expected_core = root / "rs-event-bus"
        print("event-bus graph gate: explicit five-repository layout", flush=True)
    for manifest in manifests:
        command = ["cargo", "metadata", "--locked", "--all-features",
                   "--format-version", "1", "--manifest-path", str(manifest)]
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        require_bus = (True if args.ecosystem_root is None
                       else requires_bus(manifest, root))
        try:
            validate_graph(json.loads(result.stdout), require_bus=require_bus,
                           expected_core=expected_core)
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
