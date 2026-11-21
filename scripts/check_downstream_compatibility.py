#!/usr/bin/env python3
"""Validate this event-bus checkout against pinned Redis and rs-task sources."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

try:
    from scripts.check_event_bus_graph import validate_graph_for_downstream
except ModuleNotFoundError:
    from check_event_bus_graph import validate_graph_for_downstream


TIMEOUT_SECONDS = 900
TESTS = (
    ("redis-sync", "tests/sync_standalone_tests.rs", "test_sync_spi_conformance",
     ["--all-features", "--test", "sync_standalone_tests", "test_sync_spi_conformance"]),
    ("redis-async", "tests/async_standalone_tests.rs", "test_async_spi_conformance",
     ["--all-features", "--test", "async_standalone_tests", "test_async_spi_conformance"]),
    ("rs-task-outbox", "tests/typed_service_tests.rs",
     "service::typed_task_execution_tests::test_cancelled_submit_wakes_durable_outbox_publisher",
     ["--features", "sqlite,event-bus", "--test", "typed_service_tests",
      "service::typed_task_execution_tests::test_cancelled_submit_wakes_durable_outbox_publisher"]),
)


def run(command, *, cwd, env, label, print_stdout=True):
    print(f"[{label}] $ {' '.join(map(str, command))}", flush=True)
    try:
        result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True,
                                timeout=TIMEOUT_SECONDS, check=False)
    except subprocess.TimeoutExpired as error:
        print(f"[{label}] timed out after {TIMEOUT_SECONDS}s", file=sys.stderr)
        if error.stdout:
            print(error.stdout, file=sys.stderr)
        if error.stderr:
            print(error.stderr, file=sys.stderr)
        raise RuntimeError(f"{label} timed out") from error
    if result.stdout and print_stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode:
        raise RuntimeError(f"{label} exited {result.returncode}")
    print(f"[{label}] passed", flush=True)
    return result.stdout


def cargo_config(core, redis=None):
    configs = ["patch.crates-io.qubit-event-bus.path=" + json.dumps(str(core))]
    if redis is not None:
        configs.append("patch.crates-io.qubit-event-bus-redis.path=" + json.dumps(str(redis)))
    return [part for config in configs for part in ("--config", config)]


def metadata(root, features, core, redis, target):
    manifest = root / "Cargo.toml"
    command = ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest)]
    command.extend(cargo_config(core, redis))
    if features:
        command.extend(["--features", features])
    else:
        command.append("--all-features")
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target)
    output = run(command, cwd=root, env=env, label=f"metadata:{root.name}:{features or 'all'}",
                 print_stdout=False)
    graph = json.loads(output)
    validate_graph_for_downstream(graph, expected_core=core,
                                  expected_redis=redis if root.name == "rs-task" else None)
    return graph


def verify_test(root, name, arguments, core, redis, env):
    manifest = root / "Cargo.toml"
    config = cargo_config(core, redis)
    list_command = ["cargo", "test", "--manifest-path", str(manifest), *config,
                   *arguments, "--", "--list", "--exact"]
    listing = run(list_command, cwd=root, env=env, label=f"list:{root.name}:{name}")
    matches = [line for line in listing.splitlines() if line.strip() == f"{name}: test"]
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one test listing for {name!r}, got {len(matches)}")
    command = ["cargo", "test", "--locked", "--manifest-path", str(manifest), *config,
               *arguments, "--", "--exact", "--test-threads=1"]
    run(command, cwd=root, env=env, label=f"test:{root.name}:{name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-root", type=Path, required=True)
    parser.add_argument("--task-root", type=Path, required=True)
    args = parser.parse_args()
    core = Path(__file__).resolve().parents[1]
    redis = args.redis_root.resolve()
    task = args.task_root.resolve()
    for label, root in (("core", core), ("redis", redis), ("rs-task", task)):
        if not (root / "Cargo.toml").is_file():
            raise ValueError(f"{label} root has no Cargo.toml: {root}")
        revision = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                                  capture_output=True, text=True, check=True).stdout.strip()
        print(f"{label}: revision={revision} root={root}", flush=True)

    locks = {root / "Cargo.lock": (root / "Cargo.lock").read_bytes()
             for root in (redis, task) if (root / "Cargo.lock").is_file()}
    try:
        with tempfile.TemporaryDirectory(prefix="event-bus-downstream-target-") as temp:
            target_root = Path(temp)
            env = os.environ.copy()
            for path, features, package in ((redis, None, None), (task, "sqlite,event-bus", redis)):
                target = target_root / ("redis-metadata" if path == redis else "task-metadata")
                graph = metadata(path, features, core, package, target)
                print(f"metadata: {path.name} packages={len(graph['packages'])} core={core}")
            for label, manifest, test_name, command in TESTS:
                root = task if label == "rs-task-outbox" else redis
                package = redis if root == task else None
                target = target_root / label
                env["CARGO_TARGET_DIR"] = str(target)
                verify_test(root, test_name, command, core, package, env)
    finally:
        for path, contents in locks.items():
            path.write_bytes(contents)
    print("downstream compatibility checks passed", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"downstream compatibility check failed: {error}", file=sys.stderr)
        raise SystemExit(1)
