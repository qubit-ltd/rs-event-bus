#!/usr/bin/env python3
"""Validate downstream projects from isolated copies of their tracked state."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

try:
    from scripts.check_event_bus_graph import validate_graph_for_downstream
    from scripts.downstream_source_snapshot import copy_tracked_source
    from scripts.run_conformance_watchdog import process_group_options
    from scripts.run_conformance_watchdog import terminate_process_group
except ModuleNotFoundError:
    from check_event_bus_graph import validate_graph_for_downstream
    from downstream_source_snapshot import copy_tracked_source
    from run_conformance_watchdog import process_group_options
    from run_conformance_watchdog import terminate_process_group


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


def _read_output(stream):
    stream.flush()
    stream.seek(0)
    return stream.read()


def _print_captured(label, stdout, stderr, *, print_stdout):
    if stdout and print_stdout:
        print(stdout, end="")
    if stderr:
        print(stderr, end="", file=sys.stderr)


def run(command, *, cwd, env, label, print_stdout=True, timeout=TIMEOUT_SECONDS):
    """Run a bounded command and kill/reap its process group on timeout or interrupt."""
    print(f"[{label}] $ {' '.join(map(str, command))}", flush=True)
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace") as stdout_file, \
            tempfile.TemporaryFile(mode="w+", encoding="utf-8", errors="replace") as stderr_file:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            text=True,
            stdout=stdout_file,
            stderr=stderr_file,
            **process_group_options(),
        )
        timed_out = False
        interrupted = False
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            terminate_process_group(process)
            timed_out = True
        except KeyboardInterrupt:
            terminate_process_group(process)
            interrupted = True

        stdout = _read_output(stdout_file)
        stderr = _read_output(stderr_file)
        _print_captured(label, stdout, stderr, print_stdout=print_stdout or timed_out or interrupted)
        if timed_out:
            print(f"[{label}] timed out after {timeout}s", file=sys.stderr)
            raise RuntimeError(f"{label} timed out")
        if interrupted:
            print(f"[{label}] interrupted; terminated process group", file=sys.stderr)
            raise KeyboardInterrupt
        if process.returncode:
            raise RuntimeError(f"{label} exited {process.returncode}")
        print(f"[{label}] passed", flush=True)
        return stdout


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

    # The first pass writes a lockfile in the isolated snapshot if Cargo needs to
    # resolve a patched local dependency. The locked pass validates that stable graph.
    run(command, cwd=root, env=env, label=f"metadata-update:{root.name}:{features or 'all'}",
        print_stdout=False)
    locked_output = run([*command, "--locked"], cwd=root, env=env,
                        label=f"metadata-locked:{root.name}:{features or 'all'}",
                        print_stdout=False)
    graph = json.loads(locked_output)
    validate_graph_for_downstream(graph, expected_core=core,
                                  expected_redis=redis if root.name == "rs-task" else None)
    return graph


def verify_test(root, name, arguments, core, redis, env):
    manifest = root / "Cargo.toml"
    config = cargo_config(core, redis)
    list_command = ["cargo", "test", "--locked", "--manifest-path", str(manifest), *config,
                    *arguments, "--", "--list", "--exact"]
    listing = run(list_command, cwd=root, env=env, label=f"list:{root.name}:{name}")
    matches = [line for line in listing.splitlines() if line.strip() == f"{name}: test"]
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one test listing for {name!r}, got {len(matches)}")
    command = ["cargo", "test", "--locked", "--manifest-path", str(manifest), *config,
               *arguments, "--", "--exact", "--test-threads=1", "--format", "pretty",
               "--color", "never"]
    output = run(command, cwd=root, env=env, label=f"test:{root.name}:{name}")
    target_line = f"test {name} ... ok"
    target_runs = [line.strip() for line in output.splitlines() if line.strip() == target_line]
    if len(target_runs) != 1:
        raise RuntimeError(
            f"expected exactly one passing execution of {name!r}, got {len(target_runs)}"
        )
    summaries = [line.strip() for line in output.splitlines()
                 if line.strip().startswith("test result:")]
    if len(summaries) != 1:
        raise RuntimeError(f"expected exactly one successful test summary for {name!r}")
    summary = re.fullmatch(
        r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; "
        r"(\d+) measured; (\d+) filtered out(?:; finished in .+)?",
        summaries[0],
    )
    if summary is None:
        raise RuntimeError(f"expected exactly one successful test summary for {name!r}")
    passed, failed, ignored, measured, _filtered = map(int, summary.groups())
    if (passed, failed, ignored, measured) != (1, 0, 0, 0):
        raise RuntimeError(
            f"expected {name!r} to be the only executed test (1 passed, 0 failed/ignored/measured), "
            f"got {passed} passed, {failed} failed, {ignored} ignored, {measured} measured"
        )


def git_revision_and_tracked_changes(root):
    revision = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    changes = subprocess.run(["git", "-C", str(root), "status", "--porcelain=v1",
                              "--untracked-files=no"],
                             capture_output=True, text=True, check=True).stdout.splitlines()
    return revision, changes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-root", type=Path, required=True)
    parser.add_argument("--task-root", type=Path, required=True)
    args = parser.parse_args()
    core = Path(__file__).resolve().parents[1]
    inputs = (("redis", args.redis_root.resolve()), ("rs-task", args.task_root.resolve()))
    for label, root in (("core", core), *inputs):
        if not (root / "Cargo.toml").is_file():
            raise ValueError(f"{label} root has no Cargo.toml: {root}")
    for label, root in (("core", core), *inputs):
        revision, changes = git_revision_and_tracked_changes(root)
        print(f"{label}: revision={revision} root={root} "
              f"tracked_changes={changes!r}", flush=True)

    with tempfile.TemporaryDirectory(prefix="event-bus-downstream-") as temp:
        temp_root = Path(temp)
        snapshots = {}
        for label, source in inputs:
            snapshot = temp_root / "sources" / label
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            copy_tracked_source(source, snapshot)
            if not (snapshot / "Cargo.toml").is_file():
                raise ValueError(f"tracked source snapshot is missing Cargo.toml: {label}")
            snapshots[label] = snapshot

        target_root = temp_root / "target"
        redis = snapshots["redis"]
        task = snapshots["rs-task"]
        metadata(redis, None, core, None, target_root / "redis")
        metadata(task, "sqlite,event-bus", core, redis, target_root / "rs-task")
        for label, manifest, test_name, command in TESTS:
            root = task if label == "rs-task-outbox" else redis
            package = redis if root == task else None
            env = os.environ.copy()
            env["CARGO_TARGET_DIR"] = str(target_root / ("rs-task" if root == task else "redis"))
            verify_test(root, test_name, command, core, package, env)
    print("downstream compatibility checks passed", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError,
            json.JSONDecodeError) as error:
        print(f"downstream compatibility check failed: {error}", file=sys.stderr)
        raise SystemExit(1)
