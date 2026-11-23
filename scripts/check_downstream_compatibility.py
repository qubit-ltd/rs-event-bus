#!/usr/bin/env python3
"""Validate downstream projects from tracked-source snapshots."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass

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
@dataclass(frozen=True)
class TestConfig:
    label: str
    root: str
    manifest: str
    target: str
    test_name: str
    features: str | None = None
    all_features: bool = False


TESTS = (
    TestConfig("redis-sync", "redis", "Cargo.toml", "sync_standalone_tests",
               "test_sync_spi_conformance", all_features=True),
    TestConfig("redis-async", "redis", "Cargo.toml", "async_standalone_tests",
               "test_async_spi_conformance", all_features=True),
    TestConfig("rs-task-outbox", "rs-task", "Cargo.toml", "typed_service_tests",
               "service::typed_task_execution_tests::test_cancelled_submit_wakes_durable_outbox_publisher",
               features="sqlite,event-bus"),
    TestConfig("redis-wire-limit", "redis", "Cargo.toml", "wire_limit_tests",
               "durable::test_real_core_codec_failures_leave_redis_durable_record_recoverable",
               all_features=True),
    TestConfig("execution-services-managed-event-bus", "execution-services",
               "tests/fixtures/ioc_application_consumer/Cargo.toml",
               "managed_event_bus_tests", "test_blocked_handler_graceful_request_cancel_resume"),
    TestConfig("execution-services-managed-event-bus", "execution-services",
               "tests/fixtures/ioc_application_consumer/Cargo.toml",
               "managed_event_bus_tests", "test_blocked_handler_request_cancel_resume_and_abort_upgrade"),
    TestConfig("execution-services-managed-event-bus", "execution-services",
               "tests/fixtures/ioc_application_consumer/Cargo.toml",
               "managed_event_bus_tests", "test_provider_timed_out_report_reaches_container_diagnostic"),
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


def metadata(root, features, core, redis, target, *, expected_redis=None,
             manifest=None, all_features=False, allowed_local_roots=None):
    manifest = Path(manifest) if manifest is not None else root / "Cargo.toml"
    command = ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest)]
    command.extend(cargo_config(core, redis))
    if all_features:
        command.append("--all-features")
    elif features:
        command.extend(["--features", features])
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target)

    # The first pass writes a lockfile in the isolated snapshot if Cargo needs to
    # resolve a patched local dependency. The locked pass validates that stable graph.
    run(command, cwd=manifest.parent, env=env, label=f"metadata-update:{root.name}:{features or ('all' if all_features else 'default')}",
        print_stdout=False)
    locked_output = run([*command, "--locked"], cwd=manifest.parent, env=env,
                        label=f"metadata-locked:{root.name}:{features or ('all' if all_features else 'default')}",
                        print_stdout=False)
    graph = json.loads(locked_output)
    validate_graph_for_downstream(graph, expected_core=core, expected_redis=expected_redis,
                                  allowed_local_roots=allowed_local_roots)
    return graph


def run_downstream_checks(roots, core, target_root, *, include_execution_services=False):
    selected = (TESTS if include_execution_services else
                tuple(config for config in TESTS if config.root != "execution-services"))
    checked_manifests = set()
    for config in selected:
        root = roots[config.root]
        manifest = root / config.manifest
        package = roots["redis"] if config.root == "rs-task" else None
        target = target_root / config.root
        if config.root == "redis":
            allowed_local_roots = (root,)
        elif config.root == "rs-task":
            allowed_local_roots = (root, roots["redis"])
        else:
            allowed_local_roots = (root.parents[1],)
        key = (manifest, config.features, config.all_features)
        if key not in checked_manifests:
            metadata(root, config.features, core, package, target,
                     expected_redis=package, manifest=manifest,
                     all_features=config.all_features,
                     allowed_local_roots=allowed_local_roots)
            checked_manifests.add(key)
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(target)
        arguments = (["--all-features"] if config.all_features else
                     (["--features", config.features] if config.features else []))
        arguments.extend(["--test", config.target, config.test_name])
        verify_test(root, config.test_name, arguments, core, package, env,
                    manifest=manifest)


def _path_entries(value):
    if isinstance(value, dict):
        if isinstance(value.get("path"), str):
            yield value["path"]
        else:
            for nested in value.values():
                yield from _path_entries(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _path_entries(nested)


def _manifest_paths(data):
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        yield from _path_entries(data.get(section, {}))
    for target in data.get("target", {}).values():
        for section in ("dependencies", "dev-dependencies", "build-dependencies"):
            yield from _path_entries(target.get(section, {}))
    yield from _path_entries(data.get("workspace", {}).get("dependencies", {}))
    yield from _path_entries(data.get("patch", {}))


def _repository_root(path, boundary):
    path = path.resolve()
    for candidate in (path, *path.parents):
        if candidate == boundary.parent:
            break
        if (candidate / ".git").exists():
            return candidate
    return path


def discover_local_repositories(manifest, sibling_root, source_root=None):
    """Find sibling repositories required by an IoC fixture's local paths and patches."""
    import tomllib

    manifest = Path(manifest).resolve()
    boundary = Path(sibling_root).resolve()
    source_root = Path(source_root).resolve() if source_root else manifest.parents[3]
    if not manifest.is_file():
        raise ValueError(f"manifest does not exist: {manifest}")
    repositories = {source_root}
    pending = [manifest]
    scanned = set()
    while pending:
        current = pending.pop().resolve()
        if current in scanned:
            continue
        scanned.add(current)
        try:
            data = tomllib.loads(current.read_text())
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise ValueError(f"cannot read local dependency manifest {current}: {error}") from error
        for relative in _manifest_paths(data):
            if Path(relative).is_absolute():
                raise ValueError(
                    f"absolute local path in {current}: {relative}; use a path relative "
                    "to the referencing manifest so it remains valid in the source snapshot"
                )
            dependency = (current.parent / relative).resolve()
            try:
                dependency.relative_to(boundary)
            except ValueError as error:
                raise ValueError(f"local path escapes sibling root in {current}: {relative} -> {dependency}") from error
            dependency_manifest = dependency / "Cargo.toml"
            if not dependency.is_dir() or not dependency_manifest.is_file():
                raise ValueError(f"missing local path in {current}: {relative} -> {dependency}")
            repository = _repository_root(dependency, boundary)
            repositories.add(repository)
            pending.append(dependency_manifest)
    return sorted(repositories)


def verify_test(root, name, arguments, core, redis, env, *, manifest=None):
    manifest = Path(manifest) if manifest is not None else root / "Cargo.toml"
    config = cargo_config(core, redis)
    list_command = ["cargo", "test", "--locked", "--manifest-path", str(manifest), *config,
                    *arguments, "--", "--list", "--exact"]
    listing = run(list_command, cwd=manifest.parent, env=env, label=f"list:{root.name}:{name}")
    matches = [line for line in listing.splitlines() if line.strip() == f"{name}: test"]
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one test listing for {name!r}, got {len(matches)}")
    command = ["cargo", "test", "--locked", "--manifest-path", str(manifest), *config,
               *arguments, "--", "--exact", "--test-threads=1", "--format", "pretty",
               "--color", "never"]
    output = run(command, cwd=manifest.parent, env=env, label=f"test:{root.name}:{name}")
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
    parser.add_argument("--execution-services-root", type=Path)
    parser.add_argument("--core-root", type=Path)
    parser.add_argument("--use-worktrees", action="store_true",
                        help="compatibility alias; validate tracked-source snapshots")
    args = parser.parse_args()
    if args.execution_services_root is not None:
        try:
            import tomllib  # noqa: F401
        except ModuleNotFoundError as error:
            if error.name == "tomllib":
                raise ValueError(
                    "--execution-services-root requires Python 3.11 or newer (tomllib is unavailable)"
                ) from error
            raise
    core = (args.core_root or Path(__file__).resolve().parents[1]).resolve()
    inputs = [("redis", args.redis_root.resolve()), ("rs-task", args.task_root.resolve())]
    if args.execution_services_root is not None:
        inputs.append(("execution-services", args.execution_services_root.resolve()))
    for label, root in (("core", core), *inputs):
        if not (root / "Cargo.toml").is_file():
            raise ValueError(f"{label} root has no Cargo.toml: {root}")
    for label, root in (("core", core), *inputs):
        revision, changes = git_revision_and_tracked_changes(root)
        print(f"{label}: revision={revision} root={root} "
              f"tracked_changes={changes!r}", flush=True)

    local_repositories = None
    if args.execution_services_root is not None:
        execution_source = args.execution_services_root.resolve()
        sibling_root = execution_source.parent
        fixture_manifest = execution_source / "tests/fixtures/ioc_application_consumer/Cargo.toml"
        local_repositories = discover_local_repositories(
            fixture_manifest, sibling_root, execution_source)

    with tempfile.TemporaryDirectory(prefix="event-bus-downstream-") as temp:
        temp_root = Path(temp)
        target_root = temp_root / "target"
        snapshots = {}
        for label, source in inputs:
            snapshot = temp_root / "sources" / label
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            copy_tracked_source(source, snapshot)
            if not (snapshot / "Cargo.toml").is_file():
                raise ValueError(f"tracked source snapshot is missing Cargo.toml: {label}")
            snapshots[label] = snapshot
        if args.execution_services_root is not None:
            execution_source = args.execution_services_root.resolve()
            sibling_root = execution_source.parent
            for repository in local_repositories:
                relative = repository.relative_to(sibling_root)
                snapshot = temp_root / "sources" / "siblings" / relative
                snapshot.parent.mkdir(parents=True, exist_ok=True)
                if snapshot.exists():
                    continue
                copy_tracked_source(repository, snapshot)
            snapshots["execution-services"] = (
                temp_root / "sources" / "siblings" /
                execution_source.relative_to(sibling_root))
        run_downstream_checks(snapshots, core, target_root,
                              include_execution_services=args.execution_services_root is not None)
    if args.execution_services_root is None:
        print("IoC checks not selected (pass --execution-services-root to enable)", flush=True)
    print("downstream compatibility checks passed", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError,
            json.JSONDecodeError) as error:
        print(f"downstream compatibility check failed: {error}", file=sys.stderr)
        raise SystemExit(1)
