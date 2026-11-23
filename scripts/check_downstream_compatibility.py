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
            allowed_local_roots = (root.parent,)
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
    yield from _path_entries(data.get("patch", {}))


def _dependency_sections(data):
    """Yield dependency tables from normal, target-specific, and workspace scope."""
    for section in ("dependencies", "dev-dependencies", "build-dependencies"):
        yield data.get(section, {})
    for target in data.get("target", {}).values():
        for section in ("dependencies", "dev-dependencies", "build-dependencies"):
            yield target.get(section, {})


def _workspace_root(manifest, data):
    """Find the workspace Cargo uses for this package, respecting nested roots."""
    package = data.get("package", {})
    if isinstance(package.get("workspace"), str):
        return (manifest.parent / package["workspace"]).resolve()
    if "workspace" in data:
        return manifest.parent
    tomllib = __import__("tomllib")
    for parent in manifest.parent.parents:
        root_manifest = parent / "Cargo.toml"
        if not root_manifest.is_file():
            continue
        try:
            root_data = tomllib.loads(root_manifest.read_text())
        except (OSError, tomllib.TOMLDecodeError):
            continue
        if "workspace" in root_data:
            return parent if _is_workspace_member(manifest.parent, parent, root_data) else None
    return None


def _glob_matches(path, pattern):
    import re
    pattern = pattern.strip("/")
    parts = []
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if char == "*":
            if index + 1 < len(pattern) and pattern[index + 1] == "*":
                parts.append(".*")
                index += 2
            else:
                parts.append("[^/]*")
                index += 1
        elif char == "?":
            parts.append("[^/]")
            index += 1
        else:
            parts.append(re.escape(char))
            index += 1
    return re.fullmatch("".join(parts), path) is not None


def _is_workspace_member(package_dir, root, root_data):
    workspace = root_data["workspace"]
    relative = package_dir.relative_to(root).as_posix()
    excludes = workspace.get("exclude", [])
    if any(_glob_matches(relative, pattern) for pattern in excludes):
        return False
    explicit = False
    for member in workspace.get("members", []):
        if _glob_matches(relative, member):
            explicit = True
            break
        try:
            for candidate in root.glob(member):
                candidate_manifest = candidate / "Cargo.toml" if candidate.is_dir() else candidate
                if candidate_manifest.is_file() and candidate_manifest.parent.resolve() == package_dir:
                    explicit = True
                    break
        except (OSError, ValueError):
            continue
        if explicit:
            break
    if explicit:
        return True

    # Cargo also makes in-tree path dependencies implicit workspace members.
    # Follow paths only from explicit/root members, and stop at nested workspace
    # roots so unrelated fixture packages are not pulled in accidentally.
    tomllib = __import__("tomllib")
    members = {root.resolve()} if "package" in root_data else set()
    for member in workspace.get("members", []):
        try:
            candidates = root.glob(member)
        except (OSError, ValueError):
            continue
        for candidate in candidates:
            candidate_manifest = candidate / "Cargo.toml" if candidate.is_dir() else candidate
            if candidate_manifest.is_file():
                members.add(candidate_manifest.parent.resolve())
    pending = list(members)
    visited = set()
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        current_manifest = current / "Cargo.toml"
        try:
            current_data = tomllib.loads(current_manifest.read_text())
        except (OSError, tomllib.TOMLDecodeError):
            continue
        paths = list(_manifest_paths(current_data))
        if "workspace" in current_data:
            paths.extend(_inherited_workspace_paths(current_data, current_data))
        else:
            paths.extend(_inherited_workspace_paths(current_data, root_data))
        for relative in paths:
            if Path(relative).is_absolute():
                continue
            dependency = (current / relative).resolve()
            try:
                dependency.relative_to(root.resolve())
            except ValueError:
                continue
            dependency_manifest = dependency / "Cargo.toml"
            if not dependency_manifest.is_file():
                continue
            try:
                dependency_data = tomllib.loads(dependency_manifest.read_text())
            except (OSError, tomllib.TOMLDecodeError):
                continue
            if "workspace" in dependency_data:
                continue
            if any(_glob_matches(dependency.relative_to(root).as_posix(), pattern)
                   for pattern in excludes):
                continue
            if dependency == package_dir:
                return True
            pending.append(dependency)
    return False


def _inherited_workspace_paths(data, workspace_data):
    workspace_dependencies = workspace_data.get("workspace", {}).get("dependencies", {})
    used = set()
    for section in _dependency_sections(data):
        for name, dependency in section.items():
            if isinstance(dependency, dict) and dependency.get("workspace") is True:
                used.add(name)
    for name in sorted(used):
        dependency = workspace_dependencies.get(name)
        if dependency is None:
            continue
        yield from _path_entries(dependency)


def _cargo_config_paths(manifest, source_root):
    """Find Cargo configs discovered from the fixture cwd inside the checkout."""
    source_root = source_root.resolve()
    current = manifest.parent.resolve()
    configs = []
    while True:
        if current == source_root or source_root in current.parents:
            for name in ("config", "config.toml"):
                candidate = current / ".cargo" / name
                if candidate.is_file():
                    configs.append(candidate)
        if current == source_root or current.parent == current:
            break
        current = current.parent
    return configs


def _config_patch_declarations(config_path, data):
    """Return configured patches keyed by registry and package name."""
    for registry, packages in data.get("patch", {}).items():
        if isinstance(packages, dict):
            for name, package in packages.items():
                if isinstance(package, dict) and isinstance(package.get("path"), str):
                    yield (registry, name), package["path"]


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
    manifest = Path(manifest).resolve()
    boundary = Path(sibling_root).resolve()
    source_root = Path(source_root).resolve() if source_root else manifest.parents[3]
    if not manifest.is_file():
        raise ValueError(f"manifest does not exist: {manifest}")
    repositories = {source_root}
    pending = [manifest]
    scanned = set()
    import tomllib

    config_paths = _cargo_config_paths(manifest, source_root)
    effective_config_patches = {}
    for config_path in config_paths:
        try:
            config_data = tomllib.loads(config_path.read_text())
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise ValueError(f"cannot read Cargo config {config_path}: {error}") from error
        # Cargo applies the nearest config first. More distant config files
        # provide defaults only for patch entries that are not overridden.
        for key, relative in _config_patch_declarations(config_path, config_data):
            effective_config_patches.setdefault(
                key, (config_path, relative, config_path.parent.parent))
    for declaration, relative, base in effective_config_patches.values():
        pending.append((declaration, relative, base))
    while pending:
        entry = pending.pop()
        if isinstance(entry, tuple):
            declaration, relative, base = entry
            dependency = _resolve_local_path(relative, base, declaration, boundary)
            dependency_manifest = dependency / "Cargo.toml"
            if not dependency.is_dir() or not dependency_manifest.is_file():
                raise ValueError(f"missing local path in {declaration}: {relative} -> {dependency}")
            repositories.add(_repository_root(dependency, boundary))
            pending.append(dependency_manifest)
            continue
        current = entry.resolve()
        if current in scanned:
            continue
        scanned.add(current)
        try:
            data = tomllib.loads(current.read_text())
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise ValueError(f"cannot read local dependency manifest {current}: {error}") from error
        workspace_root = _workspace_root(current, data)
        workspace_data = None
        if workspace_root is not None:
            root_manifest = workspace_root / "Cargo.toml"
            if root_manifest == current:
                workspace_data = data
            else:
                try:
                    workspace_data = tomllib.loads(root_manifest.read_text())
                except (OSError, tomllib.TOMLDecodeError) as error:
                    raise ValueError(f"cannot read workspace manifest {root_manifest}: {error}") from error
                # Root dependencies and patch declarations can themselves
                # contribute paths. Only the member's inherited dependencies
                # are selected from workspace.dependencies below.
                for relative in _manifest_paths({"patch": workspace_data.get("patch", {})}):
                    pending.append((root_manifest, relative, workspace_root))
        paths = [(relative, current.parent, current) for relative in _manifest_paths(data)]
        for relative, base, declaration in paths:
            pending.append((declaration, relative, base))
        if workspace_data is not None:
            pending.extend((workspace_root / "Cargo.toml", relative, workspace_root)
                           for relative in _inherited_workspace_paths(data, workspace_data))
    return sorted(repositories)


def _resolve_local_path(relative, base, declaration, boundary):
    if Path(relative).is_absolute():
        raise ValueError(
            f"absolute local path in {declaration}: {relative}; use a path relative "
            "to the declaring manifest or Cargo config so it remains valid in the source snapshot"
        )
    dependency = (base / relative).resolve()
    try:
        dependency.relative_to(boundary)
    except ValueError as error:
        raise ValueError(f"local path escapes sibling root in {declaration}: {relative} -> {dependency}") from error
    return dependency


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
        copied_sources = {core: []}
        for label, source in inputs:
            snapshot = temp_root / "sources" / label
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            copy_tracked_source(source, snapshot)
            if not (snapshot / "Cargo.toml").is_file():
                raise ValueError(f"tracked source snapshot is missing Cargo.toml: {label}")
            snapshots[label] = snapshot
            copied_sources.setdefault(source.resolve(), []).append(snapshot)
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
                copied_sources.setdefault(repository.resolve(), []).append(snapshot)
            snapshots["execution-services"] = (
                temp_root / "sources" / "siblings" /
                execution_source.relative_to(sibling_root))
        for source, destinations in sorted(copied_sources.items()):
            revision, changes = git_revision_and_tracked_changes(source)
            if destinations:
                print(f"source: revision={revision} root={source} snapshot={destinations[0]} "
                      f"additional_snapshots={destinations[1:]} "
                      f"tracked_changes={changes!r}", flush=True)
            else:
                print(f"core: revision={revision} root={source} "
                      f"tracked_changes={changes!r}", flush=True)
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
