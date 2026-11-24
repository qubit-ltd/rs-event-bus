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
    from scripts.downstream_source_snapshot import tracked_source_files
    from scripts.run_conformance_watchdog import process_group_options
    from scripts.run_conformance_watchdog import terminate_process_group
except ModuleNotFoundError:
    from check_event_bus_graph import validate_graph_for_downstream
    from downstream_source_snapshot import copy_tracked_source
    from downstream_source_snapshot import tracked_source_files
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


def _cargo_config_patch_declarations(core, redis=None):
    patches = [("crates-io", "qubit-event-bus", Path(core))]
    if redis is not None:
        patches.append(("crates-io", "qubit-event-bus-redis", Path(redis)))
    return patches


def cargo_config(core, redis=None):
    configs = [f"patch.{registry}.{name}.path=" + json.dumps(str(path))
               for registry, name, path in _cargo_config_patch_declarations(core, redis)]
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
    """Extract only path fields from dependency declarations, never table keys."""
    if isinstance(value, dict) and isinstance(value.get("path"), str):
        yield value["path"]


def _manifest_paths(data, *, include_dev=True):
    for dependencies in _dependency_sections(data, include_dev=include_dev):
        if isinstance(dependencies, dict):
            for dependency in dependencies.values():
                yield from _path_entries(dependency)


def _dependency_sections(data, *, include_dev=True):
    """Yield dependency tables from normal, target-specific, and workspace scope."""
    sections = ("dependencies", "dev-dependencies", "build-dependencies") if include_dev else (
        "dependencies", "build-dependencies")
    names = set(sections) | ({"dev_dependencies", "build_dependencies"} if include_dev else set())
    for section in names:
        yield data.get(section, {})
    for target in data.get("target", {}).values():
        for section in names:
            yield target.get(section, {})


_TRACKED_FILES = {}
_WORKSPACE_MEMBER_CACHE = {}


def _repo_for(path):
    path = Path(path).resolve()
    for candidate in (path, *path.parents):
        if (candidate / ".git").exists():
            result = subprocess.run(
                ["git", "-C", str(candidate), "rev-parse", "--show-toplevel"],
                capture_output=True, text=True, check=False)
            if result.returncode == 0 and Path(result.stdout.strip()).resolve() == candidate:
                return candidate
    return None


def _tracked_files(root):
    root = Path(root).resolve()
    if root not in _TRACKED_FILES:
        _TRACKED_FILES[root] = set(tracked_source_files(root))
    return _TRACKED_FILES[root]


def _tracked_file(path):
    path = Path(path).resolve()
    root = _repo_for(path)
    if root is None:
        return path.is_file()
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return relative in _tracked_files(root) and path.is_file()


def _read_manifest(path):
    if not _tracked_file(path):
        raise FileNotFoundError(f"manifest is not present in tracked source view: {path}")
    return Path(path).read_text()


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
        if not _tracked_file(root_manifest):
            continue
        try:
            root_data = tomllib.loads(_read_manifest(root_manifest))
        except (OSError, tomllib.TOMLDecodeError):
            continue
        if "workspace" in root_data:
            members = _workspace_member_manifests(parent, root_data)
            return parent if manifest.parent.resolve() in {item.parent.resolve()
                                                           for item in members} else None
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


def _literal_workspace_path(pattern):
    return Path(pattern).as_posix().removeprefix("./").rstrip("/")


def _workspace_glob_manifests(root, pattern):
    """Expand member globs over Cargo manifests visible in the tracked snapshot."""
    root = Path(root).resolve()
    repository = _repo_for(root)
    if repository is None:
        return []
    result = []
    for relative in _tracked_files(repository):
        if relative.name != "Cargo.toml":
            continue
        manifest = repository / relative
        try:
            package_dir = manifest.parent.relative_to(root).as_posix()
        except ValueError:
            continue
        if package_dir == ".":
            package_dir = ""
        if (_glob_matches(package_dir, pattern) or
                _glob_matches((package_dir + "/Cargo.toml").lstrip("/"), pattern)):
            result.append(manifest)
    return result


def _is_workspace_member(package_dir, root, root_data, *, tracked=None):
    workspace = root_data["workspace"]
    try:
        relative = package_dir.relative_to(root).as_posix()
    except ValueError:
        return False
    nested_manifest = package_dir / "Cargo.toml"
    if package_dir != root and _tracked_file(nested_manifest):
        tomllib = __import__("tomllib")
        try:
            nested_data = tomllib.loads(_read_manifest(nested_manifest))
        except (OSError, tomllib.TOMLDecodeError):
            nested_data = {}
        if "workspace" in nested_data:
            return False
        package_workspace = nested_data.get("package", {}).get("workspace")
        if isinstance(package_workspace, str) and \
                (package_dir / package_workspace).resolve() != root.resolve():
            return False
    excludes = workspace.get("exclude", [])
    normalized = Path(relative).as_posix().lstrip("./")
    if any(normalized == _literal_workspace_path(pattern) or
           normalized.startswith(_literal_workspace_path(pattern).rstrip("/") + "/")
           for pattern in excludes):
        return False
    explicit = False
    for member in workspace.get("members", []):
        if _glob_matches(relative, member):
            explicit = True
            break
        try:
            for candidate_manifest in _workspace_glob_manifests(root, member):
                if candidate_manifest.parent.resolve() == package_dir:
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
        for candidate_manifest in _workspace_glob_manifests(root, member):
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
            current_data = tomllib.loads(_read_manifest(current_manifest))
        except (OSError, tomllib.TOMLDecodeError):
            continue
        paths = [(relative, current) for relative in _manifest_paths(current_data)]
        if "workspace" in current_data:
            paths.extend((relative, current)
                         for relative in _inherited_workspace_paths(current_data, current_data))
        else:
            paths.extend((relative, root)
                         for relative in _inherited_workspace_paths(current_data, root_data))
        for relative, base in paths:
            if Path(relative).is_absolute():
                continue
            dependency = (base / relative).resolve()
            try:
                dependency.relative_to(root.resolve())
            except ValueError:
                continue
            dependency_manifest = dependency / "Cargo.toml"
            if not _tracked_file(dependency_manifest):
                continue
            try:
                dependency_data = tomllib.loads(_read_manifest(dependency_manifest))
            except (OSError, tomllib.TOMLDecodeError):
                continue
            if "workspace" in dependency_data:
                continue
            dep_rel = dependency.relative_to(root).as_posix()
            if any(dep_rel == _literal_workspace_path(pattern) or
                   dep_rel.startswith(_literal_workspace_path(pattern).rstrip("/") + "/")
                   for pattern in excludes):
                continue
            if dependency == package_dir:
                return True
            pending.append(dependency)
    return False


def _inherited_workspace_paths(data, workspace_data, *, include_dev=True):
    workspace_dependencies = workspace_data.get("workspace", {}).get("dependencies", {})
    used = set()
    for section in _dependency_sections(data, include_dev=include_dev):
        for name, dependency in section.items():
            if isinstance(dependency, dict) and dependency.get("workspace") is True:
                used.add(name)
    for name in sorted(used):
        dependency = workspace_dependencies.get(name)
        if dependency is None:
            continue
        yield from _path_entries(dependency)


def _cargo_config_paths(manifest, source_root):
    """Find tracked Cargo configs discovered from cwd and copied into snapshots."""
    source_root = source_root.resolve()
    if not (source_root / ".git").exists():
        return []
    tracked_files = _tracked_files(source_root)
    current = manifest.parent.resolve()
    configs = []
    while True:
        if current == source_root or source_root in current.parents:
            for name in ("config", "config.toml"):
                candidate = current / ".cargo" / name
                if candidate.is_file() and candidate.relative_to(source_root) in tracked_files:
                    configs.append(candidate)
                    break
        if current == source_root or current.parent == current:
            break
        current = current.parent
    return configs


def _config_patch_declarations(config_path, data):
    """Return configured patches keyed by registry and package name."""
    for registry, packages in data.get("patch", {}).items():
        if isinstance(packages, dict):
            for name, package in packages.items():
                if isinstance(package, dict):
                    yield (registry, name), package.get("path")


def _manifest_patch_declarations(manifest, data):
    for registry, packages in data.get("patch", {}).items():
        if isinstance(packages, dict):
            for name, package in packages.items():
                if isinstance(package, dict):
                    yield (registry, name), package.get("path")


def _workspace_member_manifests(root, root_data):
    """Find package manifests Cargo includes in the selected workspace."""
    root = Path(root).resolve()
    cached = _WORKSPACE_MEMBER_CACHE.get(root)
    if cached is not None:
        return list(cached)
    if "workspace" not in root_data:
        result = [root / "Cargo.toml"] if "package" in root_data else []
        _WORKSPACE_MEMBER_CACHE[root] = result
        return list(result)
    repository = _repo_for(root)
    if repository is None:
        result = []
        for current, directories, files in os.walk(root):
            directories[:] = [name for name in directories if name not in {".git", "target"}]
            if "Cargo.toml" in files and _is_workspace_member(Path(current), root, root_data):
                result.append(Path(current) / "Cargo.toml")
        _WORKSPACE_MEMBER_CACHE[root] = result
        return list(result)
    root_manifest = root / "Cargo.toml"
    excludes = [_literal_workspace_path(pattern)
                for pattern in root_data["workspace"].get("exclude", [])]
    for member in root_data["workspace"].get("members", []):
        if any(marker in member for marker in "*?"):
            continue
        relative = _literal_workspace_path(member)
        if not relative or any(relative == excluded or
                               relative.startswith(excluded.rstrip("/") + "/")
                               for excluded in excludes):
            continue
        candidate = root / relative
        member_manifest = candidate if candidate.name == "Cargo.toml" else candidate / "Cargo.toml"
        if not _tracked_file(member_manifest):
            raise ValueError(
                f"workspace member declared in {root_manifest} is not present in tracked source: "
                f"{member_manifest}")
    tracked_manifests = [repository / relative for relative in _tracked_files(repository)
                         if relative.name == "Cargo.toml" and
                         (repository / relative).parent.is_relative_to(root)]
    by_directory = {manifest.parent.resolve(): manifest for manifest in tracked_manifests}
    workspace = root_data["workspace"]
    excludes = [_literal_workspace_path(pattern) for pattern in workspace.get("exclude", [])]

    def eligible(directory):
        try:
            relative = directory.relative_to(root).as_posix()
        except ValueError:
            return False
        if any(relative == excluded or relative.startswith(excluded.rstrip("/") + "/")
               for excluded in excludes):
            return False
        manifest = by_directory.get(directory.resolve())
        if manifest is None:
            return False
        try:
            package_data = __import__("tomllib").loads(_read_manifest(manifest))
        except (OSError, __import__("tomllib").TOMLDecodeError):
            return False
        if "workspace" in package_data:
            return False
        package_workspace = package_data.get("package", {}).get("workspace")
        return not isinstance(package_workspace, str) or \
            (directory / package_workspace).resolve() == root.resolve()

    members = {root.resolve()} if "package" in root_data else set()
    for member in workspace.get("members", []):
        for manifest in _workspace_glob_manifests(root, member):
            if eligible(manifest.parent.resolve()):
                members.add(manifest.parent.resolve())

    tomllib = __import__("tomllib")
    pending = list(members)
    while pending:
        current = pending.pop()
        manifest = by_directory.get(current)
        if manifest is None:
            continue
        try:
            data = tomllib.loads(_read_manifest(manifest))
        except (OSError, tomllib.TOMLDecodeError):
            continue
        paths = [(relative, current) for relative in _manifest_paths(data)]
        if "workspace" in data:
            paths.extend((relative, current)
                         for relative in _inherited_workspace_paths(data, data))
        else:
            paths.extend((relative, root)
                         for relative in _inherited_workspace_paths(data, root_data))
        for relative, base in paths:
            if Path(relative).is_absolute():
                continue
            dependency = (base / relative).resolve()
            if dependency not in members and eligible(dependency):
                members.add(dependency)
                pending.append(dependency)
    result = [by_directory[member] for member in sorted(members) if member in by_directory]
    _WORKSPACE_MEMBER_CACHE[root] = result
    return list(result)


def _repository_root(path, boundary):
    path = path.resolve()
    for candidate in (path, *path.parents):
        if candidate == boundary.parent:
            break
        if (candidate / ".git").exists():
            return candidate
    return path


def discover_local_repositories(manifest, sibling_root, source_root=None, *,
                                cli_patches=None):
    """Find sibling repositories required by an IoC fixture's local paths and patches."""
    manifest = Path(manifest).resolve()
    boundary = Path(sibling_root).resolve()
    source_root = Path(source_root).resolve() if source_root else manifest.parents[3]
    _TRACKED_FILES.clear()
    _WORKSPACE_MEMBER_CACHE.clear()
    if not manifest.is_file():
        raise ValueError(f"manifest does not exist: {manifest}")
    repositories = {source_root}
    pending_manifests = []
    pending_paths = []
    scanned = set()
    import tomllib

    try:
        initial_data = tomllib.loads(_read_manifest(manifest))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot read local dependency manifest {manifest}: {error}") from error
    initial_workspace = _workspace_root(manifest, initial_data)
    initial_root = initial_workspace / "Cargo.toml" if initial_workspace else manifest
    try:
        initial_root.parent.resolve().relative_to(boundary)
    except ValueError as error:
        raise ValueError(
            f"initial workspace root escapes sibling boundary: {manifest}: {initial_root.parent}") from error
    if initial_workspace is not None:
        repositories.add(_repository_root(initial_root.parent, boundary))
    try:
        initial_root_data = (initial_data if initial_root == manifest else
                             tomllib.loads(_read_manifest(initial_root)))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot read workspace manifest {initial_root}: {error}") from error
    effective_patches = {
        key: (initial_root, relative, initial_root.parent, relative is not None)
        for key, relative in _manifest_patch_declarations(initial_root, initial_root_data)
    }

    config_paths = _cargo_config_paths(manifest, source_root)
    effective_config_patches = {}
    for config_path in config_paths:
        try:
            config_data = tomllib.loads(_read_manifest(config_path))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise ValueError(f"cannot read Cargo config {config_path}: {error}") from error
        # Config files closer to the selected manifest override farther files.
        for key, relative in _config_patch_declarations(config_path, config_data):
            effective_config_patches.setdefault(
                key, (config_path, relative, config_path.parent.parent, relative is not None))
    effective_patches.update(effective_config_patches)
    for registry_name, relative in (cli_patches or {}).items():
        effective_patches[registry_name] = (manifest, str(relative), Path(relative).parent, True)
    for key, (declaration, relative, base, is_local_path) in effective_patches.items():
        # The core path is selected on the command line and validated by the
        # caller as the requested source checkout, outside sibling discovery.
        if key == ("crates-io", "qubit-event-bus") and key in (cli_patches or {}):
            continue
        if not is_local_path:
            continue
        pending_paths.append((declaration, relative, base))
    workspace_members = {
        member_manifest.resolve()
        for member_manifest in _workspace_member_manifests(initial_root.parent, initial_root_data)
    }
    for member_manifest in workspace_members:
        pending_manifests.append((member_manifest, True))
    if not pending_manifests:
        pending_manifests.append((manifest, True))
    scanned = set()
    while pending_paths or pending_manifests:
        if pending_paths:
            declaration, relative, base = pending_paths.pop()
            dependency = _resolve_local_path(relative, base, declaration, boundary)
            dependency_manifest = dependency / "Cargo.toml"
            if not dependency.is_dir() or not dependency_manifest.is_file():
                raise ValueError(f"missing local path in {declaration}: {relative} -> {dependency}")
            repositories.add(_repository_root(dependency, boundary))
            try:
                dependency_data = tomllib.loads(_read_manifest(dependency_manifest))
            except (OSError, tomllib.TOMLDecodeError) as error:
                raise ValueError(
                    f"cannot read local dependency manifest {dependency_manifest} "
                    f"(declared in {declaration}): {error}") from error
            selected_member = dependency_manifest.resolve() in workspace_members
            pending_manifests.append((dependency_manifest, selected_member))
            continue
        entry, selected_member = pending_manifests.pop()
        current = entry.resolve()
        if current in scanned:
            continue
        scanned.add(current)
        try:
            data = tomllib.loads(_read_manifest(current))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise ValueError(f"cannot read local dependency manifest {current}: {error}") from error
        workspace_root = _workspace_root(current, data)
        workspace_data = None
        if workspace_root is not None:
            root_manifest = workspace_root / "Cargo.toml"
            try:
                workspace_root.relative_to(boundary)
            except ValueError as error:
                raise ValueError(
                    f"workspace root escapes sibling boundary: {current}: {workspace_root}") from error
            if root_manifest == current:
                workspace_data = data
            else:
                try:
                    workspace_data = tomllib.loads(_read_manifest(root_manifest))
                except (OSError, tomllib.TOMLDecodeError) as error:
                    raise ValueError(f"cannot read workspace manifest {root_manifest}: {error}") from error
            repositories.add(_repository_root(workspace_root, boundary))
        paths = [(relative, current.parent, current)
                 for relative in _manifest_paths(data, include_dev=selected_member)]
        for relative, base, declaration in paths:
            pending_paths.append((declaration, relative, base))
        if workspace_data is not None:
            pending_paths.extend((current, relative, workspace_root)
                                 for relative in _inherited_workspace_paths(
                                     data, workspace_data, include_dev=selected_member))
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
            fixture_manifest, sibling_root, execution_source,
            cli_patches={(registry, name): str(path)
                         for registry, name, path in
                         _cargo_config_patch_declarations(core)})

    with tempfile.TemporaryDirectory(prefix="event-bus-downstream-") as temp:
        temp_root = Path(temp)
        target_root = temp_root / "target"
        snapshots = {}
        copied_sources = {core: []}
        for label, source in inputs:
            if label == "execution-services":
                continue
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
