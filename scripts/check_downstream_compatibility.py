#!/usr/bin/env python3
"""Validate downstream projects from tracked-source snapshots."""

import argparse
from contextvars import ContextVar
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
    from scripts.downstream_source_snapshot import is_tracked_snapshot_file
    from scripts.downstream_source_snapshot import validate_tracked_config_snapshot_file
    from scripts.downstream_source_snapshot import TrackedSourceView
    from scripts.downstream_source_snapshot import tracked_snapshot_directory_identity
    from scripts.downstream_source_snapshot import validate_tracked_snapshot_file
    from scripts.run_conformance_watchdog import process_group_options
    from scripts.run_conformance_watchdog import terminate_process_group
except ModuleNotFoundError:
    from check_event_bus_graph import validate_graph_for_downstream
    from downstream_source_snapshot import copy_tracked_source
    from downstream_source_snapshot import is_tracked_snapshot_file
    from downstream_source_snapshot import validate_tracked_config_snapshot_file
    from downstream_source_snapshot import TrackedSourceView
    from downstream_source_snapshot import tracked_snapshot_directory_identity
    from downstream_source_snapshot import validate_tracked_snapshot_file
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


def _normalize_patch_registry(registry):
    """Map Cargo's two equivalent crates.io patch table names to one identity."""
    if registry in ("crates-io", "https://github.com/rust-lang/crates.io-index"):
        return "crates-io"
    return registry


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
    """Yield Cargo's selected dependency table per category and scope."""
    categories = (("dependencies",), ("build-dependencies", "build_dependencies"))
    if include_dev:
        categories = (("dependencies",), ("dev-dependencies", "dev_dependencies"),
                      ("build-dependencies", "build_dependencies"))
    for scope in (data, *data.get("target", {}).values()):
        for aliases in categories:
            for name in aliases:
                if name in scope:
                    yield scope[name]
                    break


_TRACKED_VIEWS = ContextVar("tracked_source_views", default=None)
_WORKSPACE_MEMBER_CACHE = ContextVar("workspace_member_cache", default=None)


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
    return _tracked_view(root).tracked


def _tracked_view(root):
    root = Path(root).resolve()
    views = _TRACKED_VIEWS.get()
    if views is None:
        return TrackedSourceView(root)
    if root not in views:
        views[root] = TrackedSourceView(root)
    return views[root]


def _workspace_cache():
    cache = _WORKSPACE_MEMBER_CACHE.get()
    return cache if cache is not None else {}


def _tracked_file(path):
    # Establish ownership from the declaration's lexical location, then check
    # both that declaration and its resolved target against the Git index.
    path = Path(os.path.abspath(path))
    root = None
    for candidate in (path.parent, *path.parent.parents):
        if (candidate / ".git").exists():
            result = subprocess.run(
                ["git", "-C", str(candidate), "rev-parse", "--show-toplevel"],
                capture_output=True, text=True, check=False)
            if result.returncode == 0 and Path(result.stdout.strip()).resolve() == candidate:
                root = candidate
                break
    if root is None:
        return path.is_file()
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    if relative not in _tracked_files(root):
        return False
    if not os.path.lexists(path):
        return False
    try:
        validate_tracked_snapshot_file(root, path, view=_tracked_view(root))
    except (OSError, ValueError) as error:
        raise ValueError(f"invalid tracked file {path}: {error}") from error
    return True


def _validate_manifest_path(path):
    """Validate a manifest link against the repository owning its declaration."""
    declaration = Path(os.path.abspath(path))
    repository = _repo_for(declaration.parent)
    if repository is None:
        if not declaration.is_file():
            raise FileNotFoundError(f"manifest is not present in tracked source view: {declaration}")
        return
    if not os.path.lexists(declaration):
        raise FileNotFoundError(
            f"manifest declaration is not present in tracked source view: {declaration} "
            f"(repository boundary: {repository})")
    try:
        validate_tracked_snapshot_file(repository, declaration, view=_tracked_view(repository))
    except (OSError, ValueError, RuntimeError) as error:
        try:
            attempted_target = declaration.resolve(strict=True)
        except (OSError, RuntimeError):
            attempted_target = "<unresolved>"
        raise ValueError(
            f"manifest declaration is not present in tracked source view: {declaration} "
            f"-> {attempted_target} (repository boundary: {repository}): {error}") from error
    try:
        target = declaration.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValueError(f"cannot resolve manifest declaration {declaration}: {error}") from error
    try:
        target.relative_to(repository)
    except ValueError as error:
        raise ValueError(
            f"manifest declaration escapes owning repository: {declaration} -> {target} "
            f"(repository boundary: {repository})") from error


def _read_manifest(path):
    _validate_manifest_path(path)
    return Path(path).read_text()


def _workspace_root(manifest, data, sibling_boundary=None):
    """Find the workspace Cargo uses for this package, respecting nested roots."""
    package = data.get("package", {})
    if isinstance(package.get("workspace"), str):
        declared = package["workspace"]
        if Path(declared).is_absolute():
            raise ValueError(
                f"absolute package.workspace in {manifest}: {declared}")
        return (manifest.parent / declared).resolve()
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
            members = _workspace_member_manifests(parent, root_data, sibling_boundary)
            return parent if Path(os.path.abspath(manifest.parent)) in {
                Path(os.path.abspath(item.parent)) for item in members} else None
    return None


def _glob_matches(path, pattern):
    import re
    pattern = pattern.strip("/")
    while pattern.startswith("./"):
        pattern = pattern[2:]
    # Cargo's ** segment may match zero directories; ordinary * and ? stay
    # within one path segment, while [abc] and [a-z] are character classes.
    segments = pattern.split("/")
    expression = []
    for index, segment in enumerate(segments):
        if segment == "**":
            if index and segments[index - 1] != "**":
                expression.append("/")
            if index == len(segments) - 1:
                expression.append("(?:[^/]+(?:/|$))*")
            else:
                expression.append("(?:[^/]+/)*")
            continue
        if index and segments[index - 1] != "**":
            expression.append("/")
        part = []
        cursor = 0
        while cursor < len(segment):
            char = segment[cursor]
            if char == "*":
                part.append("[^/]*")
            elif char == "?":
                part.append("[^/]")
            elif char == "[":
                close = segment.find("]", cursor + 1)
                if close >= 0:
                    chars = segment[cursor + 1:close]
                    if chars.startswith("!"):
                        chars = "^" + chars[1:]
                    part.append("[" + chars + "]")
                    cursor = close
                else:
                    part.append(r"\[")
            else:
                part.append(re.escape(char))
            cursor += 1
        expression.append("".join(part))
    return re.fullmatch("".join(expression), path) is not None


def _literal_workspace_path(pattern):
    return Path(pattern).as_posix().removeprefix("./").rstrip("/")


def _workspace_glob_manifests(root, pattern, sibling_boundary=None):
    """Expand member globs over Cargo manifests visible in the tracked snapshot."""
    root = Path(root).resolve()
    repository = _repo_for(root)
    if repository is None:
        return []
    boundary = (Path(sibling_boundary).resolve() if sibling_boundary is not None
                else repository.parent.resolve())
    path = Path(pattern)
    if path.is_absolute():
        raise ValueError(
            f"absolute workspace member declared in {root / 'Cargo.toml'}: {pattern}; "
            "use a path relative to the workspace root")
    pattern_parts = Path(pattern).parts
    fixed_parts = []
    for part in pattern_parts:
        if any(marker in part for marker in "*?["):
            break
        fixed_parts.append(part)
    declared_prefix = root.joinpath(*fixed_parts)
    current = root
    for component in fixed_parts:
        if component in ("", "."):
            continue
        if component == "..":
            current = current.parent
        else:
            current = current / component
        if current.is_symlink():
            target = current.resolve()
            if target.is_dir() and _repo_for(target) == target:
                raise ValueError(
                    f"workspace member glob declared in {root / 'Cargo.toml'} uses directory alias "
                    f"{pattern}: {current} -> {target}; use the real checkout's relative path")
    fixed_prefix = declared_prefix.resolve()
    try:
        fixed_prefix.relative_to(boundary)
    except ValueError as error:
        raise ValueError(
            f"workspace member glob declared in {root / 'Cargo.toml'} escapes sibling boundary: "
            f"{pattern} -> {fixed_prefix}") from error
    # Cargo permits a wildcard in the sibling repository name itself, for
    # example ../vendor-*/leaf-*. Enumerate only immediate Git checkouts in
    # that sibling boundary; never recursively search outside it.
    sibling_name_pattern = (pattern_parts[len(fixed_parts)]
                            if fixed_prefix == boundary and
                            len(pattern_parts) > len(fixed_parts) and
                            any(marker in pattern_parts[len(fixed_parts)] for marker in "*?[")
                            else None)
    if sibling_name_pattern is not None:
        candidate_repositories = []
        for candidate in boundary.iterdir():
            if not _glob_matches(candidate.name, sibling_name_pattern):
                continue
            if candidate.is_symlink() and _repo_for(candidate) == candidate.resolve():
                raise ValueError(
                    f"workspace member glob declared in {root / 'Cargo.toml'} uses directory alias "
                    f"{pattern}: {candidate} -> {candidate.resolve()}; use the real checkout's relative path")
            candidate_repository = _repo_for(candidate)
            if candidate_repository is None or candidate_repository != candidate.resolve():
                continue
            try:
                candidate_repository.relative_to(boundary)
            except ValueError as error:
                raise ValueError(
                    f"workspace member glob declared in {root / 'Cargo.toml'} resolves outside "
                    f"sibling boundary: {pattern} -> {candidate_repository}") from error
            candidate_repositories.append(candidate_repository)
    else:
        candidate_repository = _repo_for(fixed_prefix)
        if candidate_repository is None:
            candidate_repository = repository
        candidate_repository = candidate_repository.resolve()
        try:
            candidate_repository.relative_to(boundary)
        except ValueError as error:
            raise ValueError(
                f"workspace member glob declared in {root / 'Cargo.toml'} escapes sibling boundary: "
                f"{pattern} -> {candidate_repository}") from error
        candidate_repositories = [candidate_repository]
    result = []
    for candidate_repository in candidate_repositories:
        for relative in _tracked_files(candidate_repository):
            if relative.name != "Cargo.toml":
                continue
            manifest = candidate_repository / relative
            package_dir = Path(os.path.relpath(manifest.parent, root)).as_posix()
            if package_dir == ".":
                package_dir = ""
            if not (_glob_matches(package_dir, pattern) or
                    _glob_matches((package_dir + "/Cargo.toml").lstrip("/"), pattern)):
                continue
            resolved_manifest = manifest.resolve()
            try:
                resolved_manifest.relative_to(boundary)
            except ValueError as error:
                raise ValueError(
                    f"workspace member glob declared in {root / 'Cargo.toml'} resolves outside "
                    f"sibling boundary: {pattern} -> {resolved_manifest}") from error
            resolved_directory = manifest.parent.resolve()
            try:
                resolved_directory.relative_to(boundary)
            except ValueError as error:
                raise ValueError(
                    f"workspace member glob declared in {root / 'Cargo.toml'} resolves outside "
                    f"sibling boundary: {pattern} -> {resolved_directory}") from error
            # The index still lists a manifest deleted from the worktree;
            # Cargo's glob sees the current filesystem and omits that member.
            if not _tracked_file(manifest):
                continue
            result.append(manifest)
    return result


def _is_workspace_member(package_dir, root, root_data, *, tracked=None,
                          glob_manifests=None):
    workspace = root_data["workspace"]
    try:
        relative = package_dir.relative_to(root).as_posix()
    except ValueError:
        relative = Path(os.path.relpath(package_dir, root)).as_posix()
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
    normalized = Path(relative).as_posix().removeprefix("./")
    if any(normalized == _literal_workspace_path(pattern) or
           normalized.startswith(_literal_workspace_path(pattern).rstrip("/") + "/")
           for pattern in excludes):
        return False
    explicit = False
    for member in workspace.get("members", []):
        if _glob_matches(relative, member):
            explicit = True
            break
        manifests = (glob_manifests or {}).get(member)
        if manifests is not None and any(Path(os.path.abspath(candidate.parent)) == package_dir
                                         for candidate in manifests):
            explicit = True
        if explicit:
            break
    if explicit:
        if package_dir == root or package_dir.is_relative_to(root):
            return True
        try:
            package_data = __import__("tomllib").loads(_read_manifest(nested_manifest))
        except (OSError, __import__("tomllib").TOMLDecodeError):
            return False
        package_workspace = package_data.get("package", {}).get("workspace")
        return ("workspace" not in package_data and isinstance(package_workspace, str)
                and (package_dir / package_workspace).resolve() == root.resolve())

    # Cargo also makes in-tree path dependencies implicit workspace members.
    # Follow paths only from explicit/root members, and stop at nested workspace
    # roots so unrelated fixture packages are not pulled in accidentally.
    tomllib = __import__("tomllib")
    members = {root.resolve()} if "package" in root_data else set()
    for member in workspace.get("members", []):
        for candidate_manifest in (glob_manifests or {}).get(member, []):
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
    view = _tracked_view(source_root)
    tracked_files = view.tracked
    current = manifest.parent.resolve()
    configs = []
    while True:
        if current == source_root or source_root in current.parents:
            for name in ("config", "config.toml"):
                candidate = current / ".cargo" / name
                if candidate.relative_to(source_root) in tracked_files:
                    if validate_tracked_config_snapshot_file(
                            source_root, candidate, view=view) is not None:
                        configs.append(candidate)
                        break
        if current == source_root or current.parent == current:
            break
        current = current.parent
    return configs


def _cargo_config_sources(config_path, source_root, *, stack=(), view=None):
    """Expand tracked, repository-local Cargo config includes before their parent."""
    config_path = Path(config_path)
    source_root = Path(source_root).resolve()
    if not config_path.is_absolute():
        config_path = source_root / config_path
    try:
        config_path.relative_to(source_root)
    except ValueError as error:
        raise ValueError(f"Cargo config is outside its source repository: {config_path}") from error
    view = view or _tracked_view(source_root)
    tracked = view.tracked
    try:
        visible_config = validate_tracked_config_snapshot_file(source_root, config_path, view=view)
        if visible_config is None:
            raise FileNotFoundError(f"Cargo config is not present in tracked source view: {config_path}")
    except FileNotFoundError:
        raise
    except (OSError, ValueError) as error:
        raise ValueError(f"Cargo config is not snapshot-safe: {config_path}: {error}") from error
    try:
        declaration_dir = tracked_snapshot_directory_identity(source_root, config_path, view=view)
    except (OSError, ValueError) as error:
        raise ValueError(f"Cargo config declaration directory is not snapshot-safe: {config_path}: {error}") from error
    stack_entry = (visible_config, config_path, declaration_dir)
    identity = (visible_config, declaration_dir)
    if any((entry[0], entry[2]) == identity for entry in stack):
        chain = " -> ".join(map(lambda entry: str(entry[1]), (*stack, stack_entry)))
        raise ValueError(f"Cargo config include cycle: {chain}")
    import tomllib
    try:
        data = tomllib.loads(_read_manifest(visible_config))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot read Cargo config {config_path}: {error}") from error
    includes = data.get("include", [])
    if not isinstance(includes, list):
        raise ValueError(
            f"invalid Cargo config field 'include' in {config_path}: {includes!r}; "
            "Cargo config include must be a list, for example include = [\"common.toml\"]")
    sources = []
    for index, include in enumerate(includes):
        optional = False
        if isinstance(include, dict):
            optional = include.get("optional", False)
            if not isinstance(optional, bool):
                raise ValueError(
                    f"invalid Cargo config field 'optional' in {config_path} "
                    f"include[{index}].optional: expected a boolean, got {optional!r}")
            include = include.get("path")
        if not isinstance(include, str):
            raise ValueError(f"invalid Cargo config include in {config_path}: {include!r}")
        declared = Path(include)
        if declared.is_absolute():
            raise ValueError(f"absolute Cargo config include in {config_path}: {include}")
        if declared.suffix != ".toml":
            raise ValueError(
                f"invalid Cargo config include extension in {config_path} "
                f"include[{index}]: {include!r}; expected a .toml file")
        included = config_path.parent / declared
        try:
            included.relative_to(source_root)
        except ValueError as error:
            raise ValueError(
                f"Cargo config include escapes source repository in {config_path}: {include}") from error
        try:
            visible_include = validate_tracked_config_snapshot_file(
                source_root, included, view=view,
                require_directory=include.endswith(("/", "/.")))
        except (OSError, ValueError) as error:
            reason = str(error).replace(
                "link escapes owning repository", "link escapes source repository")
            raise ValueError(
                f"Cargo config include is not snapshot-safe in {config_path}: {include}: {reason}") from error
        if visible_include is None and optional:
            continue
        if visible_include is None:
            if included.exists() and not include.endswith(("/", "/.")):
                raise ValueError(
                    f"Cargo config include {include} in {config_path} is not present in "
                    "tracked source view")
            raise ValueError(
                f"required Cargo config include missing in {config_path}: {include}")
        try:
            sources.extend(_cargo_config_sources(
                included, source_root, stack=(*stack, stack_entry), view=view))
        except FileNotFoundError as error:
            if optional:
                continue
            raise ValueError(
                f"required Cargo config include missing in {config_path}: {include}") from error
    sources.append((config_path, visible_config))
    return sources


def _config_patch_declarations(config_path, data):
    """Return configured patches keyed by registry and package name."""
    for registry, packages in data.get("patch", {}).items():
        if isinstance(packages, dict):
            for name, package in packages.items():
                if isinstance(package, dict):
                    yield (_normalize_patch_registry(registry), name), package


def _manifest_patch_declarations(manifest, data):
    for registry, packages in data.get("patch", {}).items():
        if isinstance(packages, dict):
            for name, package in packages.items():
                if isinstance(package, dict):
                    yield (_normalize_patch_registry(registry), name), package.get("path")


def _workspace_member_manifests(root, root_data, sibling_boundary=None):
    """Find package manifests Cargo includes in the selected workspace."""
    root = Path(os.path.abspath(root))
    repository = _repo_for(root)
    sibling_boundary = (Path(sibling_boundary).resolve() if sibling_boundary is not None else
                        repository.parent.resolve() if repository is not None else root.parent.resolve())
    cache_key = (root, sibling_boundary)
    workspace_cache = _workspace_cache()
    cached = workspace_cache.get(cache_key)
    if cached is not None:
        return list(cached)
    if "workspace" not in root_data:
        result = [root / "Cargo.toml"] if "package" in root_data else []
        workspace_cache[cache_key] = result
        return list(result)
    workspace = root_data["workspace"]
    root_manifest = root / "Cargo.toml"
    for excluded in workspace.get("exclude", []):
        if Path(excluded).is_absolute():
            raise ValueError(
                f"absolute workspace exclude declared in {root_manifest}: {excluded}; "
                "use a path relative to the workspace root")
    excludes = [_literal_workspace_path(pattern)
                for pattern in workspace.get("exclude", [])]
    explicit_manifests = []
    glob_manifests = {}
    for member in workspace.get("members", []):
        if Path(member).is_absolute():
            raise ValueError(
                f"absolute workspace member declared in {root_manifest}: {member}; "
                "use a path relative to the workspace root")
    for member in workspace.get("members", []):
        if any(marker in member for marker in "*?["):
            glob_manifests[member] = _workspace_glob_manifests(
                root, member, sibling_boundary)
            continue
        relative = _literal_workspace_path(member)
        if not relative or any(relative == excluded or
                               relative.startswith(excluded.rstrip("/") + "/")
                               for excluded in excludes):
            continue
        candidate = Path(os.path.abspath(root / relative))
        resolved_candidate = candidate.resolve()
        try:
            resolved_candidate.relative_to(sibling_boundary)
        except ValueError as error:
            raise ValueError(
                f"workspace member declared in {root_manifest} escapes sibling boundary: "
                f"{member} -> {resolved_candidate}") from error
        member_manifest = candidate if candidate.name == "Cargo.toml" else candidate / "Cargo.toml"
        try:
            _validate_manifest_path(member_manifest)
        except (OSError, ValueError) as error:
            raise ValueError(
                f"workspace member declared in {root_manifest}: {member} -> {member_manifest}: "
                f"{error}") from error
        explicit_manifests.append(member_manifest)
        glob_manifests[member] = [member_manifest]
    if repository is None:
        result = []
        for current, directories, files in os.walk(root):
            directories[:] = [name for name in directories if name not in {".git", "target"}]
            if "Cargo.toml" in files and _is_workspace_member(
                    Path(current), root, root_data, glob_manifests=glob_manifests):
                result.append(Path(current) / "Cargo.toml")
        result.extend(manifest for manifest in explicit_manifests if manifest not in result)
        _workspace_cache()[cache_key] = result
        return list(result)
    tracked_manifests = [repository / relative for relative in _tracked_files(repository)
                         if relative.name == "Cargo.toml" and
                         (repository / relative).parent.is_relative_to(root)]
    for manifests in glob_manifests.values():
        tracked_manifests.extend(manifests)
    # Explicit sibling members were validated before tracked-file access.
    tracked_manifests.extend(manifest for manifest in explicit_manifests
                             if manifest not in tracked_manifests)
    by_directory = {Path(os.path.abspath(manifest.parent)): manifest
                    for manifest in tracked_manifests}

    def eligible(directory):
        directory = Path(os.path.abspath(directory))
        manifest = by_directory.get(directory)
        if manifest is None:
            return False
        return _is_workspace_member(directory, root, root_data,
                                    glob_manifests=glob_manifests)

    members = {root} if "package" in root_data else set()
    for manifests in glob_manifests.values():
        for manifest in manifests:
            if eligible(manifest.parent.resolve()):
                members.add(Path(os.path.abspath(manifest.parent)))
    for manifest in explicit_manifests:
            if eligible(manifest.parent):
                members.add(Path(os.path.abspath(manifest.parent)))

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
            dependency = Path(os.path.abspath(base / relative))
            if dependency not in members and eligible(dependency):
                members.add(dependency)
                pending.append(dependency)
    result = [by_directory[member] for member in sorted(members) if member in by_directory]
    _workspace_cache()[cache_key] = result
    return list(result)


def _repository_root(path, boundary):
    path = path.resolve()
    for candidate in (path, *path.parents):
        if candidate == boundary.parent:
            break
        if (candidate / ".git").exists():
            return candidate
    return path


def _discover_local_repositories(manifest, sibling_root, source_root=None, *,
                                 cli_patches=None):
    """Find sibling repositories required by an IoC fixture's local paths and patches."""
    # Keep the declared manifest path: Cargo resolves relative declarations
    # from this package directory even when Cargo.toml is an internal link.
    manifest = Path(os.path.abspath(manifest))
    boundary = Path(sibling_root).resolve()
    source_root = Path(source_root).resolve() if source_root else manifest.parents[3]
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
    initial_workspace = _workspace_root(manifest, initial_data, boundary)
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
    expanded_config_paths = []
    for config_path in reversed(config_paths):
        expanded_config_paths.extend(_cargo_config_sources(config_path, source_root))
    for config_source in expanded_config_paths:
        if isinstance(config_source, tuple):
            config_path, physical_config_path = config_source
        else:
            # Compatibility with callers that replace the expansion helper.
            config_path = Path(config_source)
            physical_config_path = validate_tracked_config_snapshot_file(
                source_root, config_path, view=_tracked_view(source_root))
            if physical_config_path is None:
                continue
        try:
            config_data = tomllib.loads(_read_manifest(physical_config_path))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise ValueError(f"cannot read Cargo config {config_path}: {error}") from error
        # Cargo merges patch package tables by key and field. A closer config
        # that only adds version metadata does not erase a farther path.
        for key, package in _config_patch_declarations(config_path, config_data):
            relative = package.get("path")
            if isinstance(relative, str):
                effective_config_patches[key] = (
                    config_path, relative, config_path.parent.parent, True)
            elif any(field in package for field in ("git", "registry", "registry-index")):
                effective_config_patches[key] = (
                    config_path, None, config_path.parent.parent, False)
    effective_patches.update(effective_config_patches)
    normalized_cli_patches = {
        (_normalize_patch_registry(registry), name): relative
        for (registry, name), relative in (cli_patches or {}).items()
    }
    for registry_name, relative in normalized_cli_patches.items():
        effective_patches[registry_name] = (manifest, str(relative), Path(relative).parent, True)
    cli_keys = set(normalized_cli_patches)
    for key, (declaration, relative, base, is_local_path) in effective_patches.items():
        # The core path is selected on the command line and validated by the
        # caller as the requested source checkout, outside sibling discovery.
        if key == ("crates-io", "qubit-event-bus") and key in cli_keys:
            continue
        if not is_local_path:
            continue
        pending_paths.append((declaration, relative, base))
    workspace_member_manifests = _workspace_member_manifests(
        initial_root.parent, initial_root_data, boundary)
    workspace_members = {Path(os.path.abspath(member_manifest))
                         for member_manifest in workspace_member_manifests}
    for member_manifest in workspace_members:
        repositories.add(_repository_root(member_manifest.parent, boundary))
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
            selected_member = Path(os.path.abspath(dependency_manifest)) in workspace_members
            pending_manifests.append((dependency_manifest, selected_member))
            continue
        entry, selected_member = pending_manifests.pop()
        # Preserve the package's declared directory when Cargo.toml is an
        # internal link; relative path dependencies are based there.
        current = Path(os.path.abspath(entry))
        if current in scanned:
            continue
        scanned.add(current)
        try:
            data = tomllib.loads(_read_manifest(current))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise ValueError(f"cannot read local dependency manifest {current}: {error}") from error
        workspace_root = _workspace_root(current, data, boundary)
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


def discover_local_repositories(manifest, sibling_root, source_root=None, *,
                                cli_patches=None):
    """Discover repositories with tracked views scoped to this call."""
    view_token = _TRACKED_VIEWS.set({})
    workspace_token = _WORKSPACE_MEMBER_CACHE.set({})
    try:
        return _discover_local_repositories(
            manifest, sibling_root, source_root, cli_patches=cli_patches)
    finally:
        _WORKSPACE_MEMBER_CACHE.reset(workspace_token)
        _TRACKED_VIEWS.reset(view_token)


def _resolve_local_path(relative, base, declaration, boundary):
    if Path(relative).is_absolute():
        raise ValueError(
            f"absolute local path in {declaration}: {relative}; use a path relative "
            "to the declaring manifest or Cargo config so it remains valid in the source snapshot"
        )
    dependency = Path(os.path.abspath(base / relative))
    resolved = dependency.resolve()
    try:
        resolved.relative_to(boundary)
    except ValueError as error:
        raise ValueError(f"local path escapes sibling root in {declaration}: {relative} -> {resolved}") from error
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
