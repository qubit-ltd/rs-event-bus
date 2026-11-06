#!/usr/bin/env python3
"""Compile pinned real downstream consumers against the core checkout under test."""

import argparse
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys

from check_event_bus_graph import validate_graph

REPOSITORY_NAMES = (
    "rs-event-bus-redis", "rs-task", "rs-ioc", "rs-execution-services",
    "rs-config", "rs-fs", "rs-fs-local", "rs-fs-registry", "rs-fs-testkit",
    "rs-local-files", "rs-metadata", "rs-model-metadata", "rs-redact", "rs-value",
)
TARGETS = {
    "redis": ("rs-event-bus-redis/Cargo.toml", ["--all-features"]),
    "task_notifications": (
        "rs-event-bus-redis/tests/fixtures/task_notifications/Cargo.toml", []),
    "ioc_application_consumer": (
        "rs-ioc/tests/fixtures/application_consumer_current/Cargo.toml", []),
    "async_tokio_consumer": (
        "rs-event-bus-redis/tests/fixtures/async_tokio_consumer/Cargo.toml", []),
    "async_smol_consumer": (
        "rs-event-bus-redis/tests/fixtures/async_smol_consumer/Cargo.toml", []),
}
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class CompatibilityError(Exception):
    """A configuration, layout, graph, or Cargo command failed."""


def run_command(command):
    """Run an argument-vector command without a shell."""
    return subprocess.run(command, capture_output=True, text=True, check=False)


def load_manifest(path):
    """Load and validate the immutable repository and target contract."""
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CompatibilityError(f"cannot read manifest {path}: {error}") from error
    if not isinstance(config, dict) or config.get("schema_version") != 1:
        raise CompatibilityError("manifest schema_version must be 1")

    repositories = config.get("repositories")
    if not isinstance(repositories, list):
        raise CompatibilityError("manifest repositories must be a list")
    names = []
    for repository in repositories:
        if not isinstance(repository, dict):
            raise CompatibilityError("each repository entry must be an object")
        name = repository.get("name")
        url = repository.get("url")
        revision = repository.get("revision")
        if not isinstance(name, str) or not re.fullmatch(r"rs-[a-z0-9-]+", name):
            raise CompatibilityError(f"invalid repository name: {name!r}")
        if name in names:
            raise CompatibilityError(f"duplicate repository name: {name}")
        names.append(name)
        if url != f"https://github.com/qubit-ltd/{name}.git":
            raise CompatibilityError(f"invalid normalized HTTPS URL for {name}: {url!r}")
        if not isinstance(revision, str) or not SHA_PATTERN.fullmatch(revision):
            raise CompatibilityError(f"invalid 40-character lowercase revision for {name}: {revision!r}")
    if set(names) != set(REPOSITORY_NAMES) or len(names) != len(REPOSITORY_NAMES):
        missing = sorted(set(REPOSITORY_NAMES) - set(names))
        extra = sorted(set(names) - set(REPOSITORY_NAMES))
        raise CompatibilityError(f"repository closure mismatch; missing={missing}, extra={extra}")

    targets = config.get("targets")
    if not isinstance(targets, list):
        raise CompatibilityError("manifest targets must be a list")
    target_names = []
    for target in targets:
        if not isinstance(target, dict):
            raise CompatibilityError("each target entry must be an object")
        name = target.get("name")
        if name not in TARGETS:
            raise CompatibilityError(f"unexpected target name: {name!r}")
        if name in target_names:
            raise CompatibilityError(f"duplicate target name: {name}")
        target_names.append(name)
        expected_path, expected_features = TARGETS[name]
        if target.get("manifest") != expected_path or target.get("features") != expected_features:
            raise CompatibilityError(f"target {name} must use {expected_path} with {expected_features}")
    if set(target_names) != set(TARGETS) or len(target_names) != len(TARGETS):
        raise CompatibilityError("manifest must contain exactly the five fixed downstream targets")
    return config


def checked_path(root, relative):
    """Resolve a root-relative manifest path and reject traversal or symlink escape."""
    if not isinstance(relative, str):
        raise CompatibilityError(f"invalid root-relative path: {relative!r}")
    path = PurePosixPath(relative)
    if path.is_absolute() or not path.parts or ".." in path.parts or "\\" in relative:
        raise CompatibilityError(f"path must stay within the ecosystem root: {relative!r}")
    resolved = root.joinpath(*path.parts).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as error:
        raise CompatibilityError(f"path escapes the ecosystem root: {relative!r}") from error
    return resolved


def validate_layout(root, config):
    """Require every pinned checkout and all requested manifests in the prepared layout."""
    for repository in config["repositories"]:
        directory = checked_path(root, repository["name"])
        if not directory.is_dir() or not ((directory / ".git").is_dir()
                                          or (directory / ".git").is_file()):
            raise CompatibilityError(f"missing checkout for repository {repository['name']}: {directory}")
        result = run_command(["git", "-C", str(directory), "rev-parse", "HEAD"])
        if result.returncode:
            detail = result.stderr or result.stdout
            raise CompatibilityError(f"repository {repository['name']} revision check failed: {detail.strip()}")
        actual = result.stdout.strip()
        if actual != repository["revision"]:
            raise CompatibilityError(
                f"repository {repository['name']} is at {actual}, expected {repository['revision']}")
        status = run_command([
            "git", "--no-optional-locks", "-C", str(directory), "status",
            "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none",
        ])
        if status.returncode:
            detail = status.stderr or status.stdout
            raise CompatibilityError(
                f"repository {repository['name']} worktree check failed: {detail.strip()}")
        if status.stdout:
            raise CompatibilityError(
                f"repository {repository['name']} has local changes; "
                f"fixed-revision checks require a clean downstream checkout:\n{status.stdout}")
    core = checked_path(root, "rs-event-bus")
    if not (core / "Cargo.toml").is_file():
        raise CompatibilityError(f"missing core checkout under test: {core / 'Cargo.toml'}")
    manifests = {}
    for target in config["targets"]:
        manifest = checked_path(root, target["manifest"])
        if not manifest.is_file():
            raise CompatibilityError(f"missing target manifest for {target['name']}: {manifest}")
        manifests[target["name"]] = manifest
    return core, manifests


def execute(root, config):
    """Run locked metadata, graph provenance, and compilation for each target."""
    core, manifests = validate_layout(root, config)
    for target in config["targets"]:
        name = target["name"]
        manifest = manifests[name]
        features = target["features"]
        metadata_command = ["cargo", "metadata", "--locked", *features,
                            "--format-version", "1", "--manifest-path", str(manifest)]
        print(f"downstream compatibility: {name}: metadata", flush=True)
        metadata_result = run_command(metadata_command)
        if metadata_result.returncode:
            detail = metadata_result.stderr or metadata_result.stdout
            raise CompatibilityError(f"{name} metadata failed (exit {metadata_result.returncode}):\n{detail}")
        try:
            graph = json.loads(metadata_result.stdout)
            validate_graph(graph, require_bus=True, expected_core=core)
        except (json.JSONDecodeError, ValueError, KeyError) as error:
            raise CompatibilityError(f"{name} metadata graph validation failed: {error}") from error

        check_command = ["cargo", "check", "--locked", "--all-targets", *features,
                         "--manifest-path", str(manifest)]
        print(f"downstream compatibility: {name}: check", flush=True)
        check_result = run_command(check_command)
        if check_result.returncode:
            detail = check_result.stderr or check_result.stdout
            raise CompatibilityError(f"{name} check failed (exit {check_result.returncode}):\n{detail}")
        print(f"downstream compatibility: {name}: passed", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ecosystem-root", type=Path, required=True,
                        help="prepared same-level core and pinned downstream checkouts")
    parser.add_argument("--manifest", type=Path,
                        help="override the checked-in manifest (for isolated tests)")
    args = parser.parse_args(argv)
    project = Path(__file__).resolve().parents[1]
    manifest_path = args.manifest or project / "scripts" / "downstream-compatibility.json"
    try:
        config = load_manifest(manifest_path)
        execute(args.ecosystem_root.absolute(), config)
    except (CompatibilityError, OSError) as error:
        print(f"downstream compatibility: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    raise SystemExit(0)


if __name__ == "__main__":
    main()
