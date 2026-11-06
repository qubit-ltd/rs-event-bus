#!/usr/bin/env python3
"""Run the fixed behavioral checks for the pinned Redis event bus consumer."""

import argparse
from contextlib import contextmanager
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid

import check_downstream_compatibility as compatibility


COMMAND_TIMEOUT_SECONDS = 600
TERMINATE_GRACE_SECONDS = 5
REDIS_ALIAS = "redis:7-alpine"
REDIS_IMAGE_LOCK = Path(__file__).resolve().with_name("downstream-redis-image.txt")
REDIS_IMAGE_PATTERN = re.compile(r"redis@sha256:[0-9a-f]{64}\Z")
TEST_RESULT = re.compile(r"test result: .*?(\d+) passed")


class BehaviorError(Exception):
    """A Docker, command, test execution, or cleanup failure."""


def run_process(command, timeout=COMMAND_TIMEOUT_SECONDS):
    """Run one argument-vector command in its own process group and capture output."""
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except OSError as error:
        raise BehaviorError(f"cannot start {command[0]}: {error}") from error

    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        _signal_process_group(process.pid, signal.SIGTERM)
        try:
            stdout, stderr = process.communicate(timeout=TERMINATE_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            _signal_process_group(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate()
        raise BehaviorError(
            f"command timed out after {timeout}s: {format_command(command)}\n"
            f"stdout:\n{stdout}\nstderr:\n{stderr}"
        ) from error
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def _signal_process_group(pid, sig):
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def format_command(command):
    return " ".join(str(part) for part in command)


def checked_command(command, label, timeout=COMMAND_TIMEOUT_SECONDS):
    result = run_process(command, timeout=timeout)
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", flush=True)
    if result.stderr:
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", file=sys.stderr, flush=True)
    if result.returncode:
        raise BehaviorError(
            f"{label} failed (exit {result.returncode}): {format_command(command)}"
        )
    return result


def docker_command(command):
    return checked_command(command, "Docker command").stdout


def load_redis_image(path=REDIS_IMAGE_LOCK):
    """Load the single immutable Redis image reference from the lock file."""
    try:
        contents = Path(path).read_text(encoding="utf-8")
    except OSError as error:
        raise BehaviorError(f"cannot read Redis image lock file {path}: {error}") from error
    lines = contents.splitlines()
    if len(lines) != 1 or not REDIS_IMAGE_PATTERN.fullmatch(lines[0]):
        raise BehaviorError(
            f"invalid Redis image lock file {path}: expected one redis@sha256:<64 lowercase hex> line"
        )
    return lines[0]


def inspect_image(reference, allow_missing=False):
    command = ["docker", "image", "inspect", "--format",
               "{{.Id}}|{{.Os}}/{{.Architecture}}|{{range .Config.Env}}{{println .}}{{end}}",
               reference]
    result = run_process(command)
    if result.returncode:
        diagnostic = f"{result.stderr}\n{result.stdout}".lower()
        if allow_missing and "no such image" in diagnostic:
            return None
        raise BehaviorError(
            f"inspect Docker image {reference} failed (exit {result.returncode}): "
            f"{(result.stderr or result.stdout).strip()}"
        )
    fields = result.stdout.strip().split("|", 2)
    if len(fields) != 3 or not fields[0] or not fields[1]:
        raise BehaviorError(f"Docker returned incomplete identity for image {reference}: {result.stdout!r}")
    return fields[0], fields[1], result.stdout


@contextmanager
def pinned_redis_image():
    """Prepare and restore the Redis alias used by the pinned downstream fixture."""
    image = load_redis_image()
    docker_command(["docker", "info"])
    docker_command(["docker", "pull", image])
    fixed_id, platform, details = inspect_image(image)
    version = next((line.partition("=")[2] for line in details.splitlines()
                    if line.startswith("REDIS_VERSION=")), "unknown")
    previous = inspect_image(REDIS_ALIAS, allow_missing=True)
    previous_id = previous[0] if previous else None
    tag_attempted = False
    primary_error = None
    try:
        tag_attempted = True
        checked_command(["docker", "tag", image, REDIS_ALIAS], "tag pinned Redis compatibility alias")
        aliased = inspect_image(REDIS_ALIAS)
        if aliased[0] != fixed_id:
            raise BehaviorError(
                f"Redis compatibility alias identity mismatch: expected {fixed_id}, got {aliased[0]}"
            )
        version_result = checked_command(
            ["docker", "run", "--rm", image, "redis-server", "--version"],
            "read pinned Redis version",
        )
        version_output = (version_result.stdout or version_result.stderr).strip()
        print(f"Redis image: {image}; platform: {platform}; image ID: {fixed_id}; "
              f"Redis version: {version_output or version}", flush=True)
        yield image
    except BaseException as error:
        primary_error = error
        raise
    finally:
        try:
            if tag_attempted:
                current = inspect_image(REDIS_ALIAS, allow_missing=True)
                current_id = current[0] if current else None
                if current_id == fixed_id:
                    if previous_id is None:
                        checked_command(["docker", "image", "rm", REDIS_ALIAS], "remove temporary Redis alias")
                    else:
                        checked_command(["docker", "tag", previous_id, REDIS_ALIAS],
                                        "restore previous Redis compatibility alias")
                elif current_id != previous_id:
                    observed = "missing" if current is None else current_id
                    raise BehaviorError(
                        f"Redis compatibility alias changed during run (expected {fixed_id}, got {observed}); "
                        "leaving it untouched"
                    )
        except BaseException as cleanup_error:
            if primary_error is not None:
                raise BehaviorError(f"{primary_error}; Redis alias restoration failed: {cleanup_error}") from primary_error
            if isinstance(cleanup_error, BehaviorError):
                raise
            raise BehaviorError(f"Redis alias restoration failed: {cleanup_error}") from cleanup_error


def require_test_executed(result, label):
    output = f"{result.stdout}\n{result.stderr}"
    match = TEST_RESULT.search(output)
    if not match or int(match.group(1)) < 1:
        raise BehaviorError(f"{label} executed zero tests or reported no test count:\n{output}")
    print(f"{label}: {match.group(1)} test(s) executed", flush=True)


def start_redis_container(image):
    label = f"qubit-event-bus-behavior={uuid.uuid4().hex}"
    try:
        result = checked_command(
            ["docker", "run", "-d", "--label", label, "-p", "127.0.0.1::6379",
             image, "redis-server", "--appendonly", "yes"],
            "start task fixture Redis container",
        )
        container_id = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else ""
        if not container_id:
            raise BehaviorError("docker run did not return a container ID")
    except BehaviorError as start_error:
        try:
            orphaned_ids = docker_command(
                ["docker", "ps", "-aq", "--filter", f"label={label}"]
            ).splitlines()
            for orphaned_id in orphaned_ids:
                if orphaned_id.strip():
                    remove_container(orphaned_id.strip())
        except BehaviorError as cleanup_error:
            raise BehaviorError(
                f"{start_error}; startup cleanup failed: {cleanup_error}"
            ) from cleanup_error
        raise
    return container_id


def wait_for_redis(container_id):
    port_output = docker_command(["docker", "port", container_id, "6379/tcp"]).strip()
    match = re.search(r"(?:127\.0\.0\.1|0\.0\.0\.0|\[::\]):(\d+)$", port_output)
    if not match:
        raise BehaviorError(f"cannot determine mapped Redis port from docker port output: {port_output!r}")
    port = match.group(1)
    deadline = time.monotonic() + 30
    last_error = "Redis did not answer PING"
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        result = run_process(["docker", "exec", container_id, "redis-cli", "ping"],
                             timeout=min(10, remaining))
        if result.returncode == 0 and result.stdout.strip() == "PONG":
            return f"redis://127.0.0.1:{port}"
        last_error = (result.stderr or result.stdout or last_error).strip()
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))
    raise BehaviorError(f"Redis container {container_id} readiness failed after 30s: {last_error}")


def remove_container(container_id):
    checked_command(["docker", "rm", "-f", container_id], "remove task fixture Redis container")


def execute(ecosystem_root, manifest_path):
    config = compatibility.load_manifest(manifest_path)
    core, manifests = compatibility.validate_layout(ecosystem_root, config)
    del core  # Layout validation retains the compatibility gate's core-source check.
    redis_manifest = manifests["redis"]
    commands = (
        ("sync conformance", ["cargo", "test", "--locked", "--all-features",
         "--manifest-path", str(redis_manifest), "--test", "sync_standalone_tests",
         "test_sync_spi_conformance", "--", "--exact"], True),
        ("async conformance", ["cargo", "test", "--locked", "--all-features",
         "--manifest-path", str(redis_manifest), "--test", "async_standalone_tests",
         "test_async_spi_conformance", "--", "--exact"], True),
    )
    with pinned_redis_image() as image:
        for label, command, count_tests in commands:
            print(f"downstream behavior: {label}", flush=True)
            result = checked_command(command, label)
            if count_tests:
                require_test_executed(result, label)

        container_id = start_redis_container(image)
        try:
            redis_url = wait_for_redis(container_id)
            command = ["cargo", "run", "--locked", "--manifest-path",
                       str(manifests["task_notifications"]), "--", redis_url]
            print("downstream behavior: task notifications", flush=True)
            checked_command(command, "task notifications")
        finally:
            remove_container(container_id)


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
        execute(args.ecosystem_root.absolute(), manifest_path)
    except (BehaviorError, compatibility.CompatibilityError, OSError) as error:
        print(f"downstream behavior: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    raise SystemExit(0)


if __name__ == "__main__":
    main()
