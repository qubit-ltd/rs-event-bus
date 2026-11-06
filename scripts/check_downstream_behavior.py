#!/usr/bin/env python3
"""Run the fixed behavioral checks for the pinned Redis event bus consumer."""

import argparse
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
REDIS_IMAGE = "redis:7-alpine"
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


def prepare_docker():
    docker_command(["docker", "info"])
    docker_command(["docker", "pull", REDIS_IMAGE])


def require_test_executed(result, label):
    output = f"{result.stdout}\n{result.stderr}"
    match = TEST_RESULT.search(output)
    if not match or int(match.group(1)) < 1:
        raise BehaviorError(f"{label} executed zero tests or reported no test count:\n{output}")
    print(f"{label}: {match.group(1)} test(s) executed", flush=True)


def start_redis_container():
    label = f"qubit-event-bus-behavior={uuid.uuid4().hex}"
    try:
        result = checked_command(
            ["docker", "run", "-d", "--label", label, "-p", "127.0.0.1::6379",
             REDIS_IMAGE, "redis-server", "--appendonly", "yes"],
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
    prepare_docker()

    redis_manifest = manifests["redis"]
    commands = (
        ("sync conformance", ["cargo", "test", "--locked", "--all-features",
         "--manifest-path", str(redis_manifest), "--test", "sync_standalone_tests",
         "test_sync_spi_conformance", "--", "--exact"], True),
        ("async conformance", ["cargo", "test", "--locked", "--all-features",
         "--manifest-path", str(redis_manifest), "--test", "async_standalone_tests",
         "test_async_spi_conformance", "--", "--exact"], True),
    )
    for label, command, count_tests in commands:
        print(f"downstream behavior: {label}", flush=True)
        result = checked_command(command, label)
        if count_tests:
            require_test_executed(result, label)

    container_id = start_redis_container()
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
