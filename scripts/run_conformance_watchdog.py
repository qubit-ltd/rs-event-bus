#!/usr/bin/env python3
"""Run a conformance command with an external, process-level deadline.

On POSIX the timeout kills the command's process group. On other platforms it
kills and waits for the direct child only; descendants may outlive the watchdog.
"""

import argparse
import math
import os
import signal
import subprocess
import sys
import time


def process_group_options():
    """Start a process that can be terminated together with its descendants."""
    return {"start_new_session": True} if os.name == "posix" else {}


def terminate_process_group(process):
    """Kill the child's process group on POSIX and always reap the child."""
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    elif process.poll() is None:
        process.kill()
    process.wait()


def positive_finite(value):
    try:
        number = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a finite positive number") from error
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return number


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout-seconds", required=True, type=positive_finite)
    parser.add_argument("--label", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("a command is required after --")
    return args


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    deadline = time.monotonic() + args.timeout_seconds
    try:
        process = subprocess.Popen(args.command, **process_group_options())
    except OSError as error:
        print(f"conformance watchdog [{args.label}]: failed to start command: {error}", file=sys.stderr)
        return 1

    child_pid_file = os.environ.get("CONFORMANCE_WATCHDOG_CHILD_PID_FILE")
    if child_pid_file:
        try:
            with open(child_pid_file, "w", encoding="ascii") as stream:
                stream.write(f"{process.pid}\n")
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as error:
            terminate_process_group(process)
            print(f"conformance watchdog [{args.label}]: failed to write child PID file: {error}", file=sys.stderr)
            return 1

    remaining = max(0.0, deadline - time.monotonic())
    try:
        return process.wait(timeout=remaining)
    except subprocess.TimeoutExpired:
        terminate_process_group(process)
        print(
            f"conformance watchdog [{args.label}]: timed out after {args.timeout_seconds:g} seconds; "
            "logs show the last progress event as an operation that had not completed",
            file=sys.stderr,
            flush=True,
        )
        return 124


if __name__ == "__main__":
    raise SystemExit(main())
