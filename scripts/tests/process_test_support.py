"""Helpers for observing child processes in integration tests."""

import os
from pathlib import Path
import time


def _procfs_is_available():
    """Return whether this host exposes readable procfs process state."""
    try:
        Path("/proc/self/stat").read_text(encoding="utf-8")
    except (FileNotFoundError, NotADirectoryError, PermissionError):
        return False
    return True


def _is_zombie(stat):
    """Read the state field after the parenthesized, possibly spaced name."""
    closing_parenthesis = stat.rfind(")")
    if closing_parenthesis < 0:
        return False
    fields = stat[closing_parenthesis + 1:].split()
    return bool(fields) and fields[0] == "Z"


def wait_for_process_exit(pid, timeout=2.0):
    """Wait a bounded time for a process to exit or become a zombie."""
    deadline = time.monotonic() + timeout
    procfs_available = _procfs_is_available()
    while True:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        if procfs_available:
            try:
                stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            except (FileNotFoundError, ProcessLookupError):
                return True
            if _is_zombie(stat):
                return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(0.02, remaining))
