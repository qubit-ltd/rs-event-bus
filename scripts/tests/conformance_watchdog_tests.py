import math
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_conformance_watchdog import process_group_options
from scripts.run_conformance_watchdog import terminate_process_group


ROOT = Path(__file__).resolve().parents[2]
WATCHDOG = ROOT / "scripts" / "run_conformance_watchdog.py"


class ConformanceWatchdogTests(unittest.TestCase):
    def run_watchdog(self, timeout, *command):
        return subprocess.run(
            [sys.executable, str(WATCHDOG), "--timeout-seconds", str(timeout),
             "--label", "unit-fixture", "--", *command],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            check=False,
        )

    def test_returns_success_exit_status(self):
        result = self.run_watchdog(2, sys.executable, "-c", "print('ok')")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ok", result.stdout)

    def test_preserves_child_failure_status(self):
        result = self.run_watchdog(2, sys.executable, "-c", "raise SystemExit(7)")
        self.assertEqual(result.returncode, 7, result.stderr)

    def test_timeout_kills_and_reaps_blocking_child(self):
        result = self.run_watchdog(0.1, sys.executable, "-c", "import time; time.sleep(30)")
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertIn("unit-fixture", result.stderr)
        self.assertIn("last progress", result.stderr.lower())

    def test_rejects_invalid_timeout_values(self):
        for timeout in (0, -1, float("nan"), float("inf"), -float("inf")):
            with self.subTest(timeout=timeout):
                result = self.run_watchdog(timeout, sys.executable, "-c", "pass")
                self.assertNotEqual(result.returncode, 0)
                self.assertNotEqual(result.returncode, 124)

    def test_requires_command(self):
        result = subprocess.run(
            [sys.executable, str(WATCHDOG), "--timeout-seconds", "1", "--label", "missing", "--"],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotEqual(result.returncode, 124)

    def test_process_group_options_match_platform(self):
        if os.name == "posix":
            self.assertEqual(process_group_options(), {"start_new_session": True})
        else:
            self.assertEqual(process_group_options(), {})

    @unittest.skipUnless(os.name == "posix", "process group cleanup is POSIX-specific")
    def test_termination_still_kills_group_after_leader_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "grandchild.pid"
            child = subprocess.Popen(
                [sys.executable, "-c",
                 "import subprocess,sys; "
                 "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
                 f"open({str(pid_file)!r},'w').write(str(p.pid))"],
                **process_group_options(),
            )
            self.assertEqual(child.wait(timeout=5), 0)
            child_pid = int(pid_file.read_text())

            terminate_process_group(child)

            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                stat_path = Path(f"/proc/{child_pid}/stat")
                if stat_path.exists() and stat_path.read_text().split()[2] == "Z":
                    break
                time.sleep(0.02)
            else:
                self.fail(f"descendant {child_pid} survived after its group leader exited")

    def test_pid_file_failure_kills_child_and_preserves_failure_status(self):
        with tempfile.TemporaryDirectory() as directory:
            missing_pid_file = Path(directory) / "missing" / "child.pid"
            env = os.environ.copy()
            env["CONFORMANCE_WATCHDOG_CHILD_PID_FILE"] = str(missing_pid_file)
            result = subprocess.run(
                [sys.executable, str(WATCHDOG), "--timeout-seconds", "2",
                 "--label", "pid-file-failure", "--", sys.executable,
                 "-c", "import time; time.sleep(30)"],
                cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, timeout=5, check=False,
            )
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertIn("failed to write child PID file", result.stderr)

    @unittest.skipUnless(os.name == "posix", "process group cleanup is POSIX-specific")
    def test_timeout_kills_descendant_process_group(self):
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "grandchild.pid"
            code = (
                "import subprocess,sys,time; "
                "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
                f"open({str(pid_file)!r},'w').write(str(p.pid)); "
                "time.sleep(30)"
            )
            result = self.run_watchdog(1, sys.executable, "-c", code)
            self.assertEqual(result.returncode, 124, result.stderr)
            child_pid = int(pid_file.read_text())
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                stat_path = Path(f"/proc/{child_pid}/stat")
                if stat_path.exists():
                    stat = stat_path.read_text().split()
                    if len(stat) > 2 and stat[2] == "Z":
                        break
                time.sleep(0.02)
            else:
                self.fail(f"descendant {child_pid} survived watchdog timeout")


if __name__ == "__main__":
    unittest.main()
