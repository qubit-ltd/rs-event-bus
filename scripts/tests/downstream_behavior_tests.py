"""Mock-only tests for the fixed downstream behavior gate."""

import importlib.util
from contextlib import nullcontext
from pathlib import Path
import signal
import tempfile
import subprocess
import sys
import uuid
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "check_downstream_behavior.py"
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("check_downstream_behavior", SCRIPT)
behavior = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = behavior
SPEC.loader.exec_module(behavior)


class DownstreamBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.root = Path("/ecosystem")
        self.core = self.root / "rs-event-bus"
        self.manifests = {
            "redis": self.root / "rs-event-bus-redis/Cargo.toml",
            "task_notifications": self.root / "rs-event-bus-redis/tests/fixtures/task_notifications/Cargo.toml",
            "ioc_application_consumer": self.root / "rs-ioc/tests/fixtures/application_consumer_current/Cargo.toml",
        }
        self.commands = []

    def cargo_success(self, command, **kwargs):
        self.commands.append(command)
        if "test" in command:
            stdout = "running 1 test\ntest selected::test ... ok\ntest result: ok. 1 passed; 0 failed\n"
        else:
            stdout = "fixture assertions passed\n"
        return subprocess.CompletedProcess(command, 0, stdout, "")

    def setup_gate(self):
        return mock.patch.object(behavior.compatibility, "load_manifest", return_value={}), \
            mock.patch.object(behavior.compatibility, "validate_layout", return_value=(self.core, self.manifests))

    def test_runs_three_fixed_locked_commands_and_reports_test_count(self):
        load, layout = self.setup_gate()
        with load, layout, \
             mock.patch.object(behavior, "pinned_redis_image", return_value=nullcontext("redis@sha256:" + "a" * 64)), \
             mock.patch.object(behavior, "run_process", side_effect=self.cargo_success), \
             mock.patch.object(behavior, "docker_command", return_value="container-id"), \
             mock.patch.object(behavior, "start_redis_container", return_value="container-id"), \
             mock.patch.object(behavior, "wait_for_redis", return_value="redis://127.0.0.1:43210"), \
             mock.patch.object(behavior, "remove_container") as remove, \
             mock.patch("builtins.print") as output:
            behavior.execute(self.root, Path("manifest.json"))
        commands = self.commands
        self.assertEqual(len(commands), 3)
        self.assertEqual(commands[0], [
            "cargo", "test", "--locked", "--all-features", "--manifest-path",
            str(self.manifests["redis"]), "--test", "sync_standalone_tests",
            "test_sync_spi_conformance", "--", "--exact",
        ])
        self.assertEqual(commands[1], [
            "cargo", "test", "--locked", "--all-features", "--manifest-path",
            str(self.manifests["redis"]), "--test", "async_standalone_tests",
            "test_async_spi_conformance", "--", "--exact",
        ])
        self.assertEqual(commands[2], [
            "cargo", "run", "--locked", "--manifest-path",
            str(self.manifests["task_notifications"]), "--", "redis://127.0.0.1:43210",
        ])
        self.assertTrue(any("1 passed" in str(call) for call in output.call_args_list))
        remove.assert_called_once_with("container-id")

    def test_zero_executed_tests_is_failure(self):
        result = subprocess.CompletedProcess([], 0, "running 0 tests\ntest result: ok. 0 passed\n", "")
        with self.assertRaises(behavior.BehaviorError):
            behavior.require_test_executed(result, "sync conformance")

    def test_nonzero_command_stops_before_remaining_cargo(self):
        load, layout = self.setup_gate()
        failed = subprocess.CompletedProcess([], 7, "partial", "cargo failed")
        with load, layout, \
             mock.patch.object(behavior, "pinned_redis_image", return_value=nullcontext("redis@sha256:" + "a" * 64)), \
             mock.patch.object(behavior, "run_process", return_value=failed) as run, \
             mock.patch.object(behavior, "docker_command", return_value="id"), \
             mock.patch.object(behavior, "wait_for_redis", return_value="redis://127.0.0.1:1"), \
             mock.patch.object(behavior, "remove_container"):
            with self.assertRaisesRegex(behavior.BehaviorError, "exit 7"):
                behavior.execute(self.root, Path("manifest.json"))
        self.assertEqual(run.call_count, 1)

    def test_timeout_terminates_and_reaps_process_group(self):
        process = mock.Mock(pid=1234)
        process.communicate.side_effect = [
            subprocess.TimeoutExpired(["cargo"], 600),
            subprocess.TimeoutExpired(["cargo"], 5),
            ("out", "err"),
        ]
        with mock.patch.object(behavior.subprocess, "Popen", return_value=process) as popen, \
             mock.patch.object(behavior.os, "killpg") as killpg, \
             mock.patch.object(behavior.time, "sleep"):
            with self.assertRaisesRegex(behavior.BehaviorError, "timed out"):
                behavior.run_process(["cargo", "test"], timeout=600)
        popen.assert_called_once()
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        self.assertEqual(killpg.call_args_list[0].args, (1234, signal.SIGTERM))
        self.assertEqual(killpg.call_args_list[1].args, (1234, signal.SIGKILL))
        process.communicate.assert_called()
        self.assertEqual(process.communicate.call_args_list[0].kwargs["timeout"], 600)

    def test_docker_unavailable_fails(self):
        with mock.patch.object(behavior, "run_process", side_effect=behavior.BehaviorError("docker unavailable")):
            with self.assertRaisesRegex(behavior.BehaviorError, "docker unavailable"):
                behavior.docker_command(["docker", "info"])

    def test_readiness_failure_still_removes_owned_container(self):
        load, layout = self.setup_gate()
        with load, layout, \
             mock.patch.object(behavior, "pinned_redis_image", return_value=nullcontext("redis@sha256:" + "a" * 64)), \
             mock.patch.object(behavior, "run_process", side_effect=self.cargo_success), \
             mock.patch.object(behavior, "docker_command", return_value="container-id"), \
             mock.patch.object(behavior, "start_redis_container", return_value="container-id"), \
             mock.patch.object(behavior, "wait_for_redis", side_effect=behavior.BehaviorError("readiness timeout")), \
             mock.patch.object(behavior, "remove_container") as remove:
            with self.assertRaisesRegex(behavior.BehaviorError, "readiness timeout"):
                behavior.execute(self.root, Path("manifest.json"))
        remove.assert_called_once_with("container-id")

    def test_failed_container_start_removes_container_by_unique_label(self):
        with mock.patch.object(uuid, "uuid4", return_value=mock.Mock(hex="run-id")), \
             mock.patch.object(behavior, "checked_command", side_effect=behavior.BehaviorError("docker run timed out")) as run, \
             mock.patch.object(behavior, "docker_command", return_value="orphan-id\n") as list_containers, \
             mock.patch.object(behavior, "remove_container") as remove:
            with self.assertRaisesRegex(behavior.BehaviorError, "docker run timed out"):
                behavior.start_redis_container("redis@sha256:" + "a" * 64)

        command = run.call_args.args[0]
        self.assertIn("--label", command)
        self.assertIn("qubit-event-bus-behavior=run-id", command)
        list_containers.assert_called_once_with([
            "docker", "ps", "-aq", "--filter", "label=qubit-event-bus-behavior=run-id",
        ])
        remove.assert_called_once_with("orphan-id")

    def test_container_cleanup_failure_is_reported(self):
        load, layout = self.setup_gate()
        with load, layout, \
             mock.patch.object(behavior, "pinned_redis_image", return_value=nullcontext("redis@sha256:" + "a" * 64)), \
             mock.patch.object(behavior, "run_process", side_effect=self.cargo_success), \
             mock.patch.object(behavior, "docker_command", return_value="container-id"), \
             mock.patch.object(behavior, "start_redis_container", return_value="container-id"), \
             mock.patch.object(behavior, "wait_for_redis", return_value="redis://127.0.0.1:43210"), \
             mock.patch.object(behavior, "remove_container", side_effect=behavior.BehaviorError("docker rm failed")):
            with self.assertRaisesRegex(behavior.BehaviorError, "docker rm failed"):
                behavior.execute(self.root, Path("manifest.json"))

    def test_load_redis_image_accepts_single_digest_and_rejects_invalid_files(self):
        valid = "redis@sha256:" + "a" * 64 + "\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.txt"
            path.write_text(valid)
            self.assertEqual(behavior.load_redis_image(path), valid.strip())
            for contents in ("", valid + valid, "redis:7-alpine\n",
                             "redis@sha256:" + "g" * 64 + "\n",
                             "redis@sha256:" + "a" * 63 + "\n",
                             "other@sha256:" + "a" * 64 + "\n"):
                path.write_text(contents)
                with self.subTest(contents=contents), self.assertRaises(behavior.BehaviorError):
                    behavior.load_redis_image(path)
            path.unlink()
            with self.assertRaises(behavior.BehaviorError):
                behavior.load_redis_image(path)

    def test_pinned_image_pulls_verifies_and_uses_digest_then_restores_old_alias(self):
        image = "redis@sha256:" + "a" * 64
        calls = []
        inspect_results = iter([
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|REDIS_VERSION=7.4.12\n", ""),
            subprocess.CompletedProcess([], 0, "sha256:old|linux/amd64|", ""),
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|", ""),
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|", ""),
        ])
        def checked(cmd, label, timeout=behavior.COMMAND_TIMEOUT_SECONDS):
            calls.append(cmd)
            output = "container-id\n" if cmd[:2] == ["docker", "run"] else ""
            return subprocess.CompletedProcess(cmd, 0, output, "")
        def inspected(cmd, **kwargs):
            calls.append(cmd)
            return next(inspect_results)
        with mock.patch.object(behavior, "load_redis_image", return_value=image), \
             mock.patch.object(behavior, "checked_command", side_effect=checked), \
             mock.patch.object(behavior, "run_process", side_effect=inspected), \
             mock.patch.object(behavior, "docker_command", side_effect=lambda c: calls.append(c) or ""):
            with behavior.pinned_redis_image() as pinned:
                behavior.start_redis_container(pinned)
        self.assertIn(["docker", "pull", image], calls)
        self.assertIn(["docker", "tag", image, behavior.REDIS_ALIAS], calls)
        self.assertIn(["docker", "tag", "sha256:old", behavior.REDIS_ALIAS], calls)
        run = next(c for c in calls if c[:2] == ["docker", "run"])
        self.assertEqual(run[-3], image)
        self.assertTrue(any(c[:3] == ["docker", "run", "--rm"] and image in c for c in calls))

    def test_pinned_image_removes_new_alias_and_preserves_primary_error_on_restore_failure(self):
        image = "redis@sha256:" + "b" * 64
        calls = []
        inspections = iter([
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|REDIS_VERSION=7.4.12\n", ""),
            subprocess.CompletedProcess([], 1, "", "Error: No such image: redis:7-alpine"),
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|", ""),
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|", ""),
        ])
        def checked(cmd, label, timeout=behavior.COMMAND_TIMEOUT_SECONDS):
            calls.append(cmd)
            if cmd[:3] == ["docker", "image", "rm"]:
                raise behavior.BehaviorError("remove alias failed")
            return subprocess.CompletedProcess(cmd, 0, "", "")
        def inspected(cmd, **kwargs):
            calls.append(cmd)
            return next(inspections)
        with mock.patch.object(behavior, "load_redis_image", return_value=image), \
             mock.patch.object(behavior, "checked_command", side_effect=checked), \
             mock.patch.object(behavior, "run_process", side_effect=inspected), \
             mock.patch.object(behavior, "docker_command", return_value=""):
            with self.assertRaisesRegex(behavior.BehaviorError, "primary cargo failure.*remove alias failed"):
                with behavior.pinned_redis_image():
                    raise behavior.BehaviorError("primary cargo failure")
        self.assertIn(["docker", "image", "rm", behavior.REDIS_ALIAS], calls)

    def test_pinned_image_does_not_overwrite_alias_changed_by_another_process(self):
        image = "redis@sha256:" + "c" * 64
        inspections = iter([
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|REDIS_VERSION=7.4.12\n", ""),
            subprocess.CompletedProcess([], 1, "", "Error: No such image: redis:7-alpine"),
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|", ""),
            subprocess.CompletedProcess([], 0, "sha256:external|linux/amd64|", ""),
        ])
        commands = []
        def checked(cmd, label, timeout=behavior.COMMAND_TIMEOUT_SECONDS):
            commands.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, "Redis server v=7.4.12\n", "")
        with mock.patch.object(behavior, "load_redis_image", return_value=image), \
             mock.patch.object(behavior, "checked_command", side_effect=checked), \
             mock.patch.object(behavior, "run_process", side_effect=lambda cmd, **kw: next(inspections)), \
             mock.patch.object(behavior, "docker_command", return_value=""):
            with self.assertRaisesRegex(behavior.BehaviorError, "changed during run"):
                with behavior.pinned_redis_image():
                    pass
        self.assertEqual(commands.count(["docker", "tag", image, behavior.REDIS_ALIAS]), 1)
        self.assertFalse(any(cmd[:3] == ["docker", "image", "rm"] for cmd in commands))
        self.assertFalse(any(cmd[:2] == ["docker", "tag"] and cmd[-1] == behavior.REDIS_ALIAS
                             and cmd[2] != image for cmd in commands))

    def test_pinned_image_reports_removed_alias_and_preserves_primary_error(self):
        image = "redis@sha256:" + "e" * 64
        inspections = iter([
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|REDIS_VERSION=7.4.12\n", ""),
            subprocess.CompletedProcess([], 1, "", "Error: No such image: redis:7-alpine"),
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|", ""),
            subprocess.CompletedProcess([], 1, "", "Error: No such image: redis:7-alpine"),
        ])
        with mock.patch.object(behavior, "load_redis_image", return_value=image), \
             mock.patch.object(behavior, "checked_command", return_value=subprocess.CompletedProcess([], 0, "Redis 7.4.12", "")), \
             mock.patch.object(behavior, "run_process", side_effect=lambda cmd, **kw: next(inspections)), \
             mock.patch.object(behavior, "docker_command", return_value=""):
            with self.assertRaisesRegex(behavior.BehaviorError,
                                        "primary task failure.*alias changed during run.*got missing"):
                with behavior.pinned_redis_image():
                    raise behavior.BehaviorError("primary task failure")

    def test_failed_tag_with_no_prior_alias_does_not_report_false_interference(self):
        image = "redis@sha256:" + "f" * 64
        inspections = iter([
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|REDIS_VERSION=7.4.12\n", ""),
            subprocess.CompletedProcess([], 1, "", "Error: No such image: redis:7-alpine"),
            subprocess.CompletedProcess([], 1, "", "Error: No such image: redis:7-alpine"),
        ])
        def checked(cmd, label, timeout=behavior.COMMAND_TIMEOUT_SECONDS):
            if cmd[:2] == ["docker", "tag"]:
                raise behavior.BehaviorError("tag operation failed")
            return subprocess.CompletedProcess(cmd, 0, "Redis 7.4.12", "")
        with mock.patch.object(behavior, "load_redis_image", return_value=image), \
             mock.patch.object(behavior, "checked_command", side_effect=checked), \
             mock.patch.object(behavior, "run_process", side_effect=lambda cmd, **kw: next(inspections)), \
             mock.patch.object(behavior, "docker_command", return_value=""):
            with self.assertRaisesRegex(behavior.BehaviorError, "tag operation failed") as error:
                with behavior.pinned_redis_image():
                    self.fail("workload must not start after a failed tag")
        self.assertNotIn("alias changed", str(error.exception))

    def test_pinned_image_identity_mismatch_stops_before_workload(self):
        image = "redis@sha256:" + "d" * 64
        inspections = iter([
            subprocess.CompletedProcess([], 0, "sha256:fixed|linux/amd64|REDIS_VERSION=7.4.12\n", ""),
            subprocess.CompletedProcess([], 1, "", "Error: No such image: redis:7-alpine"),
            subprocess.CompletedProcess([], 0, "sha256:wrong|linux/amd64|", ""),
            subprocess.CompletedProcess([], 0, "sha256:wrong|linux/amd64|", ""),
        ])
        with mock.patch.object(behavior, "load_redis_image", return_value=image), \
             mock.patch.object(behavior, "checked_command") as checked, \
             mock.patch.object(behavior, "run_process", side_effect=lambda cmd, **kw: next(inspections)), \
             mock.patch.object(behavior, "docker_command", return_value=""):
            with self.assertRaisesRegex(behavior.BehaviorError, "identity mismatch.*restoration failed"):
                with behavior.pinned_redis_image():
                    self.fail("workload must not run with an unexpected alias identity")
        self.assertFalse(any(call.args[0][:2] == ["cargo", "test"] for call in checked.call_args_list))

    def test_image_preparation_failure_stops_before_cargo(self):
        load, layout = self.setup_gate()
        with load, layout, \
             mock.patch.object(behavior, "pinned_redis_image") as pinned, \
             mock.patch.object(behavior, "run_process") as run:
            pinned.return_value.__enter__.side_effect = behavior.BehaviorError("pull failed")
            with self.assertRaisesRegex(behavior.BehaviorError, "pull failed"):
                behavior.execute(self.root, Path("manifest.json"))
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
