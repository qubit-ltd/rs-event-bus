"""Regression tests for the fixed downstream compilation gate."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
SCRIPT = ROOT / "scripts" / "check_downstream_compatibility.py"
MANIFEST = ROOT / "scripts" / "downstream-compatibility.json"
SPEC = importlib.util.spec_from_file_location("check_downstream_compatibility", SCRIPT)
gate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = gate
SPEC.loader.exec_module(gate)


class DownstreamCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "ecosystem"
        self.root.mkdir()
        self.config = json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else {
            "schema_version": 1,
            "repositories": [
                {"name": name, "url": f"https://github.com/qubit-ltd/{name}.git",
                 "revision": f"{index:040x}"}
                for index, name in enumerate(("rs-event-bus-redis", "rs-task", "rs-ioc",
                                              "rs-execution-services", "rs-config", "rs-fs",
                                              "rs-fs-local", "rs-fs-registry", "rs-fs-testkit",
                                              "rs-local-files", "rs-metadata", "rs-model-metadata",
                                              "rs-redact", "rs-value"), 1)
            ],
            "targets": [
                {"name": "redis", "manifest": "rs-event-bus-redis/Cargo.toml",
                 "features": ["--all-features"]},
                {"name": "task_notifications",
                 "manifest": "rs-event-bus-redis/tests/fixtures/task_notifications/Cargo.toml",
                 "features": []},
                {"name": "ioc_application_consumer",
                 "manifest": "rs-ioc/tests/fixtures/application_consumer_current/Cargo.toml",
                 "features": []},
            ],
        }
        for repository in self.config["repositories"]:
            directory = self.root / repository["name"]
            directory.mkdir()
            (directory / "Cargo.toml").write_text("[workspace]\n", encoding="utf-8")
            (directory / ".git").mkdir()
        for target in self.config["targets"]:
            manifest = self.root / target["manifest"]
            manifest.parent.mkdir(parents=True, exist_ok=True)
            manifest.write_text("[workspace]\n", encoding="utf-8")
        (self.root / "rs-event-bus").mkdir()
        (self.root / "rs-event-bus" / "Cargo.toml").write_text("[package]\n", encoding="utf-8")
        self.manifest_path = Path(self.temp.name) / "manifest.json"
        self.write_config()

    def tearDown(self):
        self.temp.cleanup()

    def write_config(self):
        self.manifest_path.write_text(json.dumps(self.config), encoding="utf-8")

    def metadata_json(self, *, core_path=None, duplicate=False):
        core_path = core_path or self.root / "rs-event-bus/Cargo.toml"
        packages = [{"id": "consumer", "name": "consumer", "version": "0.0.0"},
                    {"id": "bus", "name": "qubit-event-bus", "version": "0.21.0",
                     "source": None, "manifest_path": str(core_path)}]
        dependencies = ["bus"]
        nodes = [{"id": "consumer", "dependencies": dependencies},
                 {"id": "bus", "dependencies": []}]
        if duplicate:
            packages.append({"id": "bus-copy", "name": "qubit-event-bus", "version": "0.21.0",
                             "source": None, "manifest_path": str(core_path)})
            dependencies.append("bus-copy")
            nodes.append({"id": "bus-copy", "dependencies": []})
        return json.dumps({"packages": packages, "workspace_members": ["consumer"],
                           "resolve": {"nodes": nodes}})

    def command_runner(self, calls, *, metadata=None, check_exit=0, error=""):
        def invoke(command):
            calls.append(command)
            if command[0] == "git":
                if command[3] == "rev-parse":
                    repository = Path(command[2]).name
                    revision = next(item["revision"] for item in self.config["repositories"]
                                    if item["name"] == repository)
                    return subprocess.CompletedProcess(command, 0, revision + "\n", "")
                return subprocess.CompletedProcess(command, 0, "", "")
            if command[1] == "metadata":
                return subprocess.CompletedProcess(command, 0,
                                                   metadata or self.metadata_json(), "")
            return subprocess.CompletedProcess(command, check_exit, "", error)
        return invoke

    def test_valid_manifest_runs_metadata_before_check_for_all_targets(self):
        calls = []

        with mock.patch.object(gate, "run_command", side_effect=self.command_runner(calls)):
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as exit_result:
                    gate.main(["--ecosystem-root", str(self.root), "--manifest", str(self.manifest_path)])
        self.assertEqual(exit_result.exception.code, 0)
        cargo_calls = [call for call in calls if call[0] == "cargo"]
        self.assertEqual(len(cargo_calls), 6)
        for metadata, check in zip(cargo_calls[::2], cargo_calls[1::2]):
            self.assertEqual(metadata[1], "metadata")
            self.assertIn("--locked", metadata)
            self.assertIn("--format-version", metadata)
            self.assertIn("1", metadata)
            self.assertEqual(check[1], "check")
            self.assertIn("--locked", check)
            self.assertIn("--all-targets", check)
            self.assertIn("--manifest-path", metadata)
            self.assertEqual(
                metadata[metadata.index("--manifest-path") + 1],
                check[check.index("--manifest-path") + 1],
            )
        self.assertIn("--all-features", cargo_calls[0])
        self.assertIn("--all-features", cargo_calls[1])
        self.assertNotIn("--all-features", cargo_calls[2])

    def test_metadata_failure_does_not_start_check_and_preserves_error(self):
        calls = []

        def invoke(command):
            if command[0] == "git":
                return self.command_runner([])(command)
            calls.append(command)
            return subprocess.CompletedProcess(command, 17, "", "locked graph failure")

        with mock.patch.object(gate, "run_command", side_effect=invoke):
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as errors:
                with self.assertRaises(SystemExit) as exit_result:
                    gate.main(["--ecosystem-root", str(self.root), "--manifest", str(self.manifest_path)])
        self.assertNotEqual(exit_result.exception.code, 0)
        self.assertEqual(len(calls), 1)
        self.assertIn("metadata", errors.getvalue())
        self.assertIn("locked graph failure", errors.getvalue())

    def test_check_failure_propagates_and_names_stage(self):
        calls = []

        def invoke(command):
            if command[0] == "git":
                return self.command_runner([])(command)
            calls.append(command)
            if command[1] == "metadata":
                return subprocess.CompletedProcess(command, 0, self.metadata_json(), "")
            return subprocess.CompletedProcess(command, 9, "", "incompatible API")

        with mock.patch.object(gate, "run_command", side_effect=invoke):
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as errors:
                with self.assertRaises(SystemExit) as exit_result:
                    gate.main(["--ecosystem-root", str(self.root), "--manifest", str(self.manifest_path)])
        self.assertNotEqual(exit_result.exception.code, 0)
        self.assertEqual(len(calls), 2)
        self.assertIn("check", errors.getvalue())
        self.assertIn("incompatible API", errors.getvalue())

    def test_wrong_or_duplicate_core_metadata_is_rejected_before_check(self):
        cases = (
            (self.metadata_json(core_path=self.root / "wrong-core/Cargo.toml"), "expected local core"),
            (self.metadata_json(duplicate=True), "multiple qubit-event-bus packages"),
        )
        for graph, message in cases:
            with self.subTest(message=message):
                calls = []
                with mock.patch.object(
                    gate, "run_command", side_effect=self.command_runner(calls, metadata=graph)
                ):
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as errors:
                        with self.assertRaises(SystemExit) as exit_result:
                            gate.main(["--ecosystem-root", str(self.root),
                                       "--manifest", str(self.manifest_path)])
                self.assertNotEqual(exit_result.exception.code, 0)
                self.assertEqual(len([call for call in calls if call[0] == "cargo"]), 1)
                self.assertIn(message, errors.getvalue())

    def test_invalid_sha_and_missing_repository_are_rejected(self):
        self.config["repositories"][0]["revision"] = "deadbeef"
        self.write_config()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as errors:
            with self.assertRaises(SystemExit) as exit_result:
                gate.main(["--ecosystem-root", str(self.root), "--manifest", str(self.manifest_path)])
        self.assertNotEqual(exit_result.exception.code, 0)
        self.assertIn("revision", errors.getvalue())
        self.config["repositories"][0]["revision"] = f"{1:040x}"
        self.write_config()

        def unexpected_revision(command):
            if command[0] == "git":
                return subprocess.CompletedProcess(command, 0, "0" * 40 + "\n", "")
            self.fail("Cargo must not run when a checkout revision differs")

        with mock.patch.object(gate, "run_command", side_effect=unexpected_revision):
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as errors:
                with self.assertRaises(SystemExit) as exit_result:
                    gate.main(["--ecosystem-root", str(self.root), "--manifest", str(self.manifest_path)])
        self.assertNotEqual(exit_result.exception.code, 0)
        self.assertIn("expected", errors.getvalue())
        (self.root / self.config["repositories"][0]["name"]).rename(
            self.root / "missing-repository")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as errors:
            with self.assertRaises(SystemExit) as exit_result:
                gate.main(["--ecosystem-root", str(self.root), "--manifest", str(self.manifest_path)])
        self.assertNotEqual(exit_result.exception.code, 0)
        self.assertIn("missing", errors.getvalue())

    def test_dirty_fixed_revision_checkout_is_rejected_before_cargo(self):
        for status, label in (("M  staged", "staged"), (" M unstaged", "unstaged"),
                              ("?? untracked file", "untracked")):
            with self.subTest(label=label):
                calls = []
                def invoke(command):
                    calls.append(command)
                    if command[0] == "git" and command[3] == "rev-parse":
                        repository = Path(command[2]).name
                        revision = next(item["revision"] for item in self.config["repositories"]
                                        if item["name"] == repository)
                        return subprocess.CompletedProcess(command, 0, revision + "\n", "")
                    if command[0] == "git":
                        return subprocess.CompletedProcess(command, 0, status + "\n", "")
                    self.fail("Cargo must not run for a dirty downstream checkout")
                with mock.patch.object(gate, "run_command", side_effect=invoke):
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as errors:
                        with self.assertRaises(SystemExit) as exit_result:
                            gate.main(["--ecosystem-root", str(self.root), "--manifest", str(self.manifest_path)])
                self.assertEqual(exit_result.exception.code, 1)
                self.assertIn("rs-event-bus-redis", errors.getvalue())
                self.assertIn(status.strip(), errors.getvalue())
                self.assertFalse(any(call[0] == "cargo" for call in calls))

    def test_status_command_failure_is_rejected_with_repository_name(self):
        calls = []
        def invoke(command):
            calls.append(command)
            if command[0] == "git" and command[3] == "rev-parse":
                repository = Path(command[2]).name
                revision = next(item["revision"] for item in self.config["repositories"]
                                if item["name"] == repository)
                return subprocess.CompletedProcess(command, 0, revision + "\n", "")
            return subprocess.CompletedProcess(command, 128, "", "index unavailable")
        with mock.patch.object(gate, "run_command", side_effect=invoke):
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as errors:
                with self.assertRaises(SystemExit) as exit_result:
                    gate.main(["--ecosystem-root", str(self.root), "--manifest", str(self.manifest_path)])
        self.assertEqual(exit_result.exception.code, 1)
        self.assertIn("rs-event-bus-redis", errors.getvalue())
        self.assertIn("index unavailable", errors.getvalue())
        self.assertFalse(any(call[0] == "cargo" for call in calls))

    def test_core_checkout_may_be_dirty(self):
        calls = []
        (self.root / "rs-event-bus" / "uncommitted-core-change").write_text("under test\n")
        with mock.patch.object(gate, "run_command", side_effect=self.command_runner(calls)):
            core, _ = gate.validate_layout(self.root, {"repositories": self.config["repositories"], "targets": []})
        self.assertEqual(core, self.root / "rs-event-bus")

    def test_real_git_checkout_status_detects_staged_unstaged_and_untracked_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkout = root / "rs-task"
            checkout.mkdir()
            def git(*args):
                return subprocess.run(["git", "-C", str(checkout), *args], check=True,
                                      capture_output=True, text=True)
            git("init", "-q")
            git("config", "user.name", "Compatibility Test")
            git("config", "user.email", "compatibility@example.invalid")
            (checkout / "tracked.txt").write_text("base\n", encoding="utf-8")
            git("add", "tracked.txt")
            git("commit", "-qm", "initial")
            revision = git("rev-parse", "HEAD").stdout.strip()
            config = {"repositories": [{"name": "rs-task", "revision": revision}], "targets": []}
            (root / "rs-event-bus").mkdir()
            (root / "rs-event-bus" / "Cargo.toml").write_text("[package]\n", encoding="utf-8")
            self.assertEqual(gate.validate_layout(root, config)[0], root / "rs-event-bus")
            (checkout / "tracked.txt").write_text("staged\n", encoding="utf-8")
            git("add", "tracked.txt")
            with self.assertRaisesRegex(gate.CompatibilityError, "rs-task.*local changes"):
                gate.validate_layout(root, config)
            git("reset", "-q", "HEAD")
            (checkout / "tracked.txt").write_text("unstaged\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.CompatibilityError, "rs-task.*local changes"):
                gate.validate_layout(root, config)
            git("checkout", "--", "tracked.txt")
            (checkout / "untracked file.txt").write_text("untracked\n", encoding="utf-8")
            with self.assertRaisesRegex(gate.CompatibilityError, "(?s)rs-task.*untracked file.txt"):
                gate.validate_layout(root, config)
            exclude = checkout / ".git" / "info" / "exclude"
            exclude.write_text("target/\n", encoding="utf-8")
            (checkout / "target").mkdir()
            (checkout / "target" / "ignored").write_text("generated\n", encoding="utf-8")
            (checkout / "untracked file.txt").unlink()
            self.assertEqual(gate.validate_layout(root, config)[0], root / "rs-event-bus")


if __name__ == "__main__":
    unittest.main()
