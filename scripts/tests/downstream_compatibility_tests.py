import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from scripts.check_downstream_compatibility import cargo_config
from scripts.check_downstream_compatibility import TESTS
from scripts.check_downstream_compatibility import discover_local_repositories
from scripts.check_downstream_compatibility import main
from scripts.check_downstream_compatibility import metadata
from scripts.check_downstream_compatibility import run
from scripts.check_downstream_compatibility import validate_graph_for_downstream
from scripts.check_downstream_compatibility import verify_test
from scripts.tests.process_test_support import wait_for_process_exit


class DownstreamCompatibilityTests(unittest.TestCase):
    def test_test_configurations_record_relative_manifest_target_and_exact_test_name(self):
        configurations = {(entry.root, entry.manifest, entry.target, entry.test_name)
                          for entry in TESTS}
        self.assertEqual(configurations, {
            ("redis", "Cargo.toml", "sync_standalone_tests", "test_sync_spi_conformance"),
            ("redis", "Cargo.toml", "async_standalone_tests", "test_async_spi_conformance"),
            ("rs-task", "Cargo.toml", "typed_service_tests",
             "service::typed_task_execution_tests::test_cancelled_submit_wakes_durable_outbox_publisher"),
            ("redis", "Cargo.toml", "wire_limit_tests",
             "durable::test_real_core_codec_failures_leave_redis_durable_record_recoverable"),
            ("execution-services", "tests/fixtures/ioc_application_consumer/Cargo.toml",
             "managed_event_bus_tests", "test_blocked_handler_graceful_request_cancel_resume"),
            ("execution-services", "tests/fixtures/ioc_application_consumer/Cargo.toml",
             "managed_event_bus_tests", "test_blocked_handler_request_cancel_resume_and_abort_upgrade"),
            ("execution-services", "tests/fixtures/ioc_application_consumer/Cargo.toml",
             "managed_event_bus_tests", "test_provider_timed_out_report_reaches_container_diagnostic"),
        })

    def test_execution_services_discovers_recursive_path_dependencies_and_patches(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            execution = parent / "rs-execution-services"
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            (fixture / "Cargo.toml").write_text(
                '[dependencies]\nlocal-a = { path = "../../../../rs-local-a" }\n'
                '[patch.crates-io]\nlocal-b = { path = "../../../../rs-local-b" }\n')
            local_a = parent / "rs-local-a"
            local_a.mkdir()
            (local_a / "Cargo.toml").write_text(
                '[dependencies]\nlocal-c = { path = "../rs-local-c" }\n')
            for name in ("rs-local-b", "rs-local-c"):
                (parent / name).mkdir()
                (parent / name / "Cargo.toml").write_text("[package]\nname='local'\n")

            discovered = discover_local_repositories(fixture / "Cargo.toml", parent)

            self.assertEqual(set(discovered), {execution.resolve(), local_a.resolve(),
                                               (parent / "rs-local-b").resolve(),
                                               (parent / "rs-local-c").resolve()})

    def test_execution_services_rejects_missing_and_out_of_bounds_path_with_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            fixture = parent / "rs-execution-services/tests/fixtures/consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            manifest.write_text('[dependencies]\nmissing = { path = "../../../../missing" }\n')
            with self.assertRaisesRegex(ValueError, r"Cargo.toml.*missing"):
                discover_local_repositories(manifest, parent)

            manifest.write_text('[dependencies]\noutside = { path = "../../../../../outside" }\n')
            with self.assertRaisesRegex(ValueError, r"Cargo.toml.*outside"):
                discover_local_repositories(manifest, parent)

    def test_cargo_patch_points_at_requested_core_and_redis_checkouts(self):
        args = cargo_config(Path("/core checkout"), Path("/redis checkout"))
        self.assertEqual(args, [
            "--config", 'patch.crates-io.qubit-event-bus.path="/core checkout"',
            "--config", 'patch.crates-io.qubit-event-bus-redis.path="/redis checkout"',
        ])

    def test_graph_requires_exactly_the_requested_local_core_manifest(self):
        graph = {
            "workspace_members": ["consumer 1.0.0 (path+file:///consumer)"],
            "resolve": {"nodes": [{
                "id": "consumer 1.0.0 (path+file:///consumer)",
                "dependencies": ["qubit-event-bus 0.21.0 (path+file:///core)",
                                 "qubit-event-bus-redis 0.7.0 (path+file:///redis)"],
            }, {
                "id": "qubit-event-bus 0.21.0 (path+file:///core)",
                "dependencies": [],
            }, {
                "id": "qubit-event-bus-redis 0.7.0 (path+file:///redis)",
                "dependencies": [],
            }]},
            "packages": [{
                "id": "qubit-event-bus 0.21.0 (path+file:///core)",
                "name": "qubit-event-bus",
                "version": "0.21.0",
                "source": None,
                "manifest_path": "/core/Cargo.toml",
            }, {
                "id": "qubit-event-bus-redis 0.7.0 (path+file:///redis)",
                "name": "qubit-event-bus-redis",
                "version": "0.7.0",
                "source": None,
                "manifest_path": "/redis/Cargo.toml",
            }],
        }
        validate_graph_for_downstream(graph, expected_core=Path("/core"), expected_redis=Path("/redis"))

        graph["packages"][0]["manifest_path"] = "/other-core/Cargo.toml"
        with self.assertRaisesRegex(ValueError, "expected local core"):
            validate_graph_for_downstream(graph, expected_core=Path("/core"),
                                          expected_redis=Path("/redis"))
        graph["packages"][0]["manifest_path"] = "/core/Cargo.toml"
        graph["packages"][0]["source"] = "registry+https://github.com/rust-lang/crates.io-index"
        with self.assertRaisesRegex(ValueError, "expected local core"):
            validate_graph_for_downstream(graph, expected_core=Path("/core"),
                                          expected_redis=Path("/redis"))
        graph["packages"][0]["source"] = None

        graph["packages"][1]["manifest_path"] = "/other-redis/Cargo.toml"
        with self.assertRaisesRegex(ValueError, "expected local Redis provider"):
            validate_graph_for_downstream(
                graph, expected_core=Path("/core"), expected_redis=Path("/redis"))
        graph["packages"][1]["manifest_path"] = "/redis/Cargo.toml"
        graph["packages"][1]["source"] = "registry+https://github.com/rust-lang/crates.io-index"
        with self.assertRaisesRegex(ValueError, "expected local Redis provider"):
            validate_graph_for_downstream(
                graph, expected_core=Path("/core"), expected_redis=Path("/redis"))

    def test_test_listing_and_execution_both_use_locked_resolution(self):
        commands = []

        def fake_run(command, **kwargs):
            commands.append(command)
            if "--list" in command:
                return "test_sync_spi_conformance: test\n"
            return ("running 1 test\ntest test_sync_spi_conformance ... ok\n"
                    "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; "
                    "0 filtered out; finished in 0.00s\n")

        with patch("scripts.check_downstream_compatibility.run", side_effect=fake_run):
            verify_test(Path("/redis"), "test_sync_spi_conformance",
                        ["--all-features", "--test", "sync_standalone_tests",
                         "test_sync_spi_conformance"], Path("/core"), None, {})

        self.assertEqual(len(commands), 2)
        self.assertTrue(all("--locked" in command for command in commands))
        self.assertIn("--format", commands[1])
        self.assertIn("pretty", commands[1])
        self.assertIn("--color", commands[1])
        self.assertIn("never", commands[1])

    def test_test_execution_requires_one_successful_target_result(self):
        valid = ("running 1 test\ntest test_sync_spi_conformance ... ok\n"
                 "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; "
                 "2 filtered out; finished in 0.00s\n")
        cases = (
            ("ignored", "test test_sync_spi_conformance ... ignored\n"
             "test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out\n"),
            ("zero execution", "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out\n"),
            ("another test passed", "test unrelated ... ok\n"
             "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n"),
            ("duplicate target", valid.replace("test result:",
             "test test_sync_spi_conformance ... ok\ntest result:")),
            ("missing summary", "test test_sync_spi_conformance ... ok\n"),
            ("ambiguous summaries", valid + valid.split("test result:", 1)[1].join(["test result:", ""])),
            ("extra malformed summary", valid + "test result: ok. 0 passed\n"),
            ("extra execution", valid.replace("1 passed; 0 failed", "2 passed; 0 failed")),
        )
        for label, output in (("valid", valid), *cases):
            with self.subTest(output=label):
                results = iter(("test_sync_spi_conformance: test\n", output))
                with patch("scripts.check_downstream_compatibility.run",
                           side_effect=lambda *_args, **_kwargs: next(results)):
                    if label == "valid":
                        verify_test(Path("/redis"), "test_sync_spi_conformance",
                                    ["--test", "sync_standalone_tests",
                                     "test_sync_spi_conformance"], Path("/core"), None, {})
                    else:
                        with self.assertRaisesRegex(RuntimeError, "test_sync_spi_conformance"):
                            verify_test(Path("/redis"), "test_sync_spi_conformance",
                                        ["--test", "sync_standalone_tests",
                                         "test_sync_spi_conformance"], Path("/core"), None, {})

    def test_metadata_updates_snapshot_then_validates_locked_graph(self):
        commands = []

        def fake_run(command, **kwargs):
            commands.append((command, kwargs))
            return "{}"

        root = Path("/snapshot/arbitrary-project-name")
        with patch("scripts.check_downstream_compatibility.run", side_effect=fake_run), \
                patch("scripts.check_downstream_compatibility.validate_graph_for_downstream") as validate:
            graph = metadata(root, "sqlite,event-bus", Path("/core"),
                              Path("/snapshot/redis"), Path("/target"),
                              expected_redis=Path("/snapshot/redis"))

        self.assertEqual(graph, {})
        self.assertEqual(len(commands), 2)
        self.assertNotIn("--locked", commands[0][0])
        self.assertIn("--locked", commands[1][0])
        self.assertEqual(commands[0][1]["cwd"], root)
        self.assertEqual(commands[0][1]["env"]["CARGO_TARGET_DIR"], "/target")
        self.assertEqual(validate.call_args.kwargs["expected_core"], Path("/core"))
        self.assertEqual(validate.call_args.kwargs["expected_redis"], Path("/snapshot/redis"))

    def test_metadata_runs_real_graph_validation_for_arbitrary_root_name(self):
        core_id = "qubit-event-bus 0.21.0 (path+file:///core)"
        redis_id = "qubit-event-bus-redis 0.7.0 (path+file:///redis)"
        consumer_id = "consumer 1.0.0 (path+file:///consumer)"
        graph = {
            "workspace_members": [consumer_id],
            "resolve": {"nodes": [
                {"id": consumer_id, "dependencies": [core_id, redis_id]},
                {"id": core_id, "dependencies": []},
                {"id": redis_id, "dependencies": []},
            ]},
            "packages": [
                {"id": core_id, "name": "qubit-event-bus", "version": "0.21.0",
                 "source": None, "manifest_path": "/core/Cargo.toml"},
                {"id": redis_id, "name": "qubit-event-bus-redis", "version": "0.7.0",
                 "source": None, "manifest_path": "/redis/Cargo.toml"},
            ],
        }
        root = Path("/snapshot/arbitrary-project-name")
        with patch("scripts.check_downstream_compatibility.run", return_value=json.dumps(graph)) as run_command:
            result = metadata(root, "sqlite,event-bus", Path("/core"), Path("/redis"),
                              Path("/target"), expected_redis=Path("/redis"))
        self.assertEqual(result, graph)
        self.assertEqual(run_command.call_count, 2)

    def test_runner_returns_json_stdout_and_reports_failed_command_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = run([sys.executable, "-c", "print('{\\\"packages\\\": []}')"],
                         cwd=directory, env=os.environ.copy(), label="metadata-json",
                         print_stdout=False)
            self.assertEqual(output, '{"packages": []}\n')

            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaisesRegex(RuntimeError, "exited 7"):
                    run([sys.executable, "-c", "print('partial'); raise SystemExit(7)"],
                        cwd=directory, env=os.environ.copy(), label="failed-cargo")
            self.assertIn("partial", stdout.getvalue())

    def test_keyboard_interrupt_terminates_and_reaps_started_process(self):
        class InterruptedProcess:
            pid = 12345
            returncode = None

            def wait(self, timeout):
                raise KeyboardInterrupt

        with tempfile.TemporaryDirectory() as directory, \
                patch("scripts.check_downstream_compatibility.subprocess.Popen",
                      return_value=InterruptedProcess()), \
                patch("scripts.check_downstream_compatibility.terminate_process_group") as terminate, \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                run(["cargo", "test"], cwd=directory, env=os.environ.copy(), label="interrupted")
        terminate.assert_called_once()

    @unittest.skipUnless(os.name == "posix", "process group cleanup is POSIX-specific")
    def test_timeout_terminates_descendants_and_preserves_captured_output(self):
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "grandchild.pid"
            code = (
                "import subprocess,sys,time; "
                "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
                f"open({str(pid_file)!r},'w').write(str(p.pid)); "
                "print('started', flush=True); time.sleep(30)"
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaisesRegex(RuntimeError, "timed out"):
                    run([sys.executable, "-c", code], cwd=directory,
                        env=os.environ.copy(), label="timeout-fixture", timeout=0.3)

            self.assertIn("started", stdout.getvalue())
            self.assertIn("timed out after 0.3s", stderr.getvalue())
            child_pid = int(pid_file.read_text())
            self.assertTrue(wait_for_process_exit(child_pid),
                            f"Cargo-like descendant {child_pid} survived command timeout")

    def make_input_repository(self, root, name):
        root.mkdir()
        subprocess.run(["git", "init", "-q", str(root)], check=True)
        subprocess.run(["git", "-C", str(root), "config", "user.name", "Snapshot Test"], check=True)
        subprocess.run(["git", "-C", str(root), "config", "user.email", "snapshot@example.invalid"], check=True)
        (root / "Cargo.toml").write_text(f"[package]\nname = \"{name}\"\nversion = \"0.1.0\"\n")
        (root / "Cargo.lock").write_bytes(b"original lock\n")
        (root / "src").mkdir()
        (root / "src/lib.rs").write_text("pub fn tracked() {}\n")
        subprocess.run(["git", "-C", str(root), "add", "Cargo.toml", "Cargo.lock", "src/lib.rs"], check=True)
        subprocess.run(["git", "-C", str(root), "commit", "-qm", "baseline"], check=True)
        (root / "src/lib.rs").write_text("pub fn locally_edited() {}\n")
        return root

    def run_main_with_mocks(self, redis, task, metadata_side_effect, verify_side_effect=None):
        argv = ["check_downstream_compatibility.py", "--redis-root", str(redis),
                "--task-root", str(task)]
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch("sys.argv", argv), \
                patch("scripts.check_downstream_compatibility.metadata",
                      side_effect=metadata_side_effect), \
                patch("scripts.check_downstream_compatibility.verify_test",
                      side_effect=verify_side_effect), \
                contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            main()
        return stdout.getvalue(), stderr.getvalue()

    def test_successful_downstream_run_only_updates_snapshot_locks(self):
        with tempfile.TemporaryDirectory() as directory:
            redis = self.make_input_repository(Path(directory) / "redis", "redis")
            task = self.make_input_repository(Path(directory) / "rs-task", "rs-task")
            for root in (redis, task):
                (root / "Cargo.lock").write_bytes(b"locally edited tracked lock\n")
            original = {root: ((root / "Cargo.lock").read_bytes(), (root / "Cargo.lock").stat().st_mtime_ns)
                        for root in (redis, task)}
            snapshot_roots = []

            def metadata_snapshot(root, _features, _core, redis_patch, _target, **_kwargs):
                self.assertNotIn(root, (redis, task))
                self.assertEqual((root / "Cargo.lock").read_bytes(), b"locally edited tracked lock\n")
                self.assertEqual((root / "src/lib.rs").read_text(), "pub fn locally_edited() {}\n")
                snapshot_roots.append(root)
                with (root / "Cargo.lock").open("ab") as lock:
                    lock.write(b"resolved snapshot\n")
                if root.name == "rs-task":
                    self.assertEqual(redis_patch, snapshot_roots[0])
                return {}

            self.run_main_with_mocks(redis, task, metadata_snapshot)

            self.assertEqual({root: ((root / "Cargo.lock").read_bytes(),
                                     (root / "Cargo.lock").stat().st_mtime_ns)
                              for root in (redis, task)}, original)
            self.assertEqual([root.name for root in snapshot_roots], ["redis", "rs-task"])

    def test_failed_downstream_run_does_not_restore_or_modify_input_locks(self):
        with tempfile.TemporaryDirectory() as directory:
            redis = self.make_input_repository(Path(directory) / "redis", "redis")
            task = self.make_input_repository(Path(directory) / "rs-task", "rs-task")
            original = {root: ((root / "Cargo.lock").read_bytes(), (root / "Cargo.lock").stat().st_mtime_ns)
                        for root in (redis, task)}

            def metadata_snapshot(root, *_args, **_kwargs):
                with (root / "Cargo.lock").open("ab") as lock:
                    lock.write(b"resolved snapshot\n")
                return {}

            with self.assertRaisesRegex(RuntimeError, "synthetic downstream failure"):
                self.run_main_with_mocks(
                    redis, task, metadata_snapshot,
                    lambda *_args, **_kwargs: (_ for _ in ()).throw(
                        RuntimeError("synthetic downstream failure")),
                )

            self.assertEqual({root: ((root / "Cargo.lock").read_bytes(),
                                     (root / "Cargo.lock").stat().st_mtime_ns)
                              for root in (redis, task)}, original)

    def test_execution_services_missing_local_path_leaves_all_input_files_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            redis = self.make_input_repository(base / "rs-event-bus-redis", "redis")
            task = self.make_input_repository(base / "rs-task", "rs-task")
            execution = self.make_input_repository(base / "rs-execution-services", "execution")
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            manifest.write_text('[dependencies]\nmissing = { path = "../../../../missing" }\n')
            subprocess.run(["git", "-C", str(execution), "add", str(manifest.relative_to(execution))],
                           check=True)
            subprocess.run(["git", "-C", str(execution), "commit", "-qm", "fixture"], check=True)
            inputs = (redis, task, execution)
            original = {path: {file.relative_to(path): (file.read_bytes(), file.stat().st_mtime_ns)
                               for file in path.rglob("*") if file.is_file() and ".git" not in file.parts}
                        for path in inputs}
            argv = ["check_downstream_compatibility.py", "--redis-root", str(redis),
                    "--task-root", str(task), "--execution-services-root", str(execution)]
            with patch("sys.argv", argv):
                with self.assertRaisesRegex(ValueError, "missing local path") as error:
                    main()
            self.assertIn(str(manifest), str(error.exception))
            self.assertEqual(original, {
                path: {file.relative_to(path): (file.read_bytes(), file.stat().st_mtime_ns)
                       for file in path.rglob("*") if file.is_file() and ".git" not in file.parts}
                for path in inputs
            })

    def test_execution_services_snapshot_preserves_nested_relative_dependency_layout(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            redis = self.make_input_repository(base / "rs-event-bus-redis", "redis")
            task = self.make_input_repository(base / "rs-task", "rs-task")
            execution = self.make_input_repository(base / "rs-execution-services", "execution")
            sibling = self.make_input_repository(base / "rs-local-dependency", "local")
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            manifest.write_text('[dependencies]\nlocal = { path = "../../../../rs-local-dependency" }\n')
            subprocess.run(["git", "-C", str(execution), "add", str(manifest.relative_to(execution))],
                           check=True)
            subprocess.run(["git", "-C", str(execution), "commit", "-qm", "fixture"], check=True)
            argv = ["check_downstream_compatibility.py", "--redis-root", str(redis),
                    "--task-root", str(task), "--execution-services-root", str(execution)]
            seen = []

            def metadata_snapshot(root, _features, _core, _redis, _target, **_kwargs):
                if root.name == "rs-execution-services":
                    dependency = root.parent / "rs-local-dependency"
                    self.assertTrue((dependency / "Cargo.toml").is_file())
                    self.assertEqual((dependency / "src/lib.rs").read_text(),
                                     "pub fn locally_edited() {}\n")
                    self.assertTrue((root / "tests/fixtures/ioc_application_consumer/Cargo.toml").is_file())
                    seen.append(root)
                return {}

            with patch("sys.argv", argv), \
                    patch("scripts.check_downstream_compatibility.metadata",
                          side_effect=metadata_snapshot), \
                    patch("scripts.check_downstream_compatibility.verify_test") as verify:
                main()
            self.assertEqual(len(seen), 1)
            self.assertEqual(verify.call_count, 7)

    def test_worktrees_alias_uses_tracked_snapshots_and_does_not_touch_input_locks(self):
        with tempfile.TemporaryDirectory() as directory:
            core = self.make_input_repository(Path(directory) / "core", "core")
            redis = self.make_input_repository(Path(directory) / "redis", "redis")
            task = self.make_input_repository(Path(directory) / "rs-task", "rs-task")
            input_locks = {root: (root / "Cargo.lock").read_bytes() for root in (redis, task)}
            argv = ["check_downstream_compatibility.py", "--use-worktrees",
                    "--core-root", str(core), "--redis-root", str(redis),
                    "--task-root", str(task)]
            metadata_roots = []

            def capture_metadata(root, _features, requested_core, redis_patch, _target,
                                 *, expected_redis=None, manifest=None, all_features=False):
                metadata_roots.append(root)
                self.assertNotIn(root, (redis, task))
                self.assertEqual((root / "src/lib.rs").read_text(),
                                 "pub fn locally_edited() {}\n")
                self.assertEqual(requested_core, core.resolve())
                if root.name == "redis":
                    self.assertIsNone(expected_redis)
                    # Simulate an input lockfile being changed while Cargo runs.
                    (redis / "Cargo.lock").write_bytes(b"concurrent external update\n")
                else:
                    self.assertEqual(redis_patch, metadata_roots[0])
                    self.assertEqual(expected_redis, metadata_roots[0])
                with (root / "Cargo.lock").open("ab") as lock:
                    lock.write(b"snapshot resolution\n")
                return {}

            stdout = io.StringIO()
            with patch("sys.argv", argv), \
                    patch("scripts.check_downstream_compatibility.metadata",
                          side_effect=capture_metadata), \
                    patch("scripts.check_downstream_compatibility.verify_test",
                          side_effect=lambda root, *_args, **_kwargs: self.assertIn(
                              b"snapshot resolution", (root / "Cargo.lock").read_bytes())) as verify, \
                    contextlib.redirect_stdout(stdout):
                main()

            self.assertEqual([root.name for root in metadata_roots], ["redis", "rs-task"])
            self.assertEqual(verify.call_count, 4)
            self.assertIn("IoC checks not selected", stdout.getvalue())
            self.assertEqual((redis / "Cargo.lock").read_bytes(),
                             b"concurrent external update\n")
            self.assertEqual((task / "Cargo.lock").read_bytes(), input_locks[task])
            self.assertIn("downstream compatibility checks passed", stdout.getvalue())

    def test_worktrees_alias_preserves_generated_input_lock_after_metadata_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            core = self.make_input_repository(Path(directory) / "core", "core")
            redis = self.make_input_repository(Path(directory) / "redis", "redis")
            task = self.make_input_repository(Path(directory) / "rs-task", "rs-task")
            (redis / "Cargo.lock").unlink()
            task_lock = (task / "Cargo.lock").read_bytes()
            argv = ["check_downstream_compatibility.py", "--use-worktrees",
                    "--core-root", str(core), "--redis-root", str(redis),
                    "--task-root", str(task)]

            def fail_metadata(*_args, **_kwargs):
                (redis / "Cargo.lock").write_bytes(b"generated externally during metadata\n")
                raise RuntimeError("synthetic metadata parse failure")

            with patch("sys.argv", argv), \
                    patch("scripts.check_downstream_compatibility.metadata",
                          side_effect=fail_metadata):
                with self.assertRaisesRegex(RuntimeError, "metadata parse failure"):
                    main()

            self.assertEqual((redis / "Cargo.lock").read_bytes(),
                             b"generated externally during metadata\n")
            self.assertEqual((task / "Cargo.lock").read_bytes(), task_lock)

    def test_cli_help_describes_snapshot_compatibility_alias(self):
        with patch("sys.argv", ["check_downstream_compatibility.py", "--help"]), \
                contextlib.redirect_stdout(io.StringIO()) as stdout:
            with self.assertRaises(SystemExit) as error:
                main()
        self.assertEqual(error.exception.code, 0)
        self.assertIn("compatibility alias", stdout.getvalue())
        self.assertIn("tracked-source snapshots", stdout.getvalue())
        self.assertIn("--execution-services-root", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
