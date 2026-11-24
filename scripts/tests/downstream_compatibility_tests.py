import builtins
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
from scripts.downstream_source_snapshot import copy_tracked_source
from scripts.check_downstream_compatibility import TESTS
from scripts.check_downstream_compatibility import discover_local_repositories
from scripts.check_downstream_compatibility import main
from scripts.check_downstream_compatibility import metadata
from scripts.check_downstream_compatibility import run
from scripts.check_downstream_compatibility import run_downstream_checks
from scripts.check_downstream_compatibility import validate_graph_for_downstream
from scripts.check_downstream_compatibility import verify_test
from scripts.tests.process_test_support import wait_for_process_exit


class DownstreamCompatibilityTests(unittest.TestCase):
    def test_legacy_cli_and_help_do_not_require_tomllib(self):
        original_import = builtins.__import__

        def import_without_tomllib(name, *args, **kwargs):
            if name == "tomllib":
                raise ModuleNotFoundError("No module named 'tomllib'", name="tomllib")
            return original_import(name, *args, **kwargs)

        with patch("sys.argv", ["check_downstream_compatibility.py", "--help"]), \
                patch("builtins.__import__", side_effect=import_without_tomllib):
            with self.assertRaises(SystemExit) as help_exit:
                main()
            self.assertEqual(help_exit.exception.code, 0)

        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            core = self.make_input_repository(base / "core", "core")
            redis = self.make_input_repository(base / "redis", "redis")
            task = self.make_input_repository(base / "task", "task")
            argv = ["check_downstream_compatibility.py", "--core-root", str(core),
                    "--redis-root", str(redis), "--task-root", str(task)]
            with patch("sys.argv", argv), patch("builtins.__import__",
                                               side_effect=import_without_tomllib), \
                    patch("scripts.check_downstream_compatibility.metadata", return_value={}), \
                    patch("scripts.check_downstream_compatibility.verify_test"):
                main()

            execution = self.make_input_repository(base / "execution", "execution")
            argv = ["check_downstream_compatibility.py", "--core-root", str(core),
                    "--redis-root", str(redis), "--task-root", str(task),
                    "--execution-services-root", str(execution)]
            with patch("sys.argv", argv), patch("builtins.__import__",
                                               side_effect=import_without_tomllib), \
                    patch("scripts.check_downstream_compatibility.copy_tracked_source") as copy_source:
                with self.assertRaisesRegex(ValueError, "requires Python 3.11 or newer"):
                    main()
            copy_source.assert_not_called()

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

    def test_cargo_1_94_uses_only_preferred_tracked_config_filename(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="ioc-consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nvalid-local="1.0.0"\n')
            (fixture / "src").mkdir()
            (fixture / "src/lib.rs").write_text("pub fn fixture() {}\n")
            config_dir = execution / ".cargo"
            config_dir.mkdir()
            (config_dir / "config").write_text(
                '[patch.crates-io]\nvalid-local = { path = "../rs-valid-local" }\n')
            (config_dir / "config.toml").write_text(
                '[patch.crates-io]\nmissing-local = { path = "../does-not-exist" }\n')
            dependency = self.make_input_repository(parent / "rs-valid-local", "valid-local")
            (dependency / "Cargo.toml").write_text(
                '[package]\nname="valid-local"\nversion="1.0.0"\nedition="2021"\n')
            for root in (execution, dependency):
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
                subprocess.run(["git", "-C", str(root), "commit", "-qm", "fixture"], check=True)

            self._cargo_metadata_offline(manifest)
            discovered = discover_local_repositories(manifest, parent, execution)
            self.assertEqual(set(discovered), {execution.resolve(), dependency.resolve()})

    def test_cargo_1_94_ignores_transitive_workspace_patch_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            dependency = self.make_input_repository(parent / "rs-local-dependency", "local-dependency")
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="ioc-consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nlocal-dependency={path="../../../../rs-local-dependency"}\n')
            (fixture / "src").mkdir()
            (fixture / "src/lib.rs").write_text("pub fn fixture() {}\n")
            (dependency / "Cargo.toml").write_text(
                '[package]\nname="local-dependency"\nversion="0.1.0"\nedition="2021"\n'
                '[patch.crates-io]\nmissing-patch={path="../missing-patch"}\n')
            for root in (execution, dependency):
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
                subprocess.run(["git", "-C", str(root), "commit", "-qm", "fixture"], check=True)

            self._cargo_metadata_offline(manifest)
            discovered = discover_local_repositories(manifest, parent, execution)
            self.assertEqual(set(discovered), {execution.resolve(), dependency.resolve()})

    def _cargo_metadata_offline(self, manifest, *, locked=False):
        command = ["cargo", "metadata", "--format-version", "1", "--offline",
                   "--manifest-path", str(manifest)]
        if locked:
            command.append("--locked")
        result = subprocess.run(command, cwd=manifest.parent, capture_output=True,
                                text=True, check=False)
        if result.returncode:
            raise AssertionError(
                f"Cargo metadata exited {result.returncode}: {result.stderr.strip()}")
        return result

    def _assert_cargo_snapshot_discovers_only_reachable_packages(self, parent, fixture_manifest,
                                                                  dependency_name):
        source_graph = json.loads(self._cargo_metadata_offline(fixture_manifest).stdout)
        self._cargo_metadata_offline(fixture_manifest, locked=True)
        snapshots_root = parent.parent / "snapshot"
        execution = parent / "rs-execution-services"
        discovered = discover_local_repositories(fixture_manifest, parent, execution)
        for repository in discovered:
            destination = snapshots_root / "siblings" / repository.relative_to(parent)
            copy_tracked_source(repository, destination)
        relative = fixture_manifest.parent.relative_to(execution)
        snapshot_manifest = (snapshots_root / "siblings" / execution.name / relative /
                            "Cargo.toml")
        snapshot_result = self._cargo_metadata_offline(snapshot_manifest, locked=True)
        snapshot_graph = json.loads(snapshot_result.stdout)
        self.assertEqual(set(discovered), {
            (parent / "rs-execution-services").resolve(),
            (parent / ("rs-workspace-dependency" if dependency_name == "workspace-dependency"
                       else "rs-config-dependency")).resolve(),
        })
        source_package = next(package for package in source_graph["packages"]
                              if package["name"] == dependency_name)
        snapshot_package = next(package for package in snapshot_graph["packages"]
                                if package["name"] == dependency_name)
        self.assertTrue(Path(source_package["manifest_path"]).is_relative_to(parent))
        self.assertTrue(Path(snapshot_package["manifest_path"]).is_relative_to(snapshots_root))

    def test_cargo_1_94_workspace_inherited_path_dependency_is_snapshotted(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            dependency = self.make_input_repository(parent / "rs-workspace-dependency", "workspace-dependency")
            unused = self.make_input_repository(parent / "rs-unused-workspace-dependency", "unused")
            (dependency / "Cargo.toml").write_text(
                '[package]\nname="workspace-dependency"\nversion="0.1.0"\n'
                'edition="2021"\n')
            (dependency / "Cargo.lock").write_text("")
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["tests/fixtures/ioc_application_consumer"]\n'
                '[workspace.dependencies]\nworkspace-dependency={path="../rs-workspace-dependency"}\n'
                'unused={path="../rs-unused-workspace-dependency"}\n')
            (execution / "Cargo.lock").write_text("")
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            (fixture / "Cargo.toml").write_text(
                '[package]\nname="ioc-consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nworkspace-dependency={workspace=true}\n')
            (fixture / "src").mkdir()
            (fixture / "src/lib.rs").write_text("pub fn fixture() {}\n")
            decoy = execution / "tests/fixtures/rs-workspace-dependency"
            decoy.mkdir()
            (decoy / "Cargo.toml").write_text(
                '[package]\nname="workspace-dependency"\nversion="0.1.0"\n'
                'edition="2021"\n')
            for root in (execution, dependency, unused):
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
                subprocess.run(["git", "-C", str(root), "commit", "-qm", "cargo fixture"], check=True)
            self._assert_cargo_snapshot_discovers_only_reachable_packages(
                parent, fixture / "Cargo.toml", "workspace-dependency")

    def test_discovery_respects_non_default_workspace_root_and_nested_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            for name in ("rs-non-default-dep", "rs-nested-dep", "rs-unreferenced"):
                self.make_input_repository(parent / name, name.removeprefix("rs-"))
            workspace = execution / "workspace"
            member = workspace / "members/consumer"
            member.mkdir(parents=True)
            (workspace / "Cargo.toml").write_text(
                '[workspace]\nmembers=["members/consumer"]\n'
                '[workspace.dependencies]\nnon-default={path="../../rs-non-default-dep"}\n')
            (member / "Cargo.toml").write_text(
                '[package]\nname="consumer"\nversion="0.1.0"\nedition="2021"\n'
                'workspace="../.."\n[dependencies]\nnon-default={workspace=true}\n')
            nested = execution / "tests/fixtures/nested-consumer"
            nested.mkdir(parents=True)
            (nested / "Cargo.toml").write_text(
                '[package]\nname="nested-consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\n[workspace.dependencies]\nnested={path="../../../../rs-nested-dep"}\n'
                '[dependencies]\nnested={workspace=true}\n')
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["workspace/members/consumer", "tests/fixtures/**"]\n'
                '[workspace.dependencies]\nunreferenced={path="../rs-unreferenced"}\n')

            found_member = set(discover_local_repositories(member / "Cargo.toml", parent))
            found_nested = set(discover_local_repositories(nested / "Cargo.toml", parent))
            self.assertEqual(found_member, {
                execution.resolve(), (parent / "rs-non-default-dep").resolve()})
            self.assertEqual(found_nested, {
                execution.resolve(), (parent / "rs-nested-dep").resolve()})

    def test_cargo_1_94_discovers_implicit_path_dependency_workspace_member(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            dependency = self.make_input_repository(parent / "rs-workspace-dependency",
                                                   "workspace-dependency")
            (dependency / "Cargo.toml").write_text(
                '[package]\nname="workspace-dependency"\nversion="0.1.0"\n'
                'edition="2021"\n')
            (dependency / "Cargo.lock").write_text("")
            package = execution / "packages/implicit-member"
            package.mkdir(parents=True)
            (package / "Cargo.toml").write_text(
                '[package]\nname="implicit-member"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nworkspace-dependency={workspace=true}\n')
            (package / "src").mkdir()
            (package / "src/lib.rs").write_text("pub fn fixture() {}\n")
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=[]\n'
                '[workspace.dependencies]\nworkspace-dependency={path="../rs-workspace-dependency"}\n'
                '[dependencies]\nimplicit-member={path="packages/implicit-member"}\n')
            (execution / "Cargo.lock").write_text("")
            for root in (execution, dependency):
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
                subprocess.run(["git", "-C", str(root), "commit", "-qm", "cargo fixture"], check=True)
            self._assert_cargo_snapshot_discovers_only_reachable_packages(
                parent, package / "Cargo.toml", "workspace-dependency")

    def test_cargo_1_94_transitive_inherited_path_from_implicit_member_is_discovered(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            dependency = self.make_input_repository(parent / "rs-transitive-dependency", "transitive")
            (dependency / "Cargo.toml").write_text(
                '[package]\nname="transitive-dependency"\nversion="0.1.0"\n'
                'edition="2021"\n')
            (dependency / "Cargo.lock").write_text("")
            consumer = execution / "members/consumer"
            consumer.mkdir(parents=True)
            (consumer / "Cargo.toml").write_text(
                '[package]\nname="consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nimplicit-member={workspace=true}\n')
            (consumer / "src").mkdir()
            (consumer / "src/lib.rs").write_text("pub fn consumer() {}\n")
            implicit_member = execution / "packages/implicit-member"
            implicit_member.mkdir(parents=True)
            (implicit_member / "Cargo.toml").write_text(
                '[package]\nname="implicit-member"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\ntransitive-dependency={workspace=true}\n')
            (implicit_member / "src").mkdir()
            (implicit_member / "src/lib.rs").write_text("pub fn implicit_member() {}\n")
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["members/consumer"]\n'
                '[workspace.dependencies]\nimplicit-member={path="packages/implicit-member"}\n'
                'transitive-dependency={path="../rs-transitive-dependency"}\n')
            (execution / "Cargo.lock").write_text("")
            for root in (execution, dependency):
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
                subprocess.run(["git", "-C", str(root), "commit", "-qm", "cargo fixture"], check=True)

            source_manifest = consumer / "Cargo.toml"
            source_graph = json.loads(self._cargo_metadata_offline(source_manifest).stdout)
            self._cargo_metadata_offline(source_manifest, locked=True)
            packages = {package["name"]: package for package in source_graph["packages"]}
            self.assertIn(packages["implicit-member"]["id"], source_graph["workspace_members"])
            self.assertEqual(Path(packages["transitive-dependency"]["manifest_path"]),
                             dependency / "Cargo.toml")

            discovered = discover_local_repositories(source_manifest, parent, execution)
            self.assertEqual(set(discovered), {execution.resolve(), dependency.resolve()})

    def test_cargo_1_94_fixture_config_path_patch_is_snapshotted(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            dependency = self.make_input_repository(parent / "rs-config-dependency", "config-dependency")
            outer_dependency = self.make_input_repository(parent / "rs-outer-config-dependency",
                                                         "config-dependency")
            (dependency / "Cargo.toml").write_text(
                '[package]\nname="config-dependency"\nversion="0.1.0"\n'
                'edition="2021"\n')
            (dependency / "Cargo.lock").write_text("")
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            (fixture / "Cargo.toml").write_text(
                '[package]\nname="ioc-consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nconfig-dependency="0.1.0"\n')
            (fixture / "Cargo.lock").write_text("")
            config = fixture / ".cargo/config.toml"
            config.parent.mkdir()
            config.write_text('[patch.crates-io]\nconfig-dependency={path="../../../../rs-config-dependency"}\n')
            (execution / "Cargo.lock").write_text("")
            outer_config = execution / ".cargo/config.toml"
            outer_config.parent.mkdir()
            outer_config.write_text(
                '[patch.crates-io]\nconfig-dependency={path="../rs-outer-config-dependency"}\n')
            (fixture / "src").mkdir()
            (fixture / "src/lib.rs").write_text("pub fn fixture() {}\n")
            for root in (execution, dependency, outer_dependency):
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
                subprocess.run(["git", "-C", str(root), "commit", "-qm", "cargo fixture"], check=True)
            self._assert_cargo_snapshot_discovers_only_reachable_packages(
                parent, fixture / "Cargo.toml", "config-dependency")

    def test_ignored_cargo_config_does_not_mask_tracked_config_toml(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            dependency = self.make_input_repository(parent / "rs-tracked-dependency", "tracked-dependency")
            (dependency / "Cargo.toml").write_text(
                '[package]\nname="config-dependency"\nversion="0.1.0"\n'
                'edition="2021"\n')
            (dependency / "Cargo.lock").write_text("")
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            (fixture / "Cargo.toml").write_text(
                '[package]\nname="ioc-consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nconfig-dependency="0.1.0"\n')
            (fixture / "Cargo.lock").write_text("")
            cargo_dir = fixture / ".cargo"
            cargo_dir.mkdir()
            tracked_config = cargo_dir / "config.toml"
            tracked_config.write_text(
                '[patch.crates-io]\nconfig-dependency={path="../../../../rs-tracked-dependency"}\n')
            (fixture / "src").mkdir()
            (fixture / "src/lib.rs").write_text("pub fn fixture() {}\n")
            (execution / ".gitignore").write_text("**/.cargo/config\n")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            subprocess.run(["git", "-C", str(execution), "commit", "-qm", "tracked Cargo config"],
                           check=True)

            ignored_config = cargo_dir / "config"
            ignored_config.write_text(
                '[patch.crates-io]\nconfig-dependency={path="../../../../../outside-dependency"}\n')
            ignored = subprocess.run(["git", "-C", str(execution), "check-ignore", str(ignored_config)],
                                     capture_output=True, text=True, check=False)
            self.assertEqual(ignored.returncode, 0, ignored.stderr)

            snapshots_root = base / "snapshot/siblings"
            copy_tracked_source(execution, snapshots_root / execution.name)
            copy_tracked_source(dependency, snapshots_root / dependency.name)
            snapshot_config_dir = (snapshots_root / execution.name /
                                   "tests/fixtures/ioc_application_consumer/.cargo")
            self.assertTrue((snapshot_config_dir / "config.toml").is_file())
            self.assertFalse((snapshot_config_dir / "config").exists())

            discovered = discover_local_repositories(fixture / "Cargo.toml", parent, execution)
            self.assertEqual(set(discovered), {execution.resolve(), dependency.resolve()})
            (execution / ".gitignore").write_text("")
            no_longer_ignored = subprocess.run(
                ["git", "-C", str(execution), "check-ignore", str(ignored_config)],
                capture_output=True, text=True, check=False)
            self.assertEqual(no_longer_ignored.returncode, 1)
            discovered_unignored = discover_local_repositories(
                fixture / "Cargo.toml", parent, execution)
            self.assertEqual(set(discovered_unignored), {execution.resolve(), dependency.resolve()})
            snapshot_manifest = (snapshots_root / execution.name /
                                 "tests/fixtures/ioc_application_consumer/Cargo.toml")
            self._cargo_metadata_offline(snapshot_manifest)
            graph = json.loads(self._cargo_metadata_offline(snapshot_manifest, locked=True).stdout)
            package = next(item for item in graph["packages"]
                           if item["name"] == "config-dependency")
            self.assertEqual(Path(package["manifest_path"]),
                             snapshots_root / dependency.name / "Cargo.toml")

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

            config = fixture / ".cargo/config.toml"
            config.parent.mkdir()
            config.write_text('[patch.crates-io]\nmissing={path="../../../../config-missing"}\n')
            subprocess.run(["git", "-C", str(fixture.parents[2]), "init", "-q"], check=True)
            subprocess.run(["git", "-C", str(fixture.parents[2]), "add", "-A"], check=True)
            with self.assertRaisesRegex(ValueError, r"config.toml.*config-missing"):
                discover_local_repositories(manifest, parent)
            config.write_text('[patch.crates-io]\noutside={path="../../../../../outside"}\n')
            with self.assertRaisesRegex(ValueError, r"config.toml.*outside"):
                discover_local_repositories(manifest, parent)

    def test_execution_services_rejects_absolute_paths_inside_and_outside_sibling_root(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            fixture = parent / "rs-execution-services/tests/fixtures/consumer"
            fixture.mkdir(parents=True)
            dependency = parent / "rs-local"
            dependency.mkdir()
            (dependency / "Cargo.toml").write_text("[package]\nname='local'\n")
            manifest = fixture / "Cargo.toml"
            for absolute in (dependency, Path(directory) / "outside"):
                manifest.write_text(f'[dependencies]\nlocal = {{ path = "{absolute}" }}\n')
                with self.subTest(path=absolute):
                    with self.assertRaisesRegex(ValueError, r"absolute local path.*Cargo.toml.*relative"):
                        discover_local_repositories(manifest, parent)

    def test_main_rejects_absolute_ioc_path_before_any_snapshot_or_cargo_call(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            core = self.make_input_repository(base / "core", "core")
            redis = self.make_input_repository(base / "rs-event-bus-redis", "redis")
            task = self.make_input_repository(base / "rs-task", "task")
            execution = self.make_input_repository(base / "rs-execution-services", "execution")
            local = self.make_input_repository(base / "local", "local")
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            manifest.write_text(f'[dependencies]\nlocal = {{ path = "{local}" }}\n')
            argv = ["check_downstream_compatibility.py", "--core-root", str(core),
                    "--redis-root", str(redis), "--task-root", str(task),
                    "--execution-services-root", str(execution)]
            with patch("sys.argv", argv), \
                    patch("scripts.check_downstream_compatibility.copy_tracked_source") as copy_source, \
                    patch("scripts.check_downstream_compatibility.metadata") as metadata_call, \
                    patch("scripts.check_downstream_compatibility.run") as cargo_run:
                with self.assertRaisesRegex(ValueError, "absolute local path") as error:
                    main()
            self.assertIn(str(manifest.resolve()), str(error.exception))
            copy_source.assert_not_called()
            metadata_call.assert_not_called()
            cargo_run.assert_not_called()

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
        validate_graph_for_downstream(
            graph, expected_core=Path("/core"), expected_redis=Path("/redis"),
            allowed_local_roots=(Path("/redis"),))

        graph["packages"][0]["manifest_path"] = "/other-core/Cargo.toml"
        with self.assertRaisesRegex(ValueError, "expected local core"):
            validate_graph_for_downstream(graph, expected_core=Path("/core"),
                                          expected_redis=Path("/redis"),
                                          allowed_local_roots=(Path("/redis"),))
        graph["packages"][0]["manifest_path"] = "/core/Cargo.toml"
        graph["packages"][0]["source"] = "registry+https://github.com/rust-lang/crates.io-index"
        with self.assertRaisesRegex(ValueError, "expected local core"):
            validate_graph_for_downstream(graph, expected_core=Path("/core"),
                                          expected_redis=Path("/redis"),
                                          allowed_local_roots=(Path("/redis"),))
        graph["packages"][0]["source"] = None

        graph["packages"][1]["manifest_path"] = "/other-redis/Cargo.toml"
        with self.assertRaisesRegex(ValueError, "expected local Redis provider"):
            validate_graph_for_downstream(
                graph, expected_core=Path("/core"), expected_redis=Path("/redis"),
                allowed_local_roots=(Path("/redis"),))
        graph["packages"][1]["manifest_path"] = "/redis/Cargo.toml"
        graph["packages"][1]["source"] = "registry+https://github.com/rust-lang/crates.io-index"
        with self.assertRaisesRegex(ValueError, "expected local Redis provider"):
            validate_graph_for_downstream(
                graph, expected_core=Path("/core"), expected_redis=Path("/redis"),
                allowed_local_roots=(Path("/redis"),))

    def test_graph_rejects_reachable_local_package_outside_snapshot_roots(self):
        graph = {
            "workspace_members": ["consumer"],
            "resolve": {"nodes": [
                {"id": "consumer", "dependencies": ["core", "local"]},
                {"id": "core", "dependencies": []},
                {"id": "local", "dependencies": []},
            ]},
            "packages": [
                {"id": "consumer", "name": "consumer", "source": None,
                 "manifest_path": "/snapshot/consumer/Cargo.toml"},
                {"id": "core", "name": "qubit-event-bus", "version": "0.21.0",
                 "source": None, "manifest_path": "/external-core/Cargo.toml"},
                {"id": "local", "name": "local", "source": None,
                 "manifest_path": "/snapshot-copy-neighbor/local/Cargo.toml"},
            ],
        }
        with self.assertRaisesRegex(ValueError, "local at /snapshot-copy-neighbor/local/Cargo.toml"):
            validate_graph_for_downstream(
                graph, expected_core=Path("/external-core"),
                allowed_local_roots=(Path("/snapshot"),))
        graph["packages"][2]["manifest_path"] = "/snapshot/local/Cargo.toml"
        validate_graph_for_downstream(
            graph, expected_core=Path("/external-core"),
            allowed_local_roots=(Path("/snapshot"),))

        graph["packages"][2]["manifest_path"] = "/external-core/nested/local/Cargo.toml"
        with self.assertRaisesRegex(ValueError, "nested/local/Cargo.toml"):
            validate_graph_for_downstream(graph, expected_core=Path("/external-core"),
                                          allowed_local_roots=(Path("/snapshot"),))

    def test_ioc_suite_allows_only_sibling_snapshot_for_other_local_packages(self):
        with tempfile.TemporaryDirectory() as directory:
            sources = Path(directory) / "sources"
            roots = {
                "redis": sources / "redis",
                "rs-task": sources / "task",
                "execution-services": sources / "siblings/rs-execution-services",
            }
            for root in roots.values():
                root.mkdir(parents=True)
            metadata_calls = []

            def capture_metadata(root, *_args, **kwargs):
                metadata_calls.append((root, kwargs))

            with patch("scripts.check_downstream_compatibility.metadata",
                       side_effect=capture_metadata), \
                    patch("scripts.check_downstream_compatibility.verify_test"):
                run_downstream_checks(roots, Path("/external/core"), Path(directory) / "target",
                                      include_execution_services=True)

            execution_root, kwargs = next(
                (root, kwargs) for root, kwargs in metadata_calls
                if root == roots["execution-services"])
            self.assertEqual(kwargs["allowed_local_roots"],
                             (roots["execution-services"].parent,))

            def local_package_graph(local_manifest):
                return {
                    "workspace_members": ["consumer"],
                    "resolve": {"nodes": [
                        {"id": "consumer", "dependencies": ["core", "local"]},
                        {"id": "core", "dependencies": []},
                        {"id": "local", "dependencies": []},
                    ]},
                    "packages": [
                        {"id": "consumer", "name": "consumer", "source": None,
                         "manifest_path": str(execution_root / "fixture/Cargo.toml")},
                        {"id": "core", "name": "qubit-event-bus", "version": "0.21.0",
                         "source": None, "manifest_path": "/external/core/Cargo.toml"},
                        {"id": "local", "name": "local", "source": None,
                         "manifest_path": str(local_manifest)},
                    ],
                }

            for suite_snapshot in (roots["redis"], roots["rs-task"]):
                with self.subTest(rejected=suite_snapshot):
                    with self.assertRaisesRegex(ValueError, "outside allowed snapshot roots"):
                        validate_graph_for_downstream(
                            local_package_graph(suite_snapshot / "local/Cargo.toml"),
                            expected_core=Path("/external/core"),
                            allowed_local_roots=kwargs["allowed_local_roots"])

            validate_graph_for_downstream(
                local_package_graph((roots["execution-services"].parent /
                                     "rs-local/Cargo.toml")),
                expected_core=Path("/external/core"),
                allowed_local_roots=kwargs["allowed_local_roots"])

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

    def test_nested_manifest_uses_its_directory_for_all_cargo_stages(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "nested/fixture/Cargo.toml"
            manifest.parent.mkdir(parents=True)
            manifest.write_text("[package]\nname='fixture'\n")
            env = {"CARGO_TARGET_DIR": "/target"}
            commands = []

            def fake_run(command, **kwargs):
                commands.append((command, kwargs))
                if "metadata" in command:
                    return "{}"
                if "--list" in command:
                    return "target: test\n"
                return ("test target ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; "
                        "0 measured; 0 filtered out\n")

            with patch("scripts.check_downstream_compatibility.run", side_effect=fake_run), \
                    patch("scripts.check_downstream_compatibility.validate_graph_for_downstream"):
                metadata(Path(directory), "sqlite", Path("/core"), None, Path("/target"),
                         manifest=manifest)
                verify_test(Path(directory), "target",
                            ["--features", "sqlite", "--test", "smoke", "target"],
                            Path("/core"), None, env, manifest=manifest)

            self.assertEqual(len(commands), 4)
            self.assertTrue(all(call[1]["cwd"] == manifest.parent for call in commands))
            self.assertTrue(all(call[1]["env"]["CARGO_TARGET_DIR"] == "/target"
                                for call in commands))
            self.assertTrue(all(command[command.index("--features") + 1] == "sqlite"
                                for command, _kwargs in commands))
            cargo_configs = []
            for command, _kwargs in commands:
                cargo_configs.append(tuple(
                    command[index + 1] for index, value in enumerate(command[:-1])
                    if value == "--config"))
            self.assertEqual(len(set(cargo_configs)), 1)

    def test_cargo_194_offline_fixture_honors_fixture_local_config_for_all_stages(self):
        cargo_version = subprocess.run(["cargo", "--version"], check=True, text=True,
                                       capture_output=True).stdout.strip()
        self.assertIn("cargo 1.94.", cargo_version)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "fixture"
            (root / ".cargo").mkdir(parents=True)
            (root / "tests").mkdir()
            patch_root = root / "local-patch"
            patch_root.mkdir()
            core_root = root / "core"
            core_root.mkdir()
            (root / "Cargo.toml").write_text(
                "[package]\nname='offline-consumer'\nversion='0.1.0'\nedition='2021'\n\n"
                "[dependencies]\nlocal-patch='1'\nqubit-event-bus='0.21'\n")
            (root / ".cargo/config.toml").write_text(
                '[patch.crates-io]\nlocal-patch = { path = "local-patch" }\n')
            (root / "tests/smoke.rs").write_text("#[test] fn offline_fixture_runs() {}\n")
            (patch_root / "Cargo.toml").write_text(
                "[package]\nname='local-patch'\nversion='1.0.0'\nedition='2021'\n")
            (patch_root / "src").mkdir()
            (patch_root / "src/lib.rs").write_text("")
            (core_root / "Cargo.toml").write_text(
                "[package]\nname='qubit-event-bus'\nversion='0.21.0'\nedition='2021'\n")
            (core_root / "src").mkdir()
            (core_root / "src/lib.rs").write_text("")
            env = os.environ.copy()
            env["CARGO_NET_OFFLINE"] = "true"
            env["CARGO_TARGET_DIR"] = str(Path(directory) / "target")
            # The fixture's own patch is resolved by metadata and reused by the
            # locked metadata, exact test listing, and exact test execution.
            with patch.dict(os.environ, {"CARGO_NET_OFFLINE": "true"}):
                metadata(root, None, core_root, None, Path(env["CARGO_TARGET_DIR"]),
                         manifest=root / "Cargo.toml", allowed_local_roots=(root,))
                verify_test(root, "offline_fixture_runs", ["--test", "smoke", "offline_fixture_runs"],
                            core_root, None, env, manifest=root / "Cargo.toml")

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
                              Path("/target"), expected_redis=Path("/redis"),
                              allowed_local_roots=(Path("/redis"),))
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
            nested = self.make_input_repository(base / "rs-nested-dependency", "nested")
            (sibling / "Cargo.toml").write_text(
                '[package]\nname="local"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nnested={path="../rs-nested-dependency"}\n')
            subprocess.run(["git", "-C", str(sibling), "add", "Cargo.toml"], check=True)
            subprocess.run(["git", "-C", str(sibling), "commit", "-qm", "nested dependency"], check=True)
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            manifest.write_text('[dependencies]\nlocal = { path = "../../../../rs-local-dependency" }\n')
            subprocess.run(["git", "-C", str(execution), "add", str(manifest.relative_to(execution))],
                           check=True)
            subprocess.run(["git", "-C", str(execution), "commit", "-qm", "fixture"], check=True)
            input_roots = (redis, task, execution, sibling, nested)
            input_before = {
                path: {file.relative_to(path): (file.read_bytes(), file.stat().st_mtime_ns)
                       for file in path.rglob("*") if file.is_file() and ".git" not in file.parts}
                for path in input_roots
            }
            argv = ["check_downstream_compatibility.py", "--redis-root", str(redis),
                    "--task-root", str(task), "--execution-services-root", str(execution)]
            seen = []
            copies = []
            metadata_manifests = []
            verify_manifests = []

            def copy_source(source, destination):
                copies.append((Path(source).resolve(), Path(destination).resolve()))
                return copy_tracked_source(source, destination)

            def metadata_snapshot(root, _features, _core, _redis, _target, **kwargs):
                if root.name == "rs-execution-services":
                    dependency = root.parent / "rs-local-dependency"
                    nested_dependency = root.parent / "rs-nested-dependency"
                    self.assertTrue((dependency / "Cargo.toml").is_file())
                    self.assertTrue((nested_dependency / "Cargo.toml").is_file())
                    self.assertEqual((dependency / "src/lib.rs").read_text(),
                                     "pub fn locally_edited() {}\n")
                    self.assertEqual((dependency / "Cargo.lock").read_bytes(),
                                     input_before[sibling][Path("Cargo.lock")][0])
                    self.assertTrue((root / "tests/fixtures/ioc_application_consumer/Cargo.toml").is_file())
                    seen.append(root)
                    metadata_manifests.append(Path(kwargs["manifest"]).resolve())
                return {}

            def verify_snapshot(root, _name, _arguments, _core, _redis, _env, **kwargs):
                if root.name == "rs-execution-services":
                    verify_manifests.append(Path(kwargs["manifest"]).resolve())

            output = io.StringIO()
            with patch("sys.argv", argv), \
                    patch("scripts.check_downstream_compatibility.copy_tracked_source",
                          side_effect=copy_source), \
                    patch("scripts.check_downstream_compatibility.metadata",
                          side_effect=metadata_snapshot), \
                    patch("scripts.check_downstream_compatibility.verify_test",
                          side_effect=verify_snapshot) as verify, \
                    contextlib.redirect_stdout(output):
                main()
            self.assertEqual(len(seen), 1)
            self.assertEqual(verify.call_count, 7)
            execution_destinations = [destination for source, destination in copies
                                      if source == execution.resolve()]
            self.assertEqual(len(execution_destinations), 1)
            execution_copy = execution_destinations[0]
            actual_manifest = execution_copy / "tests/fixtures/ioc_application_consumer/Cargo.toml"
            self.assertEqual(metadata_manifests, [actual_manifest.resolve()])
            self.assertEqual(verify_manifests, [actual_manifest.resolve()] * 3)
            sibling_copies = {source: destination for source, destination in copies
                              if source in {execution.resolve(), sibling.resolve(), nested.resolve()}}
            self.assertEqual(sibling_copies[execution.resolve()], execution_copy)
            self.assertEqual(sibling_copies[sibling.resolve()], execution_copy.parent /
                             "rs-local-dependency")
            self.assertEqual(sibling_copies[nested.resolve()], execution_copy.parent /
                             "rs-nested-dependency")
            records = output.getvalue().splitlines()
            for source, destination in sibling_copies.items():
                matching_records = [line for line in records if f"root={source}" in line]
                self.assertEqual(len(matching_records), 1)
                record = matching_records[0]
                self.assertIn(f"snapshot={destination}", record)
                if source == execution.resolve():
                    self.assertIn("additional_snapshots=[]", record)
                    self.assertEqual(actual_manifest.parent.parent.parent.parent, destination)
            self.assertEqual(input_before, {
                path: {file.relative_to(path): (file.read_bytes(), file.stat().st_mtime_ns)
                       for file in path.rglob("*") if file.is_file() and ".git" not in file.parts}
                for path in input_roots
            })

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
                                 *, expected_redis=None, manifest=None, all_features=False,
                                 allowed_local_roots=None):
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
