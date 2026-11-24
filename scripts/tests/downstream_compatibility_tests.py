import builtins
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from scripts.check_downstream_compatibility import _cargo_config_patch_declarations
from scripts.check_downstream_compatibility import _config_patch_declarations
from scripts.check_downstream_compatibility import _manifest_patch_declarations
from scripts.check_downstream_compatibility import _workspace_member_manifests
from scripts.check_downstream_compatibility import cargo_config
from scripts.downstream_source_snapshot import copy_tracked_source
from scripts.downstream_source_snapshot import tracked_source_files
from scripts.check_downstream_compatibility import TESTS
from scripts.check_downstream_compatibility import _glob_matches
from scripts.check_downstream_compatibility import discover_local_repositories
from scripts.check_downstream_compatibility import main
from scripts.check_downstream_compatibility import metadata
from scripts.check_downstream_compatibility import run
from scripts.check_downstream_compatibility import run_downstream_checks
from scripts.check_downstream_compatibility import validate_graph_for_downstream
from scripts.check_downstream_compatibility import verify_test
from scripts.tests.process_test_support import wait_for_process_exit


class DownstreamCompatibilityTests(unittest.TestCase):
    def assert_manifest_link_rejected_before_target_read(
            self, manifest, parent, source, declaration, target):
        original_read_text = Path.read_text

        def reject_target_read(path, *args, **kwargs):
            if Path(path).resolve() == target.resolve():
                raise AssertionError(f"discovery opened out-of-repository target: {target}")
            return original_read_text(path, *args, **kwargs)

        with patch.object(Path, "read_text", reject_target_read):
            with self.assertRaisesRegex(
                    ValueError,
                    rf"{declaration}.*{target}.*repository boundary: {source}"):
                discover_local_repositories(manifest, parent, source)

    def make_cargo_manifest_chain(self, root, name, middle_kind):
        repository = self.make_input_repository(root, name)
        (repository / "Cargo.lock").unlink()
        self._cargo_metadata_offline(repository / "Cargo.toml")
        (repository / "Cargo.toml").rename(repository / "real.toml")
        if middle_kind == "absolute":
            (repository / "middle.toml").symlink_to((repository / "real.toml").resolve())
        else:
            (repository / "middle.toml").symlink_to("real.toml")
        (repository / "Cargo.toml").symlink_to("middle.toml")
        git_paths = ["Cargo.toml", "real.toml", "Cargo.lock"]
        if middle_kind != "untracked":
            git_paths.append("middle.toml")
        subprocess.run(["git", "-C", str(repository), "add", *git_paths], check=True)
        subprocess.run(["git", "-C", str(repository), "commit", "-qm", "manifest chain fixture"], check=True)
        return repository

    def old_direct_link_check_accepts(self, repository):
        declaration = repository / "Cargo.toml"
        tracked = set(tracked_source_files(repository))
        target = declaration.resolve()
        return (declaration.relative_to(repository) in tracked and
                target.relative_to(repository) in tracked and
                not Path(os.readlink(declaration)).is_absolute())

    def add_untracked_middle_member(self, repository, directory):
        package = repository / directory
        package.mkdir(parents=True)
        (package / "real.toml").write_text(
            f'[package]\nname="{directory.replace("/", "-")}"\nversion="0.1.0"\n'
            'edition="2021"\n')
        (package / "src").mkdir()
        (package / "src/lib.rs").write_text("pub fn fixture() {}\n")
        (package / "middle.toml").symlink_to("real.toml")
        (package / "Cargo.toml").symlink_to("middle.toml")
        subprocess.run(["git", "-C", str(repository), "add",
                        str(Path(directory) / "Cargo.toml"),
                        str(Path(directory) / "real.toml"),
                        str(Path(directory) / "src/lib.rs")], check=True)
        return package / "Cargo.toml"

    def test_workspace_glob_matching_matches_cargo_path_segments(self):
        self.assertTrue(_glob_matches("members/no-local", "members/[nk]o-local"))
        self.assertTrue(_glob_matches("members/no-local", "./members/no-local"))
        self.assertTrue(_glob_matches("members/no-local", "members/**/no-local"))
        self.assertTrue(_glob_matches("members/deep/no-local", "members/**/no-local"))
        self.assertFalse(_glob_matches("members/a/b/no-local", "members/*/no-local"))

    def test_absolute_package_workspace_is_rejected_for_initial_and_external_packages(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            external = self.make_input_repository(parent / "rs-external", "external")
            fixture = execution / "tests/fixtures/consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="consumer"\nversion="0.1.0"\nedition="2021"\n'
                f'workspace="{execution}"\n')
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            with patch("scripts.check_downstream_compatibility._workspace_root",
                       side_effect=lambda path, data, _boundary=None:
                       (Path(path).parent / data.get("package", {}).get("workspace")).resolve()
                       if isinstance(data.get("package", {}).get("workspace"), str) else None):
                self.assertEqual(set(discover_local_repositories(manifest, parent, execution)),
                                 {execution.resolve()})
            with self.assertRaisesRegex(ValueError,
                                        rf"package.workspace.*{manifest}.*{execution}"):
                discover_local_repositories(manifest, parent, execution)

            external_manifest = external / "Cargo.toml"
            external_manifest.write_text(
                '[package]\nname="external"\nversion="0.1.0"\nedition="2021"\n'
                f'workspace="{external}"\n')
            subprocess.run(["git", "-C", str(external), "add", "-A"], check=True)
            fixture.joinpath("Cargo.toml").write_text(
                '[package]\nname="consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nexternal={path="../../../../rs-external"}\n')
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            with self.assertRaisesRegex(ValueError,
                                        rf"package.workspace.*{external_manifest}.*{external}"):
                discover_local_repositories(fixture / "Cargo.toml", parent, execution)

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

    def test_tracked_manifest_cache_refreshes_for_each_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            first = self.make_input_repository(parent / "rs-first", "first")
            second = self.make_input_repository(parent / "rs-second", "second")
            manifest = execution / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nfirst={path="../rs-first"}\n')
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml"], check=True)
            self.assertEqual(set(discover_local_repositories(manifest, parent, execution)),
                             {execution.resolve(), first.resolve()})

            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nsecond={path="../rs-second"}\n')
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml"], check=True)
            self.assertEqual(set(discover_local_repositories(manifest, parent, execution)),
                             {execution.resolve(), second.resolve()})

    def test_cargo_1_94_path_key_versions_and_legacy_dependency_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            names = ("path", "direct", "target", "dev", "build", "target-build",
                     "inherited-dev")
            repos = {name: self.make_input_repository(parent / f"rs-{name}", name)
                     for name in names}
            manifest = execution / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\n[workspace.dependencies]\n'
                'inherited-dev={path="../rs-inherited-dev"}\n'
                '[dependencies]\npath="0.1.0"\ndirect={path="../rs-direct"}\n'
                "[target.'cfg(unix)'.dependencies]\ntarget={path='../rs-target'}\n"
                "[target.'cfg(unix)'.build-dependencies]\ntarget-build={path='../rs-target-build'}\n"
                "[target.'cfg(unix)'.build_dependencies]\n"
                "target-build-legacy={path='../missing-target-build'}\n"
                '[dev-dependencies]\ndev={path="../rs-dev"}\n'
                'inherited-dev={workspace=true}\n'
                '[dev_dependencies]\ndev-legacy={path="../missing-dev-legacy"}\n'
                '[build-dependencies]\nbuild={path="../rs-build"}\n'
                '[build_dependencies]\nbuild-legacy={path="../missing-build-legacy"}\n'
                '[patch.crates-io]\npath={path="../rs-path"}\n')
            (execution / "Cargo.lock").write_text("")
            for root in (execution, *repos.values()):
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
            result = self._cargo_metadata_offline(manifest)
            graph = json.loads(result.stdout)
            self.assertEqual({package["name"] for package in graph["packages"]},
                             {"execution", *names})
            discovered = set(discover_local_repositories(manifest, parent, execution))
            self.assertEqual(discovered, {execution.resolve(), *(root.resolve()
                                                                  for root in repos.values())})

    def test_cargo_snapshot_expands_workspace_globs_from_tracked_manifests(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            manifest = execution / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["members/*"]\n')
            (execution / "Cargo.lock").write_text("")
            kept = execution / "members/kept"
            kept.mkdir(parents=True)
            (kept / "Cargo.toml").write_text(
                '[package]\nname="kept"\nversion="0.1.0"\nedition="2021"\n'
                '[dev-dependencies]\nlocal-target={path="../../target/kept"}\n')
            (kept / "src").mkdir()
            (kept / "src/lib.rs").write_text("")
            target = execution / "target/kept"
            target.mkdir(parents=True)
            (target / "Cargo.toml").write_text(
                '[package]\nname="local-target"\nversion="0.1.0"\nedition="2021"\n')
            (target / "src").mkdir()
            (target / "src/lib.rs").write_text("")
            untracked = execution / "members/untracked"
            untracked.mkdir()
            (untracked / "Cargo.toml").write_text(
                '[package]\nname="untracked"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nbad={path="../../../../missing"}\n')
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml",
                            "members/kept/Cargo.toml", "members/kept/src/lib.rs", "Cargo.lock"],
                           check=True)
            subprocess.run(["git", "-C", str(execution), "add", "-f",
                            "target/kept/Cargo.toml", "target/kept/src/lib.rs"], check=True)
            discovered = set(discover_local_repositories(manifest, parent, execution))
            self.assertEqual(discovered, {execution.resolve()})
            snapshot = base / "snapshot"
            copy_tracked_source(execution, snapshot / execution.name)
            snapshot_manifest = snapshot / execution.name / "Cargo.toml"
            self._cargo_metadata_offline(snapshot_manifest)
            self._cargo_metadata_offline(snapshot_manifest, locked=True)
            self.assertFalse((snapshot / execution.name / "members/untracked/Cargo.toml").exists())

    def test_cargo_1_94_workspace_member_globs_include_only_tracked_member_dev_inputs(self):
        cases = (("members/[nk]o-local", ("members/no-local",)),
                 ("./members/no-local", ("members/no-local",)),
                 ("members/**/no-local", ("members/no-local", "members/deep/no-local")))
        for pattern, member_paths in cases:
            with self.subTest(pattern=pattern), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                parent = base / "siblings"
                parent.mkdir()
                execution = self.make_input_repository(parent / "rs-execution", "execution")
                (execution / "Cargo.toml").write_text(
                    '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                    f'[workspace]\nmembers=["{pattern}"]\n')
                dependencies = {}
                for index, member_path in enumerate(member_paths):
                    member = execution / member_path
                    member.mkdir(parents=True)
                    dep_name = f"member-dev-{index}"
                    dependency = self.make_input_repository(parent / f"rs-{dep_name}", dep_name)
                    (dependency / "Cargo.toml").write_text(
                        f'[package]\nname="{dep_name}"\nversion="0.1.0"\n'
                        'edition="2021"\n')
                    (member / "Cargo.toml").write_text(
                        f'[package]\nname="member-{index}"\nversion="0.1.0"\n'
                        'edition="2021"\n'
                        f'[dev-dependencies]\n{dep_name}={{path="{os.path.relpath(dependency, member)}"}}\n')
                    (member / "src").mkdir()
                    (member / "src/lib.rs").write_text("")
                    dependencies[dep_name] = dependency
                (execution / "Cargo.lock").write_text("")
                for repository in (execution, *dependencies.values()):
                    subprocess.run(["git", "-C", str(repository), "add", "-A"], check=True)

                manifest = execution / "Cargo.toml"
                source = json.loads(self._cargo_metadata_offline(manifest).stdout)
                self._cargo_metadata_offline(manifest, locked=True)
                member_ids = set(source["workspace_members"])
                source_members = {item["name"] for item in source["packages"]
                                  if item["id"] in member_ids}
                self.assertEqual(source_members, {"execution", *(f"member-{i}"
                                                                  for i in range(len(member_paths)))})
                discovered = set(discover_local_repositories(manifest, parent, execution))
                self.assertEqual(discovered, {execution.resolve(), *(root.resolve()
                                                                     for root in dependencies.values())})
                snapshot_root = base / "snapshot/siblings"
                for repository in discovered:
                    copy_tracked_source(repository, snapshot_root / repository.relative_to(parent))
                snapshot_manifest = snapshot_root / execution.name / "Cargo.toml"
                snapshot = json.loads(self._cargo_metadata_offline(snapshot_manifest, locked=True).stdout)
                self.assertEqual({item["name"] for item in snapshot["packages"]},
                                 {"execution", *(f"member-{i}" for i in range(len(member_paths))),
                                  *dependencies.keys()})

    def test_absolute_workspace_members_are_rejected_before_snapshot_discovery(self):
        for member_pattern in ("ABSOLUTE_MEMBER", "ABSOLUTE_GLOB"):
            with self.subTest(member_pattern=member_pattern), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                parent = base / "siblings"
                parent.mkdir()
                execution = self.make_input_repository(parent / "rs-execution", "execution")
                member = execution / "members" / "kept"
                member.mkdir(parents=True)
                (member / "Cargo.toml").write_text(
                    '[package]\nname="kept"\nversion="0.1.0"\nedition="2021"\n')
                (member / "src").mkdir()
                (member / "src/lib.rs").write_text("")
                other_member = execution / "members" / "other"
                other_member.mkdir()
                (other_member / "Cargo.toml").write_text(
                    '[package]\nname="other"\nversion="0.1.0"\nedition="2021"\n')
                (other_member / "src").mkdir()
                (other_member / "src/lib.rs").write_text("")
                (execution / "Cargo.lock").write_text("")
                declaration = str(member if member_pattern == "ABSOLUTE_MEMBER"
                                  else member.parent / "*")
                (execution / "Cargo.toml").write_text(
                    '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                    f'[workspace]\nmembers=["members/other", "{declaration}"]\n')
                subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
                source = json.loads(self._cargo_metadata_offline(execution / "Cargo.toml").stdout)
                self.assertIn("kept", {package["name"] for package in source["packages"]})
                with patch("scripts.check_downstream_compatibility._workspace_glob_manifests") as scan:
                    with self.assertRaisesRegex(
                            ValueError,
                            rf"absolute workspace member.*{execution / 'Cargo.toml'}.*{declaration}.*relative"):
                        discover_local_repositories(execution / "Cargo.toml", parent, execution)
                    scan.assert_not_called()

    def test_cargo_1_94_workspace_glob_collects_tracked_sibling_members_and_dev_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            external = self.make_input_repository(parent / "rs-external", "external")
            member_dev = self.make_input_repository(parent / "rs-member-dev", "member-dev")
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["fixture", "../rs-external/leaf-*"]\n')
            fixture = execution / "fixture"
            fixture.mkdir()
            (fixture / "Cargo.toml").write_text(
                '[package]\nname="fixture"\nversion="0.1.0"\nedition="2021"\n')
            (fixture / "src").mkdir()
            (fixture / "src/lib.rs").write_text("")
            leaf = external / "leaf-one"
            leaf.mkdir()
            (leaf / "Cargo.toml").write_text(
                '[package]\nname="leaf-one"\nversion="0.1.0"\nedition="2021"\n'
                'workspace="../../rs-execution"\n'
                f'[dev-dependencies]\nmember-dev={{path="{os.path.relpath(member_dev, leaf)}"}}\n')
            (leaf / "src").mkdir()
            (leaf / "src/lib.rs").write_text("")
            unrelated = external / "other"
            unrelated.mkdir()
            (unrelated / "Cargo.toml").write_text(
                '[package]\nname="other"\nversion="0.1.0"\nedition="2021"\n')
            untracked = external / "untracked" / "leaf"
            untracked.mkdir(parents=True)
            (untracked / "Cargo.toml").write_text(
                '[package]\nname="leaf-untracked"\nversion="0.1.0"\nedition="2021"\n')
            (execution / "Cargo.lock").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            subprocess.run(["git", "-C", str(external), "add", "leaf-one/Cargo.toml",
                            "leaf-one/src/lib.rs", "other/Cargo.toml"], check=True)

            source = json.loads(self._cargo_metadata_offline(execution / "Cargo.toml").stdout)
            self._cargo_metadata_offline(execution / "Cargo.toml", locked=True)
            source_workspace_members = {package["name"] for package in source["packages"]
                                        if package["id"] in source["workspace_members"]}
            self.assertEqual(source_workspace_members, {"execution", "fixture", "leaf-one"})
            discovered = set(discover_local_repositories(execution / "Cargo.toml", parent, execution))
            self.assertEqual(discovered, {execution.resolve(), external.resolve(), member_dev.resolve()})
            snapshot_root = base / "snapshot/siblings"
            for repository in discovered:
                copy_tracked_source(repository, snapshot_root / repository.relative_to(parent))
            snapshot_manifest = snapshot_root / execution.name / "Cargo.toml"
            snapshot = json.loads(self._cargo_metadata_offline(snapshot_manifest, locked=True).stdout)
            snapshot_members = {package["name"] for package in snapshot["packages"]
                                if package["id"] in snapshot["workspace_members"]}
            self.assertEqual(snapshot_members, source_workspace_members)
            self.assertEqual({package["name"] for package in snapshot["packages"]},
                             {"execution", "fixture", "leaf-one", "member-dev"})
            self.assertFalse((snapshot_root / "rs-external/untracked/leaf/Cargo.toml").exists())

    def test_cargo_1_94_workspace_glob_matches_sibling_repository_names(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            vendors = []
            for vendor_name, leaf_name in (("vendor-one", "leaf-one"),
                                           ("vendor-two", "leaf-two")):
                vendor = self.make_input_repository(parent / vendor_name, vendor_name)
                leaf = vendor / leaf_name
                leaf.mkdir()
                (leaf / "Cargo.toml").write_text(
                    f'[package]\nname="{leaf_name}"\nversion="0.1.0"\nedition="2021"\n'
                    'workspace="../../rs-execution/workspace"\n')
                (leaf / "src").mkdir()
                (leaf / "src/lib.rs").write_text("")
                subprocess.run(["git", "-C", str(vendor), "add", "-A"], check=True)
                vendors.append(vendor)
            unrelated = self.make_input_repository(parent / "other-vendor", "other-vendor")
            ignored = unrelated / "leaf-ignored"
            ignored.mkdir()
            (ignored / "Cargo.toml").write_text(
                '[package]\nname="leaf-ignored"\nversion="0.1.0"\nedition="2021"\n')
            subprocess.run(["git", "-C", str(unrelated), "add", "-A"], check=True)
            workspace = execution / "workspace"
            workspace.mkdir()
            execution_manifest = workspace / "Cargo.toml"
            (execution_manifest).write_text(
                '[workspace]\nmembers=["../../vendor-*/leaf-*"]\n')
            (workspace / "Cargo.lock").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)

            source = json.loads(self._cargo_metadata_offline(execution_manifest).stdout)
            self._cargo_metadata_offline(execution_manifest, locked=True)
            source_members = {package["name"] for package in source["packages"]
                              if package["id"] in source["workspace_members"]}
            self.assertEqual(source_members, {"leaf-one", "leaf-two"})
            discovered = set(discover_local_repositories(execution_manifest, parent, execution))
            self.assertEqual(discovered, {execution.resolve(), *(vendor.resolve() for vendor in vendors)})
            snapshot_root = base / "snapshot/siblings"
            for repository in discovered:
                copy_tracked_source(repository, snapshot_root / repository.relative_to(parent))
            snapshot_manifest = snapshot_root / execution.name / "workspace/Cargo.toml"
            snapshot = json.loads(self._cargo_metadata_offline(snapshot_manifest, locked=True).stdout)
            snapshot_members = {package["name"] for package in snapshot["packages"]
                                if package["id"] in snapshot["workspace_members"]}
            self.assertEqual(snapshot_members, source_members)

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_workspace_globs_reject_sibling_checkout_aliases_before_enumeration(self):
        for pattern in ("../alias-one/lea*", "../alias-*/leaf-*"):
            with self.subTest(pattern=pattern), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                parent = base / "siblings"
                parent.mkdir()
                execution = self.make_input_repository(parent / "rs-execution", "execution")
                actual = self.make_input_repository(parent / "rs-real", "vendor")
                leaf = actual / "leaf-one"
                leaf.mkdir()
                (leaf / "Cargo.toml").write_text(
                    '[package]\nname="leaf-one"\nversion="0.1.0"\nedition="2021"\n'
                    'workspace="../../rs-execution"\n')
                (leaf / "src").mkdir()
                (leaf / "src/lib.rs").write_text("pub fn leaf() {}\n")
                subprocess.run(["git", "-C", str(actual), "add", "leaf-one/Cargo.toml",
                                "leaf-one/src/lib.rs"], check=True)
                subprocess.run(["git", "-C", str(actual), "commit", "-qm", "leaf member"], check=True)
                (parent / "alias-one").symlink_to(actual, target_is_directory=True)

                manifest = execution / "Cargo.toml"
                manifest.write_text(f'[workspace]\nmembers=["{pattern}"]\n')
                (execution / "Cargo.lock").unlink()
                source = json.loads(self._cargo_metadata_offline(manifest).stdout)
                self._cargo_metadata_offline(manifest, locked=True)
                self.assertIn("leaf-one", {package["name"] for package in source["packages"]})
                resolved_member = os.path.relpath(leaf, execution)
                self.assertFalse(_glob_matches(resolved_member, pattern))
                source_lock = (execution / "Cargo.lock").read_bytes()
                subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml", "Cargo.lock"],
                               check=True)
                subprocess.run(["git", "-C", str(execution), "commit", "-qm", "workspace alias fixture"],
                               check=True)

                with self.assertRaises(ValueError) as raised:
                    discover_local_repositories(manifest, parent, execution)
                for expected in ("Cargo.toml", pattern, "alias-one", str(actual), "real checkout"):
                    self.assertIn(expected, str(raised.exception))

                manifest.write_text('[workspace]\nmembers=["../rs-real/leaf-*"]\n')
                self._cargo_metadata_offline(manifest, locked=True)
                subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml"], check=True)
                discovered = set(discover_local_repositories(manifest, parent, execution))
                self.assertEqual(discovered, {execution.resolve(), actual.resolve()})
                snapshot_root = base / "snapshot/siblings"
                for repository in discovered:
                    copy_tracked_source(repository, snapshot_root / repository.relative_to(parent))
                snapshot_manifest = snapshot_root / execution.name / "Cargo.toml"
                snapshot = json.loads(self._cargo_metadata_offline(
                    snapshot_manifest, locked=True).stdout)
                self.assertEqual({package["name"] for package in snapshot["packages"]},
                                 {package["name"] for package in source["packages"]})
                source_package = next(package for package in source["packages"]
                                      if package["name"] == "leaf-one")
                snapshot_package = next(package for package in snapshot["packages"]
                                        if package["name"] == "leaf-one")
                self.assertEqual(Path(source_package["manifest_path"]).resolve(), leaf / "Cargo.toml")
                self.assertEqual(Path(snapshot_package["manifest_path"]).resolve(),
                                 snapshot_root / actual.name / "leaf-one/Cargo.toml")
                self.assertEqual((execution / "Cargo.lock").read_bytes(), source_lock)

    def test_absolute_workspace_exclude_is_rejected_and_relative_exclude_snapshots(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            excluded = execution / "members/excluded"
            excluded.mkdir(parents=True)
            (excluded / "Cargo.toml").write_text(
                '[package]\nname="excluded"\nversion="0.1.0"\nedition="2021"\n'
                '[dev-dependencies]\nmissing={path="../../../../not-present"}\n')
            (execution / "Cargo.lock").write_text("")
            manifest = execution / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nexclude=["' + str(excluded) + '"]\n')
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            self._cargo_metadata_offline(manifest)
            self._cargo_metadata_offline(manifest, locked=True)
            with self.assertRaisesRegex(ValueError,
                                        rf"absolute workspace exclude.*{manifest}.*{excluded}.*relative"):
                discover_local_repositories(manifest, parent, execution)

            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nexclude=["members/excluded"]\n')
            discovered = set(discover_local_repositories(manifest, parent, execution))
            self.assertEqual(discovered, {execution.resolve()})
            snapshot = base / "snapshot/rs-execution"
            copy_tracked_source(execution, snapshot)
            snapshot_result = self._cargo_metadata_offline(snapshot / "Cargo.toml", locked=True)
            graph = json.loads(snapshot_result.stdout)
            self.assertEqual({package["name"] for package in graph["packages"]}, {"execution"})

    def test_cargo_linked_manifest_paths_resolve_from_the_declaring_package(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            package = execution / "packages/consumer"
            package.mkdir(parents=True)
            target = execution / "linked/consumer/Cargo.toml"
            target.parent.mkdir(parents=True)
            target.write_text(
                '[package]\nname="consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nlocal={path="../../dependencies/local"}\n')
            (package / "Cargo.toml").symlink_to("../../linked/consumer/Cargo.toml")
            (package / "src").mkdir()
            (package / "src/lib.rs").write_text("")
            dependency = execution / "dependencies/local"
            dependency.mkdir(parents=True)
            dependency_target = execution / "linked/local/Cargo.toml"
            dependency_target.parent.mkdir(parents=True)
            dependency_target.write_text(
                '[package]\nname="local"\nversion="0.1.0"\nedition="2021"\n')
            (dependency / "Cargo.toml").symlink_to("../../linked/local/Cargo.toml")
            (dependency / "src").mkdir()
            (dependency / "src/lib.rs").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            manifest = package / "Cargo.toml"
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["packages/consumer", "packages/ordinary"]\n')
            ordinary = execution / "packages/ordinary"
            ordinary.mkdir()
            (ordinary / "Cargo.toml").write_text(
                '[package]\nname="ordinary"\nversion="0.1.0"\nedition="2021"\n')
            (ordinary / "src").mkdir()
            (ordinary / "src/lib.rs").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            (execution / "Cargo.lock").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml"], check=True)
            # The target-relative declaration is deliberately invalid. It is
            # valid only when Cargo uses the package's declared manifest path.
            self._cargo_metadata_offline(manifest)
            self._cargo_metadata_offline(manifest, locked=True)
            discovered = set(discover_local_repositories(manifest, parent, execution))
            self.assertEqual(discovered, {execution.resolve()})
            snapshot = base / "snapshot/rs-execution"
            copy_tracked_source(execution, snapshot)
            snapshot_manifest = snapshot / "packages/consumer/Cargo.toml"
            source_graph = json.loads(self._cargo_metadata_offline(manifest, locked=True).stdout)
            snapshot_graph = json.loads(self._cargo_metadata_offline(snapshot_manifest, locked=True).stdout)
            self.assertEqual({package["name"] for package in snapshot_graph["packages"]},
                             {package["name"] for package in source_graph["packages"]})

    def test_cargo_linked_entry_manifest_uses_its_declared_workspace_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            workspace = execution / "workspace"
            workspace.mkdir()
            target = execution / "manifest-source/Cargo.toml"
            target.parent.mkdir()
            target.write_text('[workspace]\nmembers=["members/leaf"]\n')
            (workspace / "Cargo.toml").symlink_to("../manifest-source/Cargo.toml")
            (workspace / "Cargo.lock").write_text("")
            leaf = workspace / "members/leaf"
            leaf.mkdir(parents=True)
            (leaf / "Cargo.toml").write_text(
                '[package]\nname="leaf"\nversion="0.1.0"\nedition="2021"\n')
            (leaf / "src").mkdir()
            (leaf / "src/lib.rs").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)

            manifest = workspace / "Cargo.toml"
            source = json.loads(self._cargo_metadata_offline(manifest).stdout)
            self._cargo_metadata_offline(manifest, locked=True)
            discovered = set(discover_local_repositories(manifest, parent, execution))
            self.assertEqual(discovered, {execution.resolve()})
            snapshot = base / "snapshot/rs-execution"
            copy_tracked_source(execution, snapshot)
            snapshot_graph = json.loads(self._cargo_metadata_offline(
                snapshot / "workspace/Cargo.toml", locked=True).stdout)
            self.assertEqual({package["name"] for package in snapshot_graph["packages"]},
                             {package["name"] for package in source["packages"]})

    def test_absolute_tracked_manifest_link_is_rejected_by_discovery_and_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            target = execution / "Cargo.toml"
            declaration = execution / "linked/Cargo.toml"
            declaration.parent.mkdir()
            declaration.symlink_to(target)
            subprocess.run(["git", "-C", str(execution), "add", "linked/Cargo.toml"], check=True)
            with self.assertRaisesRegex(ValueError, "tracked symlink escapes snapshot"):
                copy_tracked_source(execution, base / "snapshot")
            with self.assertRaisesRegex(ValueError, "not present in tracked source view"):
                discover_local_repositories(declaration, parent, execution)

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_cargo_1_94_manifest_chain_rejects_unsafe_middle_hops(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            for kind in ("untracked", "absolute"):
                with self.subTest(middle_kind=kind):
                    repository = self.make_cargo_manifest_chain(
                        parent / f"rs-{kind}", f"fixture-{kind}", kind)
                    manifest = repository / "Cargo.toml"
                    lock_before = (repository / "Cargo.lock").read_bytes()
                    source_graph = json.loads(self._cargo_metadata_offline(
                        manifest, locked=True).stdout)
                    self.assertTrue(source_graph["packages"])
                    self.assertEqual((repository / "Cargo.lock").read_bytes(), lock_before)
                    self.assertTrue(self.old_direct_link_check_accepts(repository))

                    with self.assertRaisesRegex(ValueError, "middle.toml"):
                        discover_local_repositories(manifest, parent, repository)
                    with self.assertRaisesRegex(ValueError, "middle.toml"):
                        copy_tracked_source(repository, base / f"snapshot-{kind}")
                    self.assertEqual((repository / "Cargo.lock").read_bytes(), lock_before)

                    if kind == "untracked":
                        legacy = base / "legacy-untracked"
                        legacy.mkdir()
                        for relative in tracked_source_files(repository):
                            source = repository / relative
                            destination = legacy / relative
                            destination.parent.mkdir(parents=True, exist_ok=True)
                            if source.is_symlink():
                                destination.symlink_to(os.readlink(source))
                            elif source.is_file():
                                shutil.copyfile(source, destination)
                        self.assertFalse((legacy / "Cargo.toml").is_file())
                        old_result = subprocess.run(
                            ["cargo", "+1.94.0", "metadata", "--format-version", "1",
                             "--offline", "--locked", "--manifest-path", str(legacy / "Cargo.toml")],
                            capture_output=True, text=True, check=False)
                        self.assertNotEqual(old_result.returncode, 0)
                        self.assertIn("Cargo.toml", old_result.stderr)

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_cargo_1_94_fully_tracked_relative_manifest_chain_survives_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            repository = self.make_cargo_manifest_chain(parent / "rs-fixture", "fixture", "relative")
            manifest = repository / "Cargo.toml"
            lock_before = (repository / "Cargo.lock").read_bytes()
            source_graph = json.loads(self._cargo_metadata_offline(manifest, locked=True).stdout)
            self.assertEqual(set(discover_local_repositories(manifest, parent, repository)),
                             {repository.resolve()})
            snapshot = base / "snapshot"
            copy_tracked_source(repository, snapshot)
            snapshot_graph = json.loads(self._cargo_metadata_offline(
                snapshot / "Cargo.toml", locked=True).stdout)
            package_summary = lambda graph: sorted(
                (package["name"], package["version"], package["source"])
                for package in graph["packages"])
            self.assertEqual(package_summary(snapshot_graph), package_summary(source_graph))
            source_package = source_graph["packages"][0]
            snapshot_package = snapshot_graph["packages"][0]
            self.assertEqual(Path(source_package["manifest_path"]).resolve(), repository / "real.toml")
            self.assertEqual(Path(snapshot_package["manifest_path"]).resolve(), snapshot / "real.toml")
            self.assertEqual((repository / "Cargo.lock").read_bytes(), lock_before)
            self.assertEqual((snapshot / "Cargo.lock").read_bytes(), lock_before)

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_untracked_manifest_middle_hop_is_rejected_for_members_and_path_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            for context in ("explicit", "glob", "path-dependency"):
                with self.subTest(context=context):
                    repository = self.make_input_repository(parent / f"rs-{context}", context)
                    if context in ("explicit", "glob"):
                        pattern = "members/leaf" if context == "explicit" else "members/*"
                        root_manifest = (
                            '[package]\nname="workspace-root"\nversion="0.1.0"\n'
                            'edition="2021"\n[workspace]\nmembers=["' + pattern + '"]\n')
                        self.add_untracked_middle_member(repository, "members/leaf")
                    else:
                        root_manifest = (
                            '[package]\nname="path-root"\nversion="0.1.0"\n'
                            'edition="2021"\n[dependencies]\nleaf={path="leaf"}\n')
                        self.add_untracked_middle_member(repository, "leaf")
                    (repository / "Cargo.toml").write_text(root_manifest)
                    (repository / "Cargo.lock").unlink()
                    self._cargo_metadata_offline(repository / "Cargo.toml")
                    subprocess.run(["git", "-C", str(repository), "add",
                                    "Cargo.toml", "Cargo.lock"], check=True)
                    subprocess.run(["git", "-C", str(repository), "commit", "-qm",
                                    "member link fixture"], check=True)
                    self._cargo_metadata_offline(repository / "Cargo.toml", locked=True)

                    with self.assertRaisesRegex(ValueError, "middle.toml"):
                        discover_local_repositories(repository / "Cargo.toml", parent, repository)

    def test_workspace_member_manifest_link_outside_owning_repository_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            external = self.make_input_repository(parent / "rs-external", "external")
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["members/linked"]\n')
            declaration = execution / "members/linked/Cargo.toml"
            declaration.parent.mkdir(parents=True)
            declaration.symlink_to("../../../rs-external/Cargo.toml")
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml", "members/linked/Cargo.toml"], check=True)
            self.assert_manifest_link_rejected_before_target_read(
                execution / "Cargo.toml", parent, execution,
                declaration, external / "Cargo.toml")

    def test_entry_manifest_link_outside_owning_repository_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            external = self.make_input_repository(parent / "rs-external", "external")
            declaration = execution / "entry/Cargo.toml"
            declaration.parent.mkdir()
            declaration.symlink_to("../../rs-external/Cargo.toml")
            subprocess.run(["git", "-C", str(execution), "add", "entry/Cargo.toml"], check=True)
            self.assert_manifest_link_rejected_before_target_read(
                declaration, parent, execution, declaration, external / "Cargo.toml")

    def test_local_dependency_manifest_link_outside_owning_repository_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            external = self.make_input_repository(parent / "rs-external", "external")
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nexternal={path="dependencies/linked"}\n')
            declaration = execution / "dependencies/linked/Cargo.toml"
            declaration.parent.mkdir(parents=True)
            declaration.symlink_to("../../../rs-external/Cargo.toml")
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml", "dependencies/linked/Cargo.toml"], check=True)
            self.assert_manifest_link_rejected_before_target_read(
                execution / "Cargo.toml", parent, execution,
                declaration, external / "Cargo.toml")

    def test_workspace_glob_skips_tracked_directory_removed_from_worktree(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            manifest = execution / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["members/*"]\n')
            (execution / "Cargo.lock").write_text("")
            kept = execution / "members/kept"
            removed = execution / "members/removed"
            for member, name in ((kept, "kept"), (removed, "removed")):
                member.mkdir(parents=True)
                (member / "Cargo.toml").write_text(
                    f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2021"\n')
                (member / "src").mkdir()
                (member / "src/lib.rs").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            shutil.rmtree(removed)

            source = json.loads(self._cargo_metadata_offline(manifest).stdout)
            source_members = {package["name"] for package in source["packages"]
                              if package["id"] in source["workspace_members"]}
            self.assertEqual(source_members, {"execution", "kept"})
            self._cargo_metadata_offline(manifest, locked=True)
            discovered = set(discover_local_repositories(manifest, parent, execution))
            self.assertEqual(discovered, {execution.resolve()})
            snapshot = base / "snapshot/rs-execution"
            copy_tracked_source(execution, snapshot)
            graph = json.loads(self._cargo_metadata_offline(snapshot / "Cargo.toml", locked=True).stdout)
            snapshot_members = {package["name"] for package in graph["packages"]
                                if package["id"] in graph["workspace_members"]}
            self.assertEqual(snapshot_members, source_members)

            no_manifest = execution / "members/no-manifest"
            no_manifest.mkdir()
            (no_manifest / "README.md").write_text("matching directory, no manifest\n")
            subprocess.run(["git", "-C", str(execution), "add", "members/no-manifest/README.md"],
                           check=True)
            source_result = subprocess.run(
                ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
                 "--manifest-path", str(manifest)], cwd=execution, capture_output=True,
                text=True, check=False)
            self.assertEqual(set(discover_local_repositories(manifest, parent, execution)),
                             {execution.resolve()})
            shutil.rmtree(snapshot)
            copy_tracked_source(execution, snapshot)
            snapshot_result = subprocess.run(
                ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
                 "--manifest-path", str(snapshot / "Cargo.toml")], cwd=snapshot,
                capture_output=True, text=True, check=False)
            self.assertEqual(snapshot_result.returncode, source_result.returncode,
                             source_result.stderr + "\n" + snapshot_result.stderr)
            if source_result.returncode:
                self.assertIn("no-manifest", source_result.stderr)

    def test_workspace_glob_fixed_prefix_outside_sibling_boundary_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            (base / "outside").mkdir()
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["../../outside/*"]\n')
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml"], check=True)
            with self.assertRaisesRegex(ValueError,
                                        r"workspace member glob.*Cargo.toml.*escapes sibling boundary"):
                discover_local_repositories(execution / "Cargo.toml", parent, execution)

    def test_workspace_glob_rejects_tracked_manifest_symlink_outside_sibling_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            external = self.make_input_repository(parent / "rs-external", "external")
            (execution / "Cargo.toml").write_text(
                '[workspace]\nmembers=["../rs-external/leaf-*"]\n')
            escaped_manifest = base / "outside-Cargo.toml"
            escaped_manifest.write_text(
                '[package]\nname="escaped"\nversion="0.1.0"\nedition="2021"\n')
            linked = external / "leaf-escape"
            linked.mkdir()
            (linked / "Cargo.toml").symlink_to(escaped_manifest)
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml"], check=True)
            subprocess.run(["git", "-C", str(external), "add", "leaf-escape/Cargo.toml"],
                           check=True)
            with self.assertRaisesRegex(
                    ValueError,
                    r"workspace member glob.*resolves outside sibling boundary"):
                discover_local_repositories(execution / "Cargo.toml", parent, execution)

    def test_workspace_glob_respects_literal_exclude_for_sibling_members(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            external = self.make_input_repository(parent / "rs-external", "external")
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["../rs-external/leaf-*"]\n'
                'exclude=["../rs-external/leaf-excluded"]\n')
            for name in ("leaf-kept", "leaf-excluded"):
                member = external / name
                member.mkdir()
                (member / "Cargo.toml").write_text(
                    f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2021"\n'
                    'workspace="../../rs-execution"\n')
                (member / "src").mkdir()
                (member / "src/lib.rs").write_text("")
            (execution / "Cargo.lock").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            subprocess.run(["git", "-C", str(external), "add", "leaf-kept/Cargo.toml",
                            "leaf-kept/src/lib.rs", "leaf-excluded/Cargo.toml",
                            "leaf-excluded/src/lib.rs"], check=True)

            root_data = __import__("tomllib").loads((execution / "Cargo.toml").read_text())
            members = _workspace_member_manifests(execution, root_data, parent)
            member_paths = {manifest.parent.resolve() for manifest in members}
            self.assertIn((execution / "Cargo.toml").parent.resolve(), member_paths)
            self.assertIn((external / "leaf-kept").resolve(), member_paths)
            self.assertNotIn((external / "leaf-excluded").resolve(), member_paths)

    def test_cargo_1_94_crates_io_registry_spellings_share_patch_identity(self):
        index = "https://github.com/rust-lang/crates.io-index"
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            dependency = self.make_input_repository(parent / "rs-config-dependency", "patched")
            manifest = execution / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\npatched="0.1.0"\n'
                f'[patch."{index}"]\npatched={{path="../missing-old-patch"}}\n')
            cargo_dir = execution / ".cargo"
            cargo_dir.mkdir()
            (cargo_dir / "config.toml").write_text(
                '[patch.crates-io]\npatched={path="../rs-config-dependency"}\n')
            (execution / "Cargo.lock").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)

            cargo = subprocess.run(
                ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
                 "--manifest-path", str(manifest)], cwd=execution, capture_output=True,
                text=True, check=False)
            self.assertEqual(cargo.returncode, 0, cargo.stderr)
            cargo_graph = json.loads(cargo.stdout)
            patched = next(package for package in cargo_graph["packages"]
                           if package["name"] == "patched")
            self.assertEqual(Path(patched["manifest_path"]).resolve(), dependency / "Cargo.toml")
            discovered = set(discover_local_repositories(manifest, parent, execution))
            self.assertEqual(discovered, {execution.resolve(), dependency.resolve()})
            snapshot_root = base / "snapshot/siblings"
            for repository in discovered:
                copy_tracked_source(repository, snapshot_root / repository.relative_to(parent))
            snapshot = json.loads(self._cargo_metadata_offline(
                snapshot_root / execution.name / "Cargo.toml", locked=True).stdout)
            snapshot_patch = next(package for package in snapshot["packages"]
                                  if package["name"] == "patched")
            self.assertEqual(Path(snapshot_patch["manifest_path"]).resolve(),
                             snapshot_root / dependency.name / "Cargo.toml")

            for registry in ("crates-io", index):
                data = {"patch": {registry: {"patched": {"path": "../patch"}}}}
                declarations = list(_manifest_patch_declarations(manifest, data))
                self.assertEqual(declarations, [(('crates-io', 'patched'), '../patch')])
                self.assertEqual(list(_config_patch_declarations(manifest, data)),
                                 [(('crates-io', 'patched'), {"path": "../patch"})])
            other = {"patch": {"https://example.invalid/index": {"patched": {"path": "x"}},
                               "https://example.invalid/other": {"patched": {"path": "y"}}}}
            self.assertEqual([key for key, _ in _manifest_patch_declarations(manifest, other)],
                             [("https://example.invalid/index", "patched"),
                              ("https://example.invalid/other", "patched")])

    def test_core_cli_patch_exception_uses_normalized_registry_identity(self):
        index = "https://github.com/rust-lang/crates.io-index"
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            core = self.make_input_repository(parent / "rs-core", "qubit-event-bus")
            manifest = execution / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nqubit-event-bus="0.1.0"\n'
                f'[patch."{index}"]\nqubit-event-bus={{path="../missing-old-core"}}\n')
            (execution / "Cargo.lock").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
            self.assertEqual(cargo_config(core)[1],
                             f'patch.crates-io.qubit-event-bus.path="{core}"')
            cargo = subprocess.run(
                ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
                 "--manifest-path", str(manifest), *cargo_config(core)], cwd=execution,
                capture_output=True, text=True, check=False)
            self.assertEqual(cargo.returncode, 0, cargo.stderr)
            graph = json.loads(cargo.stdout)
            core_package = next(package for package in graph["packages"]
                                if package["name"] == "qubit-event-bus")
            self.assertEqual(Path(core_package["manifest_path"]).resolve(), core / "Cargo.toml")
            cli_patches = {(registry, name): str(path)
                           for registry, name, path in _cargo_config_patch_declarations(core)}
            discovered = discover_local_repositories(
                manifest, parent, execution, cli_patches=cli_patches)
            self.assertEqual(discovered, [execution.resolve()])

    def test_cargo_1_94_explicit_cross_sibling_member_contributes_dev_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            external = self.make_input_repository(parent / "rs-external", "external")
            member_dev = self.make_input_repository(parent / "rs-member-dev", "member-dev")
            (execution / "Cargo.toml").write_text(
                '[workspace]\nmembers=["../rs-external/member"]\n')
            (execution / "Cargo.lock").write_text("")
            member = external / "member"
            member.mkdir()
            (member / "Cargo.toml").write_text(
                '[package]\nname="external-member"\nversion="0.1.0"\nedition="2021"\n'
                'workspace="../../rs-execution"\n'
                '[dev-dependencies]\nmember-dev={path="../../rs-member-dev"}\n')
            (member / "src").mkdir()
            (member / "src/lib.rs").write_text("")
            (member_dev / "Cargo.toml").write_text(
                '[package]\nname="member-dev"\nversion="0.1.0"\nedition="2021"\n')
            for repository in (execution, external, member_dev):
                subprocess.run(["git", "-C", str(repository), "add", "-A"], check=True)

            manifest = execution / "Cargo.toml"
            source = json.loads(self._cargo_metadata_offline(manifest).stdout)
            self._cargo_metadata_offline(manifest, locked=True)
            self.assertEqual({package["name"] for package in source["packages"]},
                             {"external-member", "member-dev"})
            discovered = set(discover_local_repositories(manifest, parent, execution))
            self.assertEqual(discovered,
                             {execution.resolve(), external.resolve(), member_dev.resolve()})
            snapshot_root = base / "snapshot/siblings"
            for repository in discovered:
                copy_tracked_source(repository, snapshot_root / repository.relative_to(parent))
            snapshot = json.loads(self._cargo_metadata_offline(
                snapshot_root / execution.name / "Cargo.toml", locked=True).stdout)
            self.assertEqual({package["name"] for package in snapshot["packages"]},
                             {"external-member", "member-dev"})

    def test_cargo_1_94_workspace_exclude_uses_literal_paths(self):
        for pattern, expected in (("members/no-*", {"no-local", "no-glob"}),
                                  ("./members/no-local", {"no-glob"}),
                                  ("members", set())):
            with self.subTest(pattern=pattern), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                parent = base / "siblings"
                parent.mkdir()
                execution = self.make_input_repository(parent / "rs-execution-services", "execution")
                root_manifest = execution / "Cargo.toml"
                root_manifest.write_text(
                    '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                    '[workspace]\nmembers=["members/*"]\nexclude=[' + repr(pattern) + ']\n')
                for name in ("no-local", "no-glob"):
                    member = execution / "members" / name
                    member.mkdir(parents=True)
                    (member / "Cargo.toml").write_text(
                        f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2021"\n')
                    (member / "src").mkdir()
                    (member / "src/lib.rs").write_text("")
                (execution / "Cargo.lock").write_text("")
                subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
                cargo = json.loads(self._cargo_metadata_offline(root_manifest).stdout)
                cargo_members = {package["name"] for package in cargo["packages"]
                                 if package["name"].startswith("no-")}
                self.assertEqual(cargo_members, expected)
                discovered = set(discover_local_repositories(root_manifest, parent, execution))
                self.assertEqual(discovered, {execution.resolve()})

    def test_explicit_untracked_or_deleted_workspace_member_reports_both_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            root_manifest = execution / "Cargo.toml"
            root_manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["members/required"]\n')
            member_manifest = execution / "members/required/Cargo.toml"
            member_manifest.parent.mkdir(parents=True)
            member_manifest.write_text(
                '[package]\nname="required"\nversion="0.1.0"\nedition="2021"\n')
            with self.assertRaisesRegex(ValueError,
                                        r"Cargo.toml.*tracked source.*members/required/Cargo.toml"):
                discover_local_repositories(root_manifest, parent, execution)
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml",
                            "members/required/Cargo.toml"], check=True)
            self.assertEqual(set(discover_local_repositories(root_manifest, parent, execution)),
                             {execution.resolve()})
            member_manifest.unlink()
            with self.assertRaisesRegex(ValueError,
                                        r"Cargo.toml.*tracked source.*members/required/Cargo.toml"):
                discover_local_repositories(root_manifest, parent, execution)

    def test_explicit_workspace_member_outside_sibling_boundary_is_rejected_before_tracking(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            root_manifest = execution / "Cargo.toml"
            root_manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["../../../outside/member"]\n')
            def tracked(path):
                if Path(path).resolve() == root_manifest.resolve():
                    return True
                raise AssertionError("out-of-bound member was checked for tracking")

            with patch("scripts.check_downstream_compatibility._tracked_file",
                       side_effect=tracked):
                with self.assertRaisesRegex(ValueError,
                                            r"workspace member.*escapes sibling boundary.*outside/member"):
                    discover_local_repositories(root_manifest, parent, execution)

    def test_initial_package_workspace_root_is_boundary_checked_before_read(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            fixture = execution / "tests/fixtures/consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="consumer"\nversion="0.1.0"\nedition="2021"\n'
                'workspace="../../../../../outside-workspace"\n'
                '[patch.crates-io]\nmissing={path="../../../../missing"}\n')
            subprocess.run(["git", "-C", str(execution), "add", str(manifest)], check=True)
            with self.assertRaisesRegex(ValueError, "initial workspace root escapes sibling boundary"):
                discover_local_repositories(manifest, parent, execution)

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
        command = ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
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
            (member / "src").mkdir()
            (member / "src/lib.rs").write_text("pub fn consumer() {}\n")
            nested = execution / "tests/fixtures/nested-consumer"
            nested.mkdir(parents=True)
            (nested / "Cargo.toml").write_text(
                '[package]\nname="nested-consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\n[workspace.dependencies]\nnested={path="../../../../rs-nested-dep"}\n'
                '[dependencies]\nnested={workspace=true}\n')
            (nested / "src").mkdir()
            (nested / "src/lib.rs").write_text("pub fn nested() {}\n")
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[workspace]\nmembers=["workspace/members/consumer", "tests/fixtures/nested-consumer"]\n'
                '[workspace.dependencies]\nunreferenced={path="../rs-unreferenced"}\n')
            (execution / "Cargo.lock").write_text("")
            (execution / "src").mkdir(exist_ok=True)
            (execution / "src/lib.rs").write_text("pub fn root() {}\n")
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)

            found_member = set(discover_local_repositories(member / "Cargo.toml", parent))
            found_nested = set(discover_local_repositories(nested / "Cargo.toml", parent))
            found_root = set(discover_local_repositories(
                execution / "Cargo.toml", parent, execution))
            self.assertEqual(found_member, {
                execution.resolve(), (parent / "rs-non-default-dep").resolve()})
            self.assertEqual(found_nested, {
                execution.resolve(), (parent / "rs-nested-dep").resolve()})
            self.assertEqual(found_root, {execution.resolve()})
            with self.assertRaisesRegex(AssertionError, "multiple workspace roots found"):
                self._cargo_metadata_offline(execution / "Cargo.toml")

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

    def test_cargo_1_94_effective_patch_overrides_missing_older_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            tracked = self.make_input_repository(parent / "rs-tracked-dependency", "patch-dependency")
            core = self.make_input_repository(parent / "rs-core", "qubit-event-bus")
            (tracked / "Cargo.toml").write_text(
                '[package]\nname="patch-dependency"\nversion="0.1.0"\n'
                'edition="2021"\n')
            (tracked / "Cargo.lock").write_text("")
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            manifest = fixture / "Cargo.toml"
            (core / "Cargo.toml").write_text(
                '[package]\nname="qubit-event-bus"\nversion="0.21.0"\nedition="2021"\n')
            (core / "Cargo.lock").write_text("")
            manifest.write_text(
                '[package]\nname="ioc-consumer"\nversion="0.1.0"\n'
                'edition="2021"\n[dependencies]\npatch-dependency="0.1.0"\n'
                'qubit-event-bus="0.21"\n'
                '[patch.crates-io]\npatch-dependency={path="../../../../missing-manifest"}\n'
                'qubit-event-bus={path="../../../../missing-core"}\n')
            (fixture / "Cargo.lock").write_text("")
            (fixture / "src").mkdir()
            (fixture / "src/lib.rs").write_text("pub fn fixture() {}\n")
            config = execution / ".cargo/config.toml"
            config.parent.mkdir()
            config.write_text(
                '[patch.crates-io]\npatch-dependency={path="../rs-tracked-dependency"}\n')
            for root in (execution, tracked, core):
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
                subprocess.run(["git", "-C", str(root), "commit", "-qm", "cargo fixture"],
                               check=True)

            config_args = [*cargo_config(core), "--config",
                           f'patch.crates-io.patch-dependency.path="{tracked}"']
            result = subprocess.run(
                ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
                 "--manifest-path", str(manifest), *config_args],
                cwd=fixture, capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            package = next(item for item in json.loads(result.stdout)["packages"]
                           if item["name"] == "patch-dependency")
            self.assertEqual(Path(package["manifest_path"]), tracked / "Cargo.toml")
            self.assertEqual(set(discover_local_repositories(
                manifest, parent, execution,
                cli_patches={(registry, name): str(path) for registry, name, path
                             in _cargo_config_patch_declarations(core)})),
                             {execution.resolve(), tracked.resolve()})
            snapshots_root = base / "snapshot/siblings"
            copy_tracked_source(execution, snapshots_root / execution.name)
            copy_tracked_source(tracked, snapshots_root / tracked.name)
            snapshot_manifest = (snapshots_root / execution.name /
                                 "tests/fixtures/ioc_application_consumer/Cargo.toml")
            snapshot_result = subprocess.run(
                ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline", "--locked",
                 "--manifest-path", str(snapshot_manifest), *cargo_config(core)],
                cwd=snapshot_manifest.parent, capture_output=True, text=True, check=False)
            self.assertEqual(snapshot_result.returncode, 0, snapshot_result.stderr)
            snapshot_package = next(item for item in json.loads(snapshot_result.stdout)["packages"]
                                    if item["name"] == "patch-dependency")
            self.assertEqual(Path(snapshot_package["manifest_path"]),
                             snapshots_root / tracked.name / "Cargo.toml")

            cli_args = cargo_config(core)
            cli_result = subprocess.run(
                ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
                 "--manifest-path", str(manifest), *cli_args],
                cwd=fixture, capture_output=True, text=True, check=False)
            self.assertEqual(cli_result.returncode, 0, cli_result.stderr)
            cli_package = next(item for item in json.loads(cli_result.stdout)["packages"]
                               if item["name"] == "qubit-event-bus")
            self.assertEqual(Path(cli_package["manifest_path"]), core / "Cargo.toml")
            self.assertEqual(set(discover_local_repositories(
                manifest, parent, execution,
                cli_patches={(registry, name): str(path) for registry, name, path
                             in _cargo_config_patch_declarations(core)})),
                {execution.resolve(), tracked.resolve()})

    def test_cargo_1_94_config_git_patch_hides_stale_manifest_path(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            git_patch = self.make_input_repository(parent / "rs-git-patch", "patched")
            (git_patch / "Cargo.toml").write_text(
                '[package]\nname="patched"\nversion="0.1.0"\nedition="2021"\n')
            (git_patch / "src/lib.rs").write_text("")
            subprocess.run(["git", "-C", str(git_patch), "add", "-A"], check=True)
            subprocess.run(["git", "-C", str(git_patch), "commit", "-qm", "git patch package"],
                           check=True)
            manifest = execution / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\npatched="0.1.0"\n'
                '[patch.crates-io]\npatched={path="../missing-old-patch"}\n')
            config = execution / ".cargo/config.toml"
            config.parent.mkdir()
            config.write_text(
                '[patch.crates-io]\npatched={git="file://' + str(git_patch) + '"}\n')
            (execution / "Cargo.lock").write_text("")
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.toml", "Cargo.lock",
                            ".cargo/config.toml"], check=True)
            cargo_home = base / "cargo-home"
            env = os.environ.copy()
            env["CARGO_HOME"] = str(cargo_home)
            preparer = base / "git-cache-preparer"
            preparer.mkdir()
            (preparer / "Cargo.toml").write_text(
                '[package]\nname="git-cache-preparer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\npatched={git="file://' + str(git_patch) + '"}\n')
            (preparer / "src").mkdir()
            (preparer / "src/main.rs").write_text("fn main() {}\n")
            prepare_env = env.copy()
            prepare_env.pop("CARGO_NET_OFFLINE", None)
            prepared = subprocess.run(
                ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--manifest-path",
                 str(preparer / "Cargo.toml")], cwd=preparer, env=prepare_env,
                capture_output=True, text=True, check=False)
            self.assertEqual(prepared.returncode, 0, prepared.stderr)
            command = ["cargo", "+1.94.0", "metadata", "--format-version", "1",
                       "--offline", "--manifest-path", str(manifest)]
            result = subprocess.run(command, cwd=execution, env=env, capture_output=True,
                                    text=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            graph = json.loads(result.stdout)
            package = next(item for item in graph["packages"] if item["name"] == "patched")
            self.assertTrue(package["source"].startswith("git+file://"))
            subprocess.run(["git", "-C", str(execution), "add", "Cargo.lock"], check=True)
            source_lock = (execution / "Cargo.lock").read_bytes()
            with patch("scripts.check_downstream_compatibility._config_patch_declarations",
                       return_value=[]):
                with self.assertRaisesRegex(ValueError, "missing local path"):
                    discover_local_repositories(manifest, parent, execution)
            self.assertEqual(set(discover_local_repositories(manifest, parent, execution)),
                             {execution.resolve()})
            self.assertEqual((execution / "Cargo.lock").read_bytes(), source_lock)
            snapshot_root = base / "snapshot/siblings"
            copy_tracked_source(execution, snapshot_root / execution.name)
            locked = subprocess.run(
                ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline", "--locked",
                 "--manifest-path", str(snapshot_root / execution.name / "Cargo.toml")],
                cwd=snapshot_root / execution.name, env=env, capture_output=True, text=True,
                check=False)
            self.assertEqual(locked.returncode, 0, locked.stderr)

    def test_cargo_1_94_external_workspace_root_is_copied_without_unrelated_members(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            external = self.make_input_repository(parent / "rs-external", "external")
            workspace = self.make_input_repository(parent / "rs-workspace", "workspace")
            inherited = self.make_input_repository(parent / "rs-inherited", "inherited")
            unused = self.make_input_repository(parent / "rs-unused", "unused")
            (workspace / "Cargo.toml").write_text(
                '[workspace]\nmembers=["../rs-external", "unused-member"]\n'
                '[workspace.package]\nversion="0.5.0"\n'
                '[workspace.dependencies]\ninherited={path="../rs-inherited"}\n')
            unused_member = workspace / "unused-member"
            unused_member.mkdir()
            (unused_member / "Cargo.toml").write_text(
                '[package]\nname="unused-member"\nversion.workspace=true\nedition="2021"\n'
                '[dev-dependencies]\nunused={path="../../rs-unused"}\n')
            (unused_member / "src").mkdir()
            (unused_member / "src/lib.rs").write_text("")
            (external / "Cargo.toml").write_text(
                '[package]\nname="external"\nversion.workspace=true\nedition="2021"\n'
                'workspace="../rs-workspace"\n[dependencies]\n'
                'inherited={workspace=true}\n[dev-dependencies]\n'
                'unused={path="../rs-unused"}\n')
            (external / "src/lib.rs").write_text("")
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nexternal={path="../rs-external"}\n')
            for root in (execution, external, workspace, inherited, unused):
                (root / "Cargo.lock").write_text("")
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
            graph = json.loads(self._cargo_metadata_offline(execution / "Cargo.toml").stdout)
            self.assertEqual(next(package["version"] for package in graph["packages"]
                                  if package["name"] == "external"), "0.5.0")
            repositories = set(discover_local_repositories(
                execution / "Cargo.toml", parent, execution))
            self.assertEqual(repositories, {
                execution.resolve(), external.resolve(), workspace.resolve(), inherited.resolve()})
            snapshot_root = base / "snapshot/siblings"
            for root in repositories:
                copy_tracked_source(root, snapshot_root / root.name)
            snapshot_manifest = snapshot_root / execution.name / "Cargo.toml"
            self._cargo_metadata_offline(snapshot_manifest)
            self._cargo_metadata_offline(snapshot_manifest, locked=True)
            self.assertFalse((snapshot_root / unused.name).exists())

    def test_external_package_workspace_root_reports_missing_and_out_of_bounds_roots(self):
        for workspace_path, expected in (("../missing-workspace", "cannot read workspace manifest"),
                                         ("../../outside-workspace", "workspace root escapes sibling boundary")):
            with self.subTest(workspace_path=workspace_path), tempfile.TemporaryDirectory() as directory:
                parent = Path(directory) / "siblings"
                parent.mkdir()
                execution = self.make_input_repository(parent / "rs-execution", "execution")
                external = self.make_input_repository(parent / "rs-external", "external")
                (execution / "Cargo.toml").write_text(
                    '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                    '[dependencies]\nexternal={path="../rs-external"}\n')
                (external / "Cargo.toml").write_text(
                    '[package]\nname="external"\nversion="0.1.0"\nedition="2021"\n'
                    f'workspace="{workspace_path}"\n')
                for root in (execution, external):
                    subprocess.run(["git", "-C", str(root), "add", "Cargo.toml"], check=True)
                with self.assertRaisesRegex(ValueError, expected):
                    discover_local_repositories(execution / "Cargo.toml", parent, execution)

    def test_cargo_1_94_workspace_member_and_external_dev_dependency_scope(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution-services", "execution")
            external = self.make_input_repository(parent / "rs-external-dependency", "external")
            member_dep = self.make_input_repository(parent / "rs-member-dev-dependency", "member-dev")
            external_build = self.make_input_repository(parent / "rs-external-build", "external-build")
            for repository, name in ((external, "external"), (member_dep, "member-dev"),
                                     (external_build, "external-build")):
                (repository / "Cargo.toml").write_text(
                    f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2021"\n')
                (repository / "Cargo.lock").write_text("")
            (external / "Cargo.toml").write_text(
                '[package]\nname="external"\nversion="0.1.0"\nedition="2021"\n'
                '[dev-dependencies]\nmissing-dev={path="../missing-external-dev"}\n'
                '[build_dependencies]\nexternal-build={path="../rs-external-build"}\n')
            fixture = execution / "tests/fixtures/ioc_application_consumer"
            fixture.mkdir(parents=True)
            (fixture / "Cargo.toml").write_text(
                '[package]\nname="ioc-consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\nexternal={path="../../../../rs-external-dependency"}\n')
            (fixture / "src").mkdir()
            (fixture / "src/lib.rs").write_text("pub fn fixture() {}\n")
            other = execution / "members/other"
            other.mkdir(parents=True)
            (other / "Cargo.toml").write_text(
                '[package]\nname="other-member"\nversion="0.1.0"\nedition="2021"\n'
                '[dev-dependencies]\nmember-dev={path="../../../rs-member-dev-dependency"}\n')
            (other / "src").mkdir()
            (other / "src/lib.rs").write_text("pub fn other() {}\n")
            root_manifest = execution / "Cargo.toml"
            root_manifest.write_text(
                '[workspace]\nmembers=["tests/fixtures/ioc_application_consumer", "members/other"]\n')
            (execution / "Cargo.lock").write_text("")
            for root in (execution, external, member_dep, external_build):
                subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
                subprocess.run(["git", "-C", str(root), "commit", "-qm", "cargo fixture"],
                               check=True)

            source = fixture / "Cargo.toml"
            source_result = self._cargo_metadata_offline(source)
            self._cargo_metadata_offline(source, locked=True)
            graph = json.loads(source_result.stdout)
            self.assertEqual(Path(next(item for item in graph["packages"]
                                       if item["name"] == "member-dev")["manifest_path"]),
                             member_dep / "Cargo.toml")
            discovered = set(discover_local_repositories(source, parent, execution))
            self.assertEqual(discovered, {execution.resolve(), external.resolve(), member_dep.resolve(),
                                          external_build.resolve()})
            snapshots_root = base / "snapshot/siblings"
            for repository in discovered:
                copy_tracked_source(repository, snapshots_root / repository.relative_to(parent))
            snapshot_manifest = (snapshots_root / execution.name /
                                "tests/fixtures/ioc_application_consumer/Cargo.toml")
            snapshot = self._cargo_metadata_offline(snapshot_manifest, locked=True)
            snapshot_graph = json.loads(snapshot.stdout)
            self.assertTrue(Path(next(item for item in snapshot_graph["packages"]
                                      if item["name"] == "member-dev")["manifest_path"])
                            .is_relative_to(snapshots_root))

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

    def test_cargo_config_patch_path_survives_nearer_metadata_and_nearer_path_wins(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            outer = self.make_input_repository(parent / "rs-outer", "patched")
            inner = self.make_input_repository(parent / "rs-inner", "patched")
            for repository in (outer, inner):
                (repository / "Cargo.toml").write_text(
                    '[package]\nname="patched"\nversion="0.1.0"\nedition="2021"\n')
            fixture = execution / "tests/fixtures/consumer"
            fixture.mkdir(parents=True)
            (fixture / "Cargo.toml").write_text(
                '[package]\nname="consumer"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\npatched="0.1.0"\n')
            (fixture / "src").mkdir()
            (fixture / "src/lib.rs").write_text("")
            (execution / "Cargo.toml").write_text(
                '[workspace]\nmembers=["tests/fixtures/consumer"]\n')
            (execution / "Cargo.lock").write_text("")
            outer_config = execution / ".cargo/config.toml"
            outer_config.parent.mkdir()
            outer_config.write_text(
                '[patch.crates-io]\npatched={path="../rs-outer"}\n')
            inner_config = fixture / ".cargo/config.toml"
            inner_config.parent.mkdir()
            inner_config.write_text(
                '[patch.crates-io]\npatched={version="=0.1.0"}\n')
            for repository in (execution, outer, inner):
                subprocess.run(["git", "-C", str(repository), "add", "-A"], check=True)

            def graph():
                result = subprocess.run(
                    ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
                     "--manifest-path", str(fixture / "Cargo.toml")], cwd=fixture,
                    capture_output=True, text=True, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                return json.loads(result.stdout)

            first = graph()
            self.assertEqual(Path(next(package["manifest_path"] for package in first["packages"]
                                        if package["name"] == "patched")), outer / "Cargo.toml")
            discovered = set(discover_local_repositories(fixture / "Cargo.toml", parent, execution))
            self.assertEqual(discovered, {execution.resolve(), outer.resolve()})
            self._cargo_metadata_offline(fixture / "Cargo.toml", locked=True)
            source_lock = (execution / "Cargo.lock").read_bytes()
            first_snapshot_root = base / "first-snapshot/siblings"
            for repository in discovered:
                copy_tracked_source(repository, first_snapshot_root / repository.relative_to(parent))
            first_snapshot_manifest = (first_snapshot_root / execution.name /
                                       "tests/fixtures/consumer/Cargo.toml")
            first_snapshot = json.loads(self._cargo_metadata_offline(
                first_snapshot_manifest, locked=True).stdout)
            self.assertEqual(Path(next(package["manifest_path"] for package in first_snapshot["packages"]
                                       if package["name"] == "patched")),
                             first_snapshot_root / outer.name / "Cargo.toml")
            self.assertEqual((execution / "Cargo.lock").read_bytes(), source_lock)

            inner_config.write_text(
                '[patch.crates-io]\npatched={path="../../../../rs-inner", version="=0.1.0"}\n')
            subprocess.run(["git", "-C", str(execution), "add", str(inner_config)], check=True)
            second = graph()
            self.assertEqual(Path(next(package["manifest_path"] for package in second["packages"]
                                        if package["name"] == "patched")), inner / "Cargo.toml")
            discovered = set(discover_local_repositories(fixture / "Cargo.toml", parent, execution))
            self.assertEqual(discovered, {execution.resolve(), inner.resolve()})
            self._cargo_metadata_offline(fixture / "Cargo.toml", locked=True)
            source_lock = (execution / "Cargo.lock").read_bytes()
            second_snapshot_root = base / "second-snapshot/siblings"
            for repository in discovered:
                copy_tracked_source(repository, second_snapshot_root / repository.relative_to(parent))
            second_snapshot_manifest = (second_snapshot_root / execution.name /
                                        "tests/fixtures/consumer/Cargo.toml")
            second_snapshot = json.loads(self._cargo_metadata_offline(
                second_snapshot_manifest, locked=True).stdout)
            self.assertEqual(Path(next(package["manifest_path"] for package in second_snapshot["packages"]
                                       if package["name"] == "patched")),
                             second_snapshot_root / inner.name / "Cargo.toml")
            self.assertEqual((execution / "Cargo.lock").read_bytes(), source_lock)

    def test_cargo_194_config_include_discovers_and_snapshots_patch_sibling(self):
        for manifest_patch in (False, True):
            with self.subTest(manifest_patch=manifest_patch), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                parent = base / "siblings"
                parent.mkdir()
                execution = self.make_input_repository(parent / "rs-execution", "execution")
                patched = self.make_input_repository(parent / "rs-patched", "patched")
                (execution / "Cargo.toml").write_text(
                    '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                    '[dependencies]\npatched="=0.1.0"\n' +
                    ('[patch.crates-io]\npatched={path="../missing-old-patch"}\n'
                     if manifest_patch else ''))
                (execution / "Cargo.lock").write_text("")
                (patched / "Cargo.toml").write_text(
                    '[package]\nname="patched"\nversion="0.1.0"\nedition="2021"\n')
                cargo_dir = execution / ".cargo"
                cargo_dir.mkdir()
                (cargo_dir / "config.toml").write_text('include=["deps.toml"]\n')
                (cargo_dir / "deps.toml").write_text(
                    '[patch.crates-io]\npatched={path="../rs-patched"}\n')
                subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)
                subprocess.run(["git", "-C", str(patched), "add", "-A"], check=True)

                source = json.loads(self._cargo_metadata_offline(execution / "Cargo.toml").stdout)
                source_package = next(item for item in source["packages"]
                                      if item["name"] == "patched")
                self.assertEqual(Path(source_package["manifest_path"]), patched / "Cargo.toml")
                self._cargo_metadata_offline(execution / "Cargo.toml", locked=True)
                input_lock = (execution / "Cargo.lock").read_bytes()

                discovered = set(discover_local_repositories(
                    execution / "Cargo.toml", parent, execution))
                self.assertEqual(discovered, {execution.resolve(), patched.resolve()})
                with patch("scripts.check_downstream_compatibility._cargo_config_sources",
                           side_effect=lambda config_path, _source_root, **_kwargs: [config_path]):
                    if manifest_patch:
                        with self.assertRaisesRegex(ValueError, "missing local path"):
                            discover_local_repositories(
                                execution / "Cargo.toml", parent, execution)
                    else:
                        old_discovery = set(discover_local_repositories(
                            execution / "Cargo.toml", parent, execution))
                        self.assertEqual(old_discovery, {execution.resolve()})
                snapshot_root = base / "snapshot/siblings"
                for repository in discovered:
                    copy_tracked_source(repository, snapshot_root / repository.name)
                snapshot_manifest = snapshot_root / execution.name / "Cargo.toml"
                snapshot = json.loads(self._cargo_metadata_offline(
                    snapshot_manifest, locked=True).stdout)
                snapshot_package = next(item for item in snapshot["packages"]
                                        if item["name"] == "patched")
                self.assertEqual(Path(snapshot_package["manifest_path"]),
                                 snapshot_root / patched.name / "Cargo.toml")
                self.assertEqual((execution / "Cargo.lock").read_bytes(), input_lock)

    def test_cargo_config_include_order_optional_and_unsafe_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            parent = base / "siblings"
            parent.mkdir()
            execution = self.make_input_repository(parent / "rs-execution", "execution")
            one = self.make_input_repository(parent / "rs-one", "patched")
            two = self.make_input_repository(parent / "rs-two", "patched")
            for repository in (one, two):
                (repository / "Cargo.toml").write_text(
                    '[package]\nname="patched"\nversion="0.1.0"\nedition="2021"\n')
                subprocess.run(["git", "-C", str(repository), "add", "Cargo.toml"], check=True)
            (execution / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\npatched="=0.1.0"\n')
            (execution / "Cargo.lock").write_text("")
            cargo_dir = execution / ".cargo"
            nested = cargo_dir / "nested"
            nested.mkdir(parents=True)
            (cargo_dir / "config.toml").write_text('include=["one.toml", "nested/two.toml"]\n')
            (cargo_dir / "one.toml").write_text(
                'include=["leaf.toml", {path="absent-optional.toml", optional=true}]\n')
            (cargo_dir / "leaf.toml").write_text(
                '[patch.crates-io]\npatched={path="../rs-one"}\n')
            (nested / "two.toml").write_text(
                '[patch.crates-io]\npatched={path="../../rs-two"}\n')
            subprocess.run(["git", "-C", str(execution), "add", "-A"], check=True)

            def selected_source():
                result = subprocess.run(
                    ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
                     "--manifest-path", str(execution / "Cargo.toml")],
                    cwd=execution, capture_output=True, text=True, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                graph = json.loads(result.stdout)
                return Path(next(item["manifest_path"] for item in graph["packages"]
                                 if item["name"] == "patched"))

            self.assertEqual(selected_source(), two / "Cargo.toml")
            discovered = set(discover_local_repositories(
                execution / "Cargo.toml", parent, execution))
            self.assertEqual(discovered, {execution.resolve(), two.resolve()})

            (cargo_dir / "config.toml").write_text('include=["nested/two.toml", "one.toml"]\n')
            subprocess.run(["git", "-C", str(execution), "add", str(cargo_dir / "config.toml")],
                           check=True)
            self.assertEqual(selected_source(), one / "Cargo.toml")
            discovered = set(discover_local_repositories(
                execution / "Cargo.toml", parent, execution))
            self.assertEqual(discovered, {execution.resolve(), one.resolve()})

            for include, message in (("{path=\"missing-required.toml\", optional=false}",
                                      "required Cargo config include missing"),
                                     ('"/absolute.toml"', "absolute Cargo config include"),
                                     ('"../../outside.toml"', "escapes source repository")):
                (cargo_dir / "config.toml").write_text(f"include=[{include}]\n")
                subprocess.run(["git", "-C", str(execution), "add", str(cargo_dir / "config.toml")],
                               check=True)
                cargo = subprocess.run(
                    ["cargo", "+1.94.0", "metadata", "--format-version", "1", "--offline",
                     "--manifest-path", str(execution / "Cargo.toml")],
                    cwd=execution, capture_output=True, text=True, check=False)
                self.assertNotEqual(cargo.returncode, 0, cargo.stdout)
                with self.subTest(include=include), self.assertRaisesRegex(ValueError, message):
                    discover_local_repositories(execution / "Cargo.toml", parent, execution)

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

            malformed = parent / "rs-malformed"
            malformed.mkdir()
            malformed_manifest = malformed / "Cargo.toml"
            malformed_manifest.write_text("[package\n")
            manifest.write_text(
                '[dependencies]\nmalformed = { path = "../../../../rs-malformed" }\n')
            with self.assertRaisesRegex(
                    ValueError, rf"{malformed_manifest}.*declared in {manifest}"):
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
            subprocess.run(["git", "-C", str(execution), "add", str(manifest)], check=True)
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
