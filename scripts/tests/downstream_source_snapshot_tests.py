import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.downstream_source_snapshot import copy_tracked_source
from scripts.downstream_source_snapshot import is_tracked_snapshot_file
from scripts.downstream_source_snapshot import tracked_source_files
from scripts.downstream_source_snapshot import validate_tracked_snapshot_file


class DownstreamSourceSnapshotTests(unittest.TestCase):
    def make_repository(self, root):
        root.mkdir()
        subprocess.run(["git", "init", "-q", str(root)], check=True)
        subprocess.run(["git", "-C", str(root), "config", "user.name", "Snapshot Test"], check=True)
        subprocess.run(["git", "-C", str(root), "config", "user.email", "snapshot@example.invalid"], check=True)
        (root / "src").mkdir()
        (root / "src" / "lib.rs").write_text("pub fn original() {}\n")
        (root / "Cargo.lock").write_bytes(b"lock contents\n")
        (root / "deleted.rs").write_text("tracked before deletion\n")
        (root / "target").mkdir()
        (root / "target" / "ignored-artifact").write_text("not tracked\n")
        (root / "untracked.rs").write_text("not tracked\n")
        subprocess.run(["git", "-C", str(root), "add", "Cargo.lock", "deleted.rs", "src/lib.rs"], check=True)
        subprocess.run(["git", "-C", str(root), "commit", "-qm", "snapshot baseline"], check=True)
        (root / "src" / "lib.rs").write_text("pub fn edited() {}\n")
        (root / "deleted.rs").unlink()
        return root

    def test_copies_tracked_worktree_state_and_skips_untracked_and_deleted_files(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source = self.make_repository(base / "source")
            destination = base / "snapshot"

            copy_tracked_source(source, destination)

            self.assertEqual((destination / "Cargo.lock").read_bytes(), b"lock contents\n")
            self.assertEqual((destination / "src/lib.rs").read_text(), "pub fn edited() {}\n")
            self.assertFalse((destination / "deleted.rs").exists())
            self.assertFalse((destination / "untracked.rs").exists())
            self.assertFalse((destination / "target").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_copies_dangling_legacy_cargo_config_with_valid_config_toml(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source_parent = base / "source"
            source_parent.mkdir()
            source = self.make_repository(source_parent / "rs-execution")
            patched = self.make_repository(source_parent / "rs-patched")
            (source / "Cargo.toml").write_text(
                '[package]\nname="execution"\nversion="0.1.0"\nedition="2021"\n'
                '[dependencies]\npatched="=0.1.0"\n')
            (source / "Cargo.lock").write_text("")
            (patched / "Cargo.toml").write_text(
                '[package]\nname="patched"\nversion="0.1.0"\nedition="2021"\n')
            cargo_dir = source / ".cargo"
            cargo_dir.mkdir()
            (cargo_dir / "config").symlink_to("missing-old-config")
            config_toml = cargo_dir / "config.toml"
            config_contents = '[patch.crates-io]\npatched={path="../rs-patched"}\n'
            config_toml.write_text(config_contents)
            subprocess.run(["git", "-C", str(source), "add", "Cargo.toml", "Cargo.lock",
                            ".cargo/config", ".cargo/config.toml"], check=True)
            subprocess.run(["git", "-C", str(patched), "add", "Cargo.toml"], check=True)

            def cargo_metadata(manifest, *, locked=False):
                command = ["cargo", "+1.94.0", "metadata", "--offline", "--format-version", "1",
                           "--manifest-path", str(manifest)]
                if locked:
                    command.append("--locked")
                result = subprocess.run(command, cwd=manifest.parent, capture_output=True,
                                        text=True, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                return json.loads(result.stdout)

            source_graph = cargo_metadata(source / "Cargo.toml")
            source_patch = next(package for package in source_graph["packages"]
                                if package["name"] == "patched")
            self.assertEqual(Path(source_patch["manifest_path"]), patched / "Cargo.toml")
            cargo_metadata(source / "Cargo.toml", locked=True)
            source_lock = (source / "Cargo.lock").read_bytes()

            snapshot_parent = base / "snapshot"
            source_snapshot = snapshot_parent / source.name
            patched_snapshot = snapshot_parent / patched.name
            copy_tracked_source(source, source_snapshot)
            copy_tracked_source(patched, patched_snapshot)

            copied_legacy = source_snapshot / ".cargo/config"
            self.assertTrue(copied_legacy.is_symlink())
            self.assertEqual(os.readlink(copied_legacy), "missing-old-config")
            self.assertFalse((source_snapshot / ".cargo/missing-old-config").exists())
            self.assertEqual((source_snapshot / ".cargo/config.toml").read_text(), config_contents)

            snapshot_graph = cargo_metadata(source_snapshot / "Cargo.toml", locked=True)
            snapshot_patch = next(package for package in snapshot_graph["packages"]
                                  if package["name"] == "patched")
            self.assertEqual(Path(snapshot_patch["manifest_path"]),
                             patched_snapshot / "Cargo.toml")
            self.assertEqual((source / "Cargo.lock").read_bytes(), source_lock)

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_rejects_a_tracked_symlink_that_escapes_the_source_tree(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source = self.make_repository(base / "source")
            outside = base / "outside.txt"
            outside.write_text("outside\n")
            (source / "escape.txt").symlink_to(outside)
            subprocess.run(["git", "-C", str(source), "add", "escape.txt"], check=True)
            destination = base / "snapshot"

            with self.assertRaisesRegex(ValueError, "escape.txt"):
                copy_tracked_source(source, destination)

    def test_rejects_submodules_with_their_relative_path(self):
        with tempfile.TemporaryDirectory() as temp:
            source = self.make_repository(Path(temp) / "source")
            (source / "nested").mkdir()
            subprocess.run(["git", "-C", str(source), "update-index", "--add", "--cacheinfo",
                            "160000", "0123456789abcdef0123456789abcdef01234567", "nested/module"], check=True)

            with self.assertRaisesRegex(ValueError, "nested/module"):
                copy_tracked_source(source, Path(temp) / "snapshot")

    def test_preserves_internal_tracked_symlinks(self):
        if not hasattr(os, "symlink"):
            self.skipTest("symbolic links are unavailable")
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source = self.make_repository(base / "source")
            (source / "src" / "current.rs").symlink_to("lib.rs")
            subprocess.run(["git", "-C", str(source), "add", "src/current.rs"], check=True)

            destination = base / "snapshot"
            copy_tracked_source(source, destination)

            self.assertTrue((destination / "src/current.rs").is_symlink())
            self.assertEqual(os.readlink(destination / "src/current.rs"), "lib.rs")

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_revisiting_a_tracked_directory_link_is_snapshot_safe_and_cargo_parseable(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_repository(Path(temp) / "source")
            (root / "real").mkdir()
            (root / "real" / "actual.toml").write_text(
                '[package]\nname="fixture"\nversion="0.1.0"\nedition="2024"\n'
                '[lib]\npath="../src/lib.rs"\n'
            )
            (root / "alias").symlink_to("real", target_is_directory=True)
            (root / "Cargo.toml").symlink_to("alias/../alias/actual.toml")
            subprocess.run(
                ["git", "-C", str(root), "add", "Cargo.toml", "alias", "real/actual.toml"],
                check=True,
            )
            subprocess.run(
                ["cargo", "generate-lockfile", "--offline", "--manifest-path", str(root / "Cargo.toml")],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(["git", "-C", str(root), "add", "Cargo.lock"], check=True)
            tracked = set(tracked_source_files(root))
            declaration = root / "Cargo.toml"

            validate_tracked_snapshot_file(root, declaration, tracked)
            self.assertTrue(is_tracked_snapshot_file(root, declaration, tracked))
            destination = Path(temp) / "snapshot"
            copy_tracked_source(root, destination)
            self.assertEqual(os.readlink(destination / "Cargo.toml"), "alias/../alias/actual.toml")
            self.assertEqual(os.readlink(destination / "alias"), "real")

            for extra_args in ([], ["--locked"]):
                subprocess.run(
                    ["cargo", "metadata", "--offline", "--format-version", "1", "--manifest-path",
                     str(destination / "Cargo.toml"), *extra_args],
                    check=True,
                    capture_output=True,
                    text=True,
                )

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_discovery_rejects_untracked_intermediate_manifest_link(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_repository(Path(temp) / "source")
            (root / "real.toml").write_text('[package]\nname="fixture"\n')
            (root / "middle.toml").symlink_to("real.toml")
            (root / "Cargo.toml").symlink_to("middle.toml")
            subprocess.run(["git", "-C", str(root), "add", "Cargo.toml", "real.toml"], check=True)
            declaration = root / "Cargo.toml"
            tracked = set(tracked_source_files(root))

            self.assertFalse(is_tracked_snapshot_file(root, declaration, tracked))
            with self.assertRaisesRegex(ValueError, "middle.toml"):
                validate_tracked_snapshot_file(root, declaration, tracked)

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_accepts_an_ordinary_chain_of_tracked_manifest_links(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_repository(Path(temp) / "source")
            (root / "real.toml").write_text('[package]\nname="fixture"\n')
            (root / "middle.toml").symlink_to("real.toml")
            (root / "Cargo.toml").symlink_to("middle.toml")
            subprocess.run(
                ["git", "-C", str(root), "add", "Cargo.toml", "middle.toml", "real.toml"],
                check=True,
            )
            tracked = set(tracked_source_files(root))

            self.assertTrue(is_tracked_snapshot_file(root, root / "Cargo.toml", tracked))

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_rejects_a_self_referencing_manifest_link(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_repository(Path(temp) / "source")
            (root / "Cargo.toml").symlink_to("Cargo.toml")
            subprocess.run(["git", "-C", str(root), "add", "Cargo.toml"], check=True)

            with self.assertRaisesRegex(ValueError, "cycle.*Cargo.toml"):
                validate_tracked_snapshot_file(root, root / "Cargo.toml")

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_reports_expansion_limit_separately_from_a_link_cycle(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_repository(Path(temp) / "source")
            (root / "real.toml").write_text('[package]\nname="fixture"\n')
            (root / "Cargo.toml").symlink_to("link0.toml")
            for index in range(41):
                target = f"link{index + 1}.toml" if index < 40 else "real.toml"
                (root / f"link{index}.toml").symlink_to(target)
            subprocess.run(
                ["git", "-C", str(root), "add", "Cargo.toml", "link*.toml", "real.toml"],
                check=True,
            )

            with self.assertRaisesRegex(ValueError, "expansion limit exceeded.*40 hops"):
                validate_tracked_snapshot_file(root, root / "Cargo.toml")

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_rejects_an_absolute_manifest_link_target(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_repository(Path(temp) / "source")
            (root / "Cargo.toml").symlink_to(root / "real.toml")
            (root / "real.toml").write_text('[package]\nname="fixture"\n')
            subprocess.run(["git", "-C", str(root), "add", "Cargo.toml", "real.toml"], check=True)

            with self.assertRaisesRegex(ValueError, "absolute symlink target.*Cargo.toml"):
                validate_tracked_snapshot_file(root, root / "Cargo.toml")

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_snapshot_rejects_manifest_chain_through_untracked_intermediate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_repository(Path(temp) / "source")
            (root / "Cargo.toml").write_text('[package]\nname="fixture"\n')
            (root / "Cargo.toml").rename(root / "real.toml")
            (root / "middle.toml").symlink_to("real.toml")
            (root / "Cargo.toml").symlink_to("middle.toml")
            subprocess.run(["git", "-C", str(root), "add", "Cargo.toml", "real.toml"], check=True)

            with self.assertRaisesRegex(ValueError, "middle.toml"):
                copy_tracked_source(root, Path(temp) / "snapshot")

    @unittest.skipUnless(hasattr(os, "symlink"), "symbolic links are unavailable")
    def test_rejects_manifest_link_cycles_and_missing_targets_with_hop(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_repository(Path(temp) / "source")
            (root / "Cargo.toml").symlink_to("middle.toml")
            (root / "middle.toml").symlink_to("Cargo.toml")
            subprocess.run(["git", "-C", str(root), "add", "Cargo.toml", "middle.toml"], check=True)
            tracked = set(tracked_source_files(root))
            with self.assertRaisesRegex(ValueError, "cycle.*Cargo.toml|cycle.*middle.toml"):
                validate_tracked_snapshot_file(root, root / "Cargo.toml", tracked)

            (root / "middle.toml").unlink()
            (root / "middle.toml").symlink_to("missing.toml")
            with self.assertRaisesRegex((ValueError, FileNotFoundError), "missing.toml"):
                validate_tracked_snapshot_file(root, root / "Cargo.toml", tracked)


if __name__ == "__main__":
    unittest.main()
