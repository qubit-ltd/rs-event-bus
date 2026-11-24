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
