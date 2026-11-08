"""Subprocess coverage for the consolidated performance evidence contract."""

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
CHECKER = REPO / "scripts" / "check_performance_evidence.py"
SNAPSHOT = "e8a1ed724954756d48c3d5216cbd13ac39a0cb81"
ORDERING = "doc/ordering_performance_artifacts/README.md"
CAPACITY_ARCHIVE = "doc/capacity_performance_artifacts/20261104-review/README.md"
PERFORMANCE = "doc/performance.md"
ORDERING_LINK = f"https://github.com/qubit-ltd/rs-event-bus/tree/{SNAPSHOT}/doc/ordering_performance_artifacts"
CAPACITY_LINK = f"https://github.com/qubit-ltd/rs-event-bus/tree/{SNAPSHOT}/doc/capacity_performance_artifacts/20261104-review"


class PerformanceEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "scripts").mkdir()
        shutil.copy2(CHECKER, self.root / "scripts/check_performance_evidence.py")
        (self.root / Path(ORDERING).parent).mkdir(parents=True)
        (self.root / Path(CAPACITY_ARCHIVE).parent).mkdir(parents=True)
        (self.root / "doc").mkdir(exist_ok=True)
        archive_text = "# Evidence\n\n## Distribution and complete evidence\n\n"
        (self.root / ORDERING).write_text(archive_text + f"Consult the evidence: {ORDERING_LINK}\n", encoding="utf-8")
        (self.root / CAPACITY_ARCHIVE).write_text(archive_text + f"Consult the evidence: {CAPACITY_LINK}\n", encoding="utf-8")
        (self.root / PERFORMANCE).write_text(f"# Performance\n\n{ORDERING_LINK}\n{CAPACITY_LINK}\n", encoding="utf-8")
        self.listing = self.root / "package-list.txt"
        self.files = [ORDERING, PERFORMANCE]
        self.write_listing()

    def tearDown(self):
        self.temp.cleanup()

    def write_listing(self):
        self.listing.write_text("\n".join(self.files) + "\n", encoding="utf-8")

    def run_checker(self, list_path=None):
        return subprocess.run(
            [sys.executable, "-B", str(self.root / "scripts/check_performance_evidence.py"),
             "--package-list", str(list_path or self.listing)],
            cwd=self.root, text=True, capture_output=True, check=False,
        )

    def assert_rejected(self, expected_path):
        result = self.run_checker()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(expected_path, result.stderr)

    def test_valid_contract(self):
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_consolidated_document_must_be_packaged(self):
        self.files.remove(PERFORMANCE)
        self.write_listing()
        self.assert_rejected(PERFORMANCE)

    def test_missing_fixed_archive_link(self):
        (self.root / PERFORMANCE).write_text(f"# Performance\n\n{ORDERING_LINK}\n", encoding="utf-8")
        self.assert_rejected(PERFORMANCE)

    def test_floating_main_reference_rejected(self):
        (self.root / ORDERING).write_text(
            "# Evidence\n\n## Distribution and complete evidence\n\n"
            "https://github.com/qubit-ltd/rs-event-bus/tree/main/doc/ordering_performance_artifacts\n",
            encoding="utf-8",
        )
        self.assert_rejected(ORDERING)

    def test_wrong_archive_path_rejected(self):
        (self.root / ORDERING).write_text(
            f"# Evidence\n\n## Distribution and complete evidence\n\n{CAPACITY_LINK}\n", encoding="utf-8"
        )
        self.assert_rejected(ORDERING)

    def test_inconsistent_snapshots_rejected(self):
        (self.root / CAPACITY_ARCHIVE).write_text(
            (self.root / CAPACITY_ARCHIVE).read_text(encoding="utf-8").replace(SNAPSHOT, "0" * 40),
            encoding="utf-8",
        )
        self.assert_rejected(CAPACITY_ARCHIVE)

    def test_representative_evidence_must_not_be_packaged(self):
        self.files.append("doc/ordering_performance_artifacts/paired_samples.txt")
        self.write_listing()
        self.assert_rejected("doc/ordering_performance_artifacts/paired_samples.txt")

    def test_capacity_representative_evidence_must_not_be_packaged(self):
        self.files.append("doc/capacity_performance_artifacts/20261104-review/manifest.json")
        self.write_listing()
        self.assert_rejected("doc/capacity_performance_artifacts/20261104-review/manifest.json")

    def test_missing_package_list_rejected(self):
        missing = self.root / "missing-list.txt"
        result = self.run_checker(missing)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(missing), result.stderr)

    def test_unreadable_package_list_rejected(self):
        directory = self.root / "directory-list"
        directory.mkdir()
        result = self.run_checker(directory)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(directory), result.stderr)


if __name__ == "__main__":
    unittest.main()
