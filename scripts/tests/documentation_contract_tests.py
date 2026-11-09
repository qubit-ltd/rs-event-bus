"""Regression tests for package-local documentation links."""

import importlib.util
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).parents[1] / "check_documentation_contract.py"
spec = importlib.util.spec_from_file_location("documentation_contract", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class DocumentationContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for directory in ("doc", "scripts", ".infra"):
            (self.root / directory).mkdir()
        for document in checker.DOCUMENTS:
            path = self.root / document
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# Guide\n\n[tool](scripts/tool.py)\n", encoding="utf-8")
        (self.root / "README.md").write_text("[Migration](doc/migration.md)\n", encoding="utf-8")
        (self.root / "README.zh_CN.md").write_text("[迁移](doc/migration.zh_CN.md)\n", encoding="utf-8")
        self.listing = self.root / "package-list.txt"
        self.listing.write_text("scripts/tool.py\n", encoding="utf-8")
        (self.root / "doc/user_guide.md").write_text("### Migrate from 0.20 to 0.21\n", encoding="utf-8")
        (self.root / "doc/user_guide.zh_CN.md").write_text("### 从 0.20 迁移到 0.21\n", encoding="utf-8")
        self._migration_pages()

    def tearDown(self):
        self.temp.cleanup()

    def test_valid_local_tool_links(self):
        self.assertTrue(checker.check(self.root, self.listing))

    def test_unlisted_local_tool_link_is_rejected(self):
        (self.root / "README.md").write_text("[missing](scripts/missing.py)\n", encoding="utf-8")
        self.assertFalse(checker.check(self.root, self.listing))

    def test_links_inside_fenced_code_are_ignored(self):
        (self.root / "README.md").write_text("[Migration](doc/migration.md)\n```md\n[missing](scripts/missing.py)\n```\n", encoding="utf-8")
        self.assertTrue(checker.check(self.root, self.listing))

    def test_missing_document_is_rejected(self):
        (self.root / "README.md").unlink()
        self.assertFalse(checker.check(self.root, self.listing))

    def test_missing_package_list_is_rejected(self):
        self.assertFalse(checker.check(self.root, self.root / "absent.txt"))

    def _migration_pages(self):
        (self.root / "doc/migration.md").write_text(
            "[简体中文](migration.zh_CN.md) [guide](user_guide.md#migrate-from-020-to-021)\n", encoding="utf-8")
        (self.root / "doc/migration.zh_CN.md").write_text(
            "[English](migration.md) [指南](user_guide.zh_CN.md#从-020-迁移到-021)\n", encoding="utf-8")
        self.listing.write_text("scripts/tool.py\ndoc/migration.md\ndoc/migration.zh_CN.md\n", encoding="utf-8")

    def test_migration_pages_exist_are_packaged_and_reciprocally_linked(self):
        self.assertTrue(checker.check_migration_contract(self.root, self.listing))

    def test_missing_migration_page_is_rejected(self):
        (self.root / "doc/migration.zh_CN.md").unlink()
        self.assertFalse(checker.check_migration_contract(self.root, self.listing))

    def test_unlisted_migration_page_is_rejected(self):
        self.listing.write_text("scripts/tool.py\ndoc/migration.md\n", encoding="utf-8")
        self.assertFalse(checker.check_migration_contract(self.root, self.listing))

    def test_broken_reciprocal_link_is_rejected(self):
        (self.root / "doc/migration.zh_CN.md").write_text("[English](elsewhere.md)\n", encoding="utf-8")
        self.assertFalse(checker.check_migration_contract(self.root, self.listing))

    def test_missing_guide_anchor_is_rejected(self):
        (self.root / "doc/migration.md").write_text("[简体中文](migration.zh_CN.md) [guide](user_guide.md#wrong)\n", encoding="utf-8")
        self.assertFalse(checker.check_migration_contract(self.root, self.listing))


if __name__ == "__main__":
    unittest.main()
