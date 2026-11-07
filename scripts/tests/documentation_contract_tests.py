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
        self.listing = self.root / "package-list.txt"
        self.listing.write_text("scripts/tool.py\n", encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def test_valid_local_tool_links(self):
        self.assertTrue(checker.check(self.root, self.listing))

    def test_unlisted_local_tool_link_is_rejected(self):
        (self.root / "README.md").write_text("[missing](scripts/missing.py)\n", encoding="utf-8")
        self.assertFalse(checker.check(self.root, self.listing))

    def test_links_inside_fenced_code_are_ignored(self):
        (self.root / "README.md").write_text("```md\n[missing](scripts/missing.py)\n```\n", encoding="utf-8")
        self.assertTrue(checker.check(self.root, self.listing))

    def test_missing_document_is_rejected(self):
        (self.root / "README.md").unlink()
        self.assertFalse(checker.check(self.root, self.listing))

    def test_missing_package_list_is_rejected(self):
        self.assertFalse(checker.check(self.root, self.root / "absent.txt"))


if __name__ == "__main__":
    unittest.main()
