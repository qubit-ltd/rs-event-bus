"""Regression coverage for the published documentation contract."""

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
CHECKER = REPO / "scripts" / "check_documentation_contract.py"
RESOURCES = {
    "scripts/downstream-compatibility.json": b'{"revisions": []}\n',
    "scripts/downstream-redis-image.txt": b"redis:7-alpine@sha256:fixture\n",
}
GRAPH_PATH = "scripts/check_event_bus_graph.py"
GRAPH_CONTENT = b"REPOSITORIES = ('a', 'b')\nFIXTURES = ('a/tests/fixture',)\n"
READMES = ("README.md", "README.zh_CN.md")
GUIDES = ("doc/user_guide.md", "doc/user_guide.zh_CN.md", "doc/design.md", "doc/design.zh_CN.md")


class DocumentationContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "scripts").mkdir()
        shutil.copy2(CHECKER, self.root / "scripts/check_documentation_contract.py")
        for path, data in RESOURCES.items():
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        (self.root / GRAPH_PATH).write_bytes(GRAPH_CONTENT)
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.email", "fixture@example.com"], cwd=self.root, check=True)
        subprocess.run(["git", "config", "user.name", "Fixture"], cwd=self.root, check=True)
        subprocess.run(["git", "add", *RESOURCES, GRAPH_PATH], cwd=self.root, check=True)
        subprocess.run(["git", "commit", "-qm", "fixture snapshot"], cwd=self.root, check=True)
        self.sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=self.root, check=True, text=True, capture_output=True
        ).stdout.strip()
        self.write_documents()
        self.listing = self.root / "package-list.txt"
        self.files = [*READMES, *GUIDES, "scripts/check_documentation_contract.py", GRAPH_PATH]
        self.write_listing()

    def tearDown(self):
        self.temp.cleanup()

    def write_documents(self):
        resource_links = "\n".join(
            f"[{Path(path).name}](https://github.com/qubit-ltd/rs-event-bus/blob/{self.sha}/{path})"
            for path in RESOURCES
        )
        for path in READMES:
            checkout = (
                "本仓库工具需要在完整 Git checkout 的根目录运行。"
                if path == "README.zh_CN.md"
                else "This tool requires a full Git checkout at the repository root."
            )
            text = (
                f"# README\n\n{checkout}\n\n"
                f"{resource_links}\n\n[checker](scripts/check_documentation_contract.py)\n"
            )
            (self.root / path).write_text(text, encoding="utf-8")
        graph_link = f"https://github.com/qubit-ltd/rs-event-bus/blob/{self.sha}/{GRAPH_PATH}"
        for path in GUIDES:
            (self.root / path).parent.mkdir(parents=True, exist_ok=True)
            (self.root / path).write_text(
                f"# Guide\n\nInput definitions: [REPOSITORIES and FIXTURES]({graph_link})\n",
                encoding="utf-8",
            )

    def write_listing(self):
        self.listing.write_text("\n".join(self.files) + "\n", encoding="utf-8")

    def run_checker(self, *, listing=None):
        return subprocess.run(
            [sys.executable, "-B", str(self.root / "scripts/check_documentation_contract.py"),
             "--root", str(self.root), "--package-list", str(listing or self.listing)],
            cwd=self.root, text=True, capture_output=True, check=False,
        )

    def assert_rejected(self, expected_path):
        result = self.run_checker()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(expected_path, result.stderr)

    def test_valid_contract(self):
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_packaged_local_script_link_must_be_in_package_list(self):
        self.files.remove("scripts/check_documentation_contract.py")
        self.write_listing()
        self.assert_rejected("README.md")

    def test_packaged_local_infra_link_must_be_in_package_list(self):
        path = self.root / "README.md"
        path.write_text(path.read_text(encoding="utf-8") + "[CI](.infra/bin/ci-check.sh)\n", encoding="utf-8")
        self.assert_rejected("README.md")

    def test_markdown_links_inside_fenced_code_are_ignored(self):
        readme = self.root / "README.md"
        readme.write_text(readme.read_text(encoding="utf-8") + "\n```md\n[missing](scripts/not-packaged.py)\n```\n", encoding="utf-8")
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_fixed_resource_blob_link_is_rejected(self):
        path = self.root / "README.md"
        path.write_text(path.read_text(encoding="utf-8").replace(f"/blob/{self.sha}/scripts/downstream-redis-image.txt", "/blob/main/scripts/downstream-redis-image.txt"), encoding="utf-8")
        self.assert_rejected("README.md")

    def test_additional_floating_resource_reference_is_rejected(self):
        path = self.root / "README.md"
        text = path.read_text(encoding="utf-8")
        path.write_text(
            text + "\n[latest image](https://github.com/qubit-ltd/rs-event-bus/blob/main/scripts/downstream-redis-image.txt)\n",
            encoding="utf-8",
        )
        self.assert_rejected("README.md")

    def test_abbreviated_sha_is_rejected(self):
        path = self.root / "README.md"
        path.write_text(path.read_text(encoding="utf-8").replace(self.sha, self.sha[:12]), encoding="utf-8")
        self.assert_rejected("README.md")

    def test_bilingual_resource_targets_must_match(self):
        path = self.root / "README.zh_CN.md"
        path.write_text(path.read_text(encoding="utf-8").replace(
            f"/blob/{self.sha}/scripts/downstream-redis-image.txt", f"/blob/{self.sha}/scripts/downstream-compatibility.json"
        ), encoding="utf-8")
        self.assert_rejected("README.zh_CN.md")

    def test_snapshot_blob_content_must_match_current_resource(self):
        (self.root / "scripts/downstream-redis-image.txt").write_text("redis:changed\n", encoding="utf-8")
        self.assert_rejected("scripts/downstream-redis-image.txt")

    def test_readme_requires_checkout_precondition(self):
        path = self.root / "README.md"
        path.write_text(path.read_text(encoding="utf-8").replace("full Git checkout", "Git clone"), encoding="utf-8")
        self.assert_rejected("README.md")

    def test_each_guide_requires_fixed_input_definition_link(self):
        for relative in GUIDES:
            with self.subTest(document=relative):
                self.write_documents()
                path = self.root / relative
                path.write_text("# Guide\n\nInput definitions are maintained in the checker.\n", encoding="utf-8")
                self.assert_rejected(relative)

    def test_floating_input_definition_link_is_rejected(self):
        path = self.root / GUIDES[0]
        path.write_text(path.read_text(encoding="utf-8").replace(self.sha, "main"), encoding="utf-8")
        self.assert_rejected(path.relative_to(self.root).as_posix())

    def test_missing_package_list_is_rejected(self):
        missing = self.root / "absent-list.txt"
        result = self.run_checker(listing=missing)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(missing), result.stderr)

    def test_unreadable_package_list_is_rejected(self):
        directory = self.root / "package-list-directory"
        directory.mkdir()
        result = self.run_checker(listing=directory)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(directory), result.stderr)


if __name__ == "__main__":
    unittest.main()
