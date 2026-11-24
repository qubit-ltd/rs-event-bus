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
        for guide, anchors in checker.GUIDE_TOC_ENTRIES.items():
            titles = {
                "判断发布失败后能否重发": "判断发布失败后能否重发",
                "预检拒绝与未确认发送": "预检拒绝与未确认发送",
                "排查结算终止并恢复消费": "排查结算终止并恢复消费",
                "portable-header-限额": "Portable header 限额",
                "对照-local-与-redis-的实际能力": "对照 local 与 Redis 的实际能力",
                "运行-redis-订单示例": "运行 Redis 订单示例",
                "用外部-watchdog-限制-conformance-执行时间": "用外部 watchdog 限制 conformance 执行时间",
                "非阻塞地请求停机": "非阻塞地请求停机",
            }
            english_titles = {
                "decide-whether-a-failed-publication-can-be-repeated": "Decide whether a failed publication can be repeated",
                "preflight-rejection-and-an-unconfirmed-send": "Preflight rejection and an unconfirmed send",
                "diagnose-settlement-termination-and-restore-consumption": "Diagnose settlement termination and restore consumption",
                "portable-header-limits": "Portable header limits",
                "compare-local-and-redis-capabilities": "Compare local and Redis capabilities",
                "run-the-redis-order-examples": "Run the Redis order examples",
                "bound-a-conformance-run-with-an-external-watchdog": "Bound a conformance run with an external watchdog",
                "request-shutdown-without-waiting": "Request shutdown without waiting",
            }
            title_map = english_titles if guide.endswith(".md") and not guide.endswith("zh_CN.md") else titles
            (self.root / guide).write_text(
                "## Contents\n" + "".join(f"- [{title_map[a]}](#{a})\n" for a in anchors) +
                "## Body\n" + ("### Migrate from 0.20 to 0.21\n" if guide.endswith(".md") and not guide.endswith("zh_CN.md")
                                  else "### 从 0.20 迁移到 0.21\n") +
                "".join(f"### {title_map[a]}\n" for a in anchors), encoding="utf-8")
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

    def test_explicit_source_links_are_checked_with_document_relative_paths_and_fragments(self):
        (self.root / "src").mkdir()
        (self.root / "src/example.rs").write_text("// source\n", encoding="utf-8")
        (self.root / "doc/design.md").write_text(
            "[source](../src/example.rs#entry)\nhttps://example.com/src/missing.rs\n"
            "[#section](#overview)\n```md\n[ignored](../src/missing.rs)\n```\n",
            encoding="utf-8",
        )
        self.assertTrue(checker.check(self.root, self.listing))

    def test_missing_explicit_source_file_is_rejected(self):
        (self.root / "README.md").write_text(
            "[Migration](doc/migration.md)\n[missing](src/missing.rs)\n", encoding="utf-8")
        self.assertFalse(checker.check(self.root, self.listing))

    def test_explicit_source_link_that_escapes_repository_is_rejected(self):
        (self.root / "doc/design.md").write_text(
            "[outside](../../outside.rs)\n", encoding="utf-8")
        self.assertFalse(checker.check(self.root, self.listing))

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

    def test_missing_operational_toc_entry_is_rejected(self):
        guide = self.root / "doc/user_guide.md"
        guide.write_text(guide.read_text(encoding="utf-8").replace(
            "- [Run the Redis order examples](#run-the-redis-order-examples)\n", ""), encoding="utf-8")
        self.assertFalse(checker.check_guide_toc_contract(self.root))

    def test_missing_operational_toc_target_is_rejected(self):
        guide = self.root / "doc/user_guide.zh_CN.md"
        guide.write_text(guide.read_text(encoding="utf-8").replace(
            "### 排查结算终止并恢复消费\n", ""), encoding="utf-8")
        self.assertFalse(checker.check_guide_toc_contract(self.root))

    def test_only_second_level_contents_starts_the_toc(self):
        guide = self.root / "doc/user_guide.md"
        guide.write_text(guide.read_text(encoding="utf-8").replace("## Contents", "### Contents", 1),
                         encoding="utf-8")
        self.assertFalse(checker.check_guide_toc_contract(self.root))

    def _fence_heading(self, guide, anchor, marker):
        path = self.root / guide
        text = path.read_text(encoding="utf-8")
        title = next(title for title in checker.heading_titles(text)
                     if anchor in checker._heading_anchors(f"## {title}\n"))
        heading = next(line for line in text.splitlines()
                       if line.lstrip().startswith("#")
                       and line.lstrip("#").strip().rstrip("#").strip() == title)
        short_marker = marker[0] * 3
        hidden_heading = f"{marker}md\n{heading}\n{short_marker}\n{marker}\n"
        path.write_text(text.replace(heading + "\n", hidden_heading, 1), encoding="utf-8")

    def test_guide_targets_inside_backtick_and_tilde_fences_are_rejected(self):
        for guide, anchors in checker.GUIDE_TOC_ENTRIES.items():
            original = (self.root / guide).read_text(encoding="utf-8")
            for marker in ("````", "~~~~"):
                with self.subTest(guide=guide, marker=marker):
                    (self.root / guide).write_text(original, encoding="utf-8")
                    self._fence_heading(guide, anchors[0], marker)
                    self.assertFalse(checker.check_guide_toc_contract(self.root))

    def test_migration_targets_inside_fences_are_rejected(self):
        for page, (guide, anchor) in checker.MIGRATION_GUIDES.items():
            original = (self.root / guide).read_text(encoding="utf-8")
            for marker in ("````", "~~~~"):
                with self.subTest(page=page, marker=marker):
                    self._migration_pages()
                    (self.root / guide).write_text(original, encoding="utf-8")
                    self._fence_heading(guide, anchor, marker)
                    self.assertFalse(checker.check_migration_contract(self.root, self.listing))

    def test_real_heading_outside_fence_remains_a_valid_target(self):
        self.assertIn("run-the-redis-order-examples",
                      checker._heading_anchors("````md\n# fake\n`````\n## Run the Redis order examples\n"))


if __name__ == "__main__":
    unittest.main()
