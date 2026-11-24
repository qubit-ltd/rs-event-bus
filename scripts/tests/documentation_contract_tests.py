"""Regression tests for package-local documentation links."""

import importlib.util
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
import re
import tempfile
import unittest
from urllib.parse import quote

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

    def test_container_and_indented_code_links_are_ignored_and_following_links_checked(self):
        examples = (
            "- example:\n\n    ```md\n    [hidden](scripts/not-packaged.py)\n    ```\n\n",
            "> ```md\n> [hidden](scripts/not-packaged.py)\n> ```\n",
            "\n    [hidden](scripts/not-packaged.py)\n",
        )
        for code in examples:
            with self.subTest(code=code):
                text = "[Migration](doc/migration.md)\n" + code + "\n[tool](scripts/tool.py)\n"
                (self.root / "README.md").write_text(text, encoding="utf-8")
                self.assertEqual(list(checker.markdown_links(text)), [
                    "doc/migration.md", "scripts/tool.py",
                ])
                self.assertTrue(checker.check(self.root, self.listing))

    def test_empty_link_destinations_are_skipped_without_hiding_real_links(self):
        links = "[empty]() [spaces]( ) [tab]( \t ) [section](#keep) [tool](scripts/tool.py)\n"
        self.assertEqual(list(checker.markdown_links(links)), ["#keep", "scripts/tool.py"])
        text = "[Migration](doc/migration.md)\n" + links
        (self.root / "README.md").write_text(text, encoding="utf-8")
        self.assertTrue(checker.check(self.root, self.listing))

    def test_real_links_in_list_continuations_and_quote_bodies_are_checked(self):
        text = "- paragraph\n    [missing](scripts/not-packaged.py)\n> [also missing](scripts/also-not-packaged.py)\n"
        self.assertEqual(list(checker.markdown_links(text)), [
            "scripts/not-packaged.py", "scripts/also-not-packaged.py",
        ])

    def test_indented_code_does_not_interrupt_a_paragraph(self):
        text = "Paragraph\n    [continued](scripts/continued.py)\n\n    [code](scripts/not-packaged.py)\n[tool](scripts/tool.py)\n"
        self.assertEqual(list(checker.markdown_links(text)), [
            "scripts/continued.py", "scripts/tool.py",
        ])

    def test_backtick_in_fence_info_does_not_hide_later_real_heading(self):
        text = "````example with `backticks`\n## Visible heading\n"
        self.assertIn("Visible heading", list(checker.heading_titles(text)))

    def test_commonmark_headings_and_links_ignore_code_and_comments(self):
        text = ("<!--\n## hidden comment\n-->\n"
                "> ```md\n> ## hidden quote fence\n> ```\n"
                "\t## hidden tab indent\n"
                "- item\n\n        ## hidden list code\n"
                "   ## Visible heading\n"
                "`[fake](scripts/missing.py)` [real](<scripts/tool.py>)\n")
        self.assertEqual(list(checker.heading_titles(text)), ["Visible heading"])
        self.assertEqual([destination for _line, destination in checker.markdown_link_lines(text)],
                         ["scripts/tool.py"])

    def test_markdown_link_destinations_keep_parentheses_and_angle_bracket_spaces(self):
        text = "[parentheses](src/name_(part).rs) [space](<src/file name.rs>)\n"
        self.assertEqual(list(checker.markdown_links(text)), [
            "src/name_(part).rs", "src/file name.rs",
        ])

    def test_markdown_reference_links_resolve_across_toc_scope(self):
        guide = self.root / "doc/user_guide.md"
        text = guide.read_text(encoding="utf-8")
        text = text.replace("](#run-the-redis-order-examples)", "][orders]", 1)
        guide.write_text(text + "\n[orders]: #run-the-redis-order-examples\n", encoding="utf-8")
        self.assertTrue(checker.check_guide_toc_contract(self.root))

    def test_images_do_not_satisfy_readme_migration_entries(self):
        for readme, target in (("README.md", "doc/migration.md"),
                               ("README.zh_CN.md", "doc/migration.zh_CN.md")):
            with self.subTest(readme=readme):
                (self.root / readme).write_text(f"![Migration]({target})\n", encoding="utf-8")
                output = StringIO()
                with redirect_stderr(output):
                    self.assertFalse(checker.check_migration_contract(self.root, self.listing))
                self.assertIn(f"{readme}: missing migration entry link to {target}", output.getvalue())

    def test_images_do_not_satisfy_reciprocal_migration_links(self):
        self._migration_pages()
        (self.root / "doc/migration.md").write_text(
            "![简体中文](migration.zh_CN.md) [guide](user_guide.md#migrate-from-020-to-021)\n",
            encoding="utf-8")
        output = StringIO()
        with redirect_stderr(output):
            self.assertFalse(checker.check_migration_contract(self.root, self.listing))
        self.assertIn("missing reciprocal migration link to doc/migration.zh_CN.md", output.getvalue())

    def test_images_do_not_satisfy_migration_guide_links(self):
        self._migration_pages()
        (self.root / "doc/migration.md").write_text(
            "[简体中文](migration.zh_CN.md) ![guide](user_guide.md#migrate-from-020-to-021)\n",
            encoding="utf-8")
        output = StringIO()
        with redirect_stderr(output):
            self.assertFalse(checker.check_migration_contract(self.root, self.listing))
        self.assertIn("missing link to doc/user_guide.md#migrate-from-020-to-021", output.getvalue())

    def test_link_wrapped_image_satisfies_navigation(self):
        self._migration_pages()
        (self.root / "doc/migration.md").write_text(
            "[![简体中文](images/flag.png)](migration.zh_CN.md) "
            "[guide](user_guide.md#migrate-from-020-to-021)\n", encoding="utf-8")
        (self.root / "doc/migration.zh_CN.md").write_text(
            "[English](migration.md) [指南](user_guide.zh_CN.md#从-020-迁移到-021)\n",
            encoding="utf-8")
        self.listing.write_text(
            "scripts/tool.py\ndoc/migration.md\ndoc/migration.zh_CN.md\ndoc/images/flag.png\n",
            encoding="utf-8")
        self.assertTrue(checker.check_migration_contract(self.root, self.listing))

    def test_image_local_resource_paths_are_still_checked(self):
        text = "![missing](scripts/not-packaged.png)\n"
        (self.root / "README.md").write_text(text, encoding="utf-8")
        self.assertEqual(list(checker.markdown_links(text)), [])
        self.assertFalse(checker.check(self.root, self.listing))

    def test_bad_url_reports_location_and_continues_collecting(self):
        (self.root / "README.md").write_text(
            "[bad](http://[)\n[missing](scripts/missing.py)\n", encoding="utf-8")
        (self.root / "README.zh_CN.md").write_text(
            "[missing](scripts/also-missing.py)\n", encoding="utf-8")
        output = StringIO()
        with redirect_stderr(output):
            self.assertFalse(checker.check(self.root, self.listing))
        diagnostics = output.getvalue()
        self.assertEqual(diagnostics.count("README.md:1: target 'http://['"), 1)
        self.assertIn("README.md: local link target scripts/missing.py", diagnostics)
        self.assertIn("README.zh_CN.md: local link target scripts/also-missing.py", diagnostics)

    def test_reference_style_missing_source_link_is_checked(self):
        (self.root / "src").mkdir()
        (self.root / "doc/design.md").write_text(
            "[missing source][implementation]\n\n[implementation]: ../src/missing.rs\n",
            encoding="utf-8")
        self.assertFalse(checker.check_explicit_source_links(self.root))

    def test_fence_ends_when_its_list_or_quote_container_ends(self):
        list_text = "- example:\n\n    ```md\n    [hidden](scripts/not-packaged.py)\n[visible](scripts/tool.py)\n"
        quote_text = "> ```md\n> - ```\n> [hidden](scripts/not-packaged.py)\n[visible](scripts/tool.py)\n"
        for text in (list_text, quote_text):
            with self.subTest(text=text):
                self.assertEqual(list(checker.markdown_links(text)), ["scripts/tool.py"])

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

    def test_bilingual_example_links_must_exist_and_be_packaged(self):
        (self.root / "examples").mkdir()
        self._migration_pages()
        for readme, migration, example in (("README.md", "doc/migration.md", "local_minimal.rs"),
                                           ("README.zh_CN.md", "doc/migration.zh_CN.md",
                                            "async_local_minimal.rs")):
            (self.root / readme).write_text(
                f"[Migration]({migration})\n[example](examples/{example})\n",
                encoding="utf-8")
        self.assertFalse(checker.check(self.root, self.listing))
        output = StringIO()
        with redirect_stderr(output):
            checker.check_explicit_source_links(self.root)
        self.assertIn("examples/local_minimal.rs does not exist as a file", output.getvalue())
        self.assertIn("examples/async_local_minimal.rs does not exist as a file", output.getvalue())

        for example in ("local_minimal.rs", "async_local_minimal.rs"):
            (self.root / "examples" / example).write_text("// example\n", encoding="utf-8")
        output = StringIO()
        with redirect_stderr(output):
            self.assertFalse(checker.check(self.root, self.listing))
        self.assertIn(
            "README.md: local link target examples/local_minimal.rs is not included in the Cargo package list",
            output.getvalue())
        self.assertIn(
            "README.zh_CN.md: local link target examples/async_local_minimal.rs is not included in the Cargo package list",
            output.getvalue())

        with self.listing.open("a", encoding="utf-8") as package_list:
            package_list.write("examples/local_minimal.rs\nexamples/async_local_minimal.rs\n")
        (self.root / "doc/design.md").write_text(
            "[example](../examples/local_minimal.rs#main)\n", encoding="utf-8")
        self.assertTrue(checker.check(self.root, self.listing))

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

    def test_images_do_not_satisfy_operational_toc_entries(self):
        for document, anchors in checker.GUIDE_TOC_ENTRIES.items():
            anchor = anchors[0]
            with self.subTest(document=document):
                path = self.root / document
                original = path.read_text(encoding="utf-8")
                text, count = re.subn(r"- \[[^\]]+\]\(#" + re.escape(anchor) + r"\)",
                                      f"- ![entry](#{anchor})", original, count=1)
                self.assertEqual(count, 1)
                path.write_text(text, encoding="utf-8")
                output = StringIO()
                with redirect_stderr(output):
                    self.assertFalse(checker.check_guide_toc_contract(self.root))
                self.assertIn(f"missing TOC entry for #{anchor}", output.getvalue())
                path.write_text(original, encoding="utf-8")

    def test_percent_encoded_chinese_toc_fragment_matches_utf8_heading(self):
        guide = self.root / "doc/user_guide.zh_CN.md"
        text = guide.read_text(encoding="utf-8")
        anchor = checker.GUIDE_TOC_ENTRIES["doc/user_guide.zh_CN.md"][0]
        encoded = quote(anchor, safe="-")
        guide.write_text(text.replace(f"](#{anchor})", f"](#{encoded})", 1), encoding="utf-8")
        self.assertTrue(checker.check_guide_toc_contract(self.root))

    def test_toc_rejects_path_and_external_fragment_masquerades(self):
        guide = self.root / "doc/user_guide.md"
        original = guide.read_text(encoding="utf-8")
        anchor = checker.GUIDE_TOC_ENTRIES["doc/user_guide.md"][0]
        for destination in (f"other.md#{anchor}", f"https://example.com/#{anchor}"):
            with self.subTest(destination=destination):
                guide.write_text(original.replace(f"](#{anchor})", f"]({destination})", 1),
                                 encoding="utf-8")
                self.assertFalse(checker.check_guide_toc_contract(self.root))
        guide.write_text(original, encoding="utf-8")

    def test_percent_encoded_chinese_migration_fragment_matches_utf8_heading(self):
        self._migration_pages()
        page = self.root / "doc/migration.zh_CN.md"
        text = page.read_text(encoding="utf-8")
        anchor = checker.MIGRATION_GUIDES["doc/migration.zh_CN.md"][1]
        page.write_text(text.replace(f"#{anchor}", f"#{quote(anchor, safe='-')}"), encoding="utf-8")
        self.assertTrue(checker.check_migration_contract(self.root, self.listing))

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
        hidden_heading = f"{marker}md\n{short_marker}\n{heading}\n{marker}\n"
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
