#!/usr/bin/env python3
"""Check links to repository tools against the crate package contents."""

import argparse
import os
import re
import sys
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))

from markdown_headings import heading_lines, heading_titles, lines_outside_fences


DOCUMENTS = ("README.md", "README.zh_CN.md", "doc/user_guide.md", "doc/user_guide.zh_CN.md",
             "doc/design.md", "doc/design.zh_CN.md")
MIGRATION_PAGES = ("doc/migration.md", "doc/migration.zh_CN.md")
SOURCE_LINK_ROOTS = ("src", "tests", "benches", "fuzz")
MIGRATION_GUIDES = {
    "doc/migration.md": ("doc/user_guide.md", "migrate-from-020-to-021"),
    "doc/migration.zh_CN.md": ("doc/user_guide.zh_CN.md", "从-020-迁移到-021"),
}
GUIDE_TOC_ENTRIES = {
    "doc/user_guide.md": (
        "decide-whether-a-failed-publication-can-be-repeated",
        "preflight-rejection-and-an-unconfirmed-send",
        "diagnose-settlement-termination-and-restore-consumption",
        "portable-header-limits",
        "compare-local-and-redis-capabilities",
        "run-the-redis-order-examples",
        "bound-a-conformance-run-with-an-external-watchdog",
        "request-shutdown-without-waiting",
    ),
    "doc/user_guide.zh_CN.md": (
        "判断发布失败后能否重发",
        "预检拒绝与未确认发送",
        "排查结算终止并恢复消费",
        "portable-header-限额",
        "对照-local-与-redis-的实际能力",
        "运行-redis-订单示例",
        "用外部-watchdog-限制-conformance-执行时间",
        "非阻塞地请求停机",
    ),
}
LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")


def markdown_links(text):
    """Yield link destinations outside fenced code blocks."""
    for _line_number, line in lines_outside_fences(text):
        for match in LINK.finditer(line):
            destination = match.group(1).strip()
            if not destination:
                continue
            yield destination.split(maxsplit=1)[0].strip("<>")


def normalized_local_path(document, destination):
    parts = urlsplit(destination)
    if parts.scheme or parts.netloc or not parts.path:
        return None
    raw = unquote(parts.path).replace("\\", "/")
    combined = raw if raw.startswith("/") else str(PurePosixPath(document).parent / raw)
    normalized = os.path.normpath(combined).lstrip("/")
    if normalized == ".." or normalized.startswith("../"):
        return None
    if normalized.startswith("scripts/") or normalized == "scripts":
        return normalized
    if normalized.startswith(".infra/") or normalized == ".infra":
        return normalized
    return None


def check(root, package_list):
    try:
        packaged = {line.strip().replace("\\", "/").removeprefix("./")
                    for line in package_list.read_text(encoding="utf-8").splitlines() if line.strip()}
    except (OSError, UnicodeError) as error:
        print(f"{package_list}: cannot read package list: {error}", file=sys.stderr)
        return False
    errors = []
    for document in DOCUMENTS:
        try:
            text = (root / document).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            errors.append((document, f"cannot read document: {error}"))
            continue
        for destination in markdown_links(text):
            local = normalized_local_path(document, destination)
            if local is not None and local not in packaged:
                errors.append((document, f"local link target {local} is not included in the Cargo package list"))
    for document, message in errors:
        print(f"{document}: {message}", file=sys.stderr)
    return (not errors and check_explicit_source_links(root) and
            check_migration_contract(root, package_list) and
            check_guide_toc_contract(root))


def check_guide_toc_contract(root):
    """Require selected operational sections to be listed and link to real headings."""
    errors = []
    for document, required_anchors in GUIDE_TOC_ENTRIES.items():
        try:
            text = (root / document).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            errors.append((document, f"cannot read guide TOC: {error}"))
            continue
        lines = text.splitlines()
        headings = list(heading_lines(text))
        toc_start = next((line_number for line_number, level, title in headings
                          if level == 2 and title in ("Contents", "目录")), None)
        if toc_start is None:
            errors.append((document, "missing Contents/目录 section"))
            continue
        toc_end = next((line_number for line_number, _level, _title in headings
                        if line_number > toc_start), len(lines))
        toc = "\n".join(lines[toc_start + 1:toc_end])
        toc_links = set(markdown_links(toc))
        anchors = _heading_anchors(text)
        for anchor in required_anchors:
            if f"#{anchor}" not in toc_links:
                errors.append((document, f"missing TOC entry for #{anchor}"))
            if anchor not in anchors:
                errors.append((document, f"TOC target heading #{anchor} does not exist"))
    for document, message in errors:
        print(f"{document}: {message}", file=sys.stderr)
    return not errors


def check_explicit_source_links(root):
    """Check explicit Markdown links to source files in the full checkout."""
    root = root.resolve()
    errors = []
    for document in DOCUMENTS:
        try:
            text = (root / document).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            errors.append((document, f"cannot read document for source links: {error}"))
            continue
        for destination in markdown_links(text):
            parts = urlsplit(destination)
            if parts.scheme or parts.netloc or not parts.path or "*" in parts.path:
                continue
            raw = unquote(parts.path).replace("\\", "/")
            combined = PurePosixPath(raw.lstrip("/")) if raw.startswith("/") else PurePosixPath(document).parent / raw
            normalized = PurePosixPath(os.path.normpath(combined.as_posix()))
            relative = normalized.as_posix()
            if relative == ".." or relative.startswith("../"):
                errors.append((document, f"source link target {raw} escapes the repository"))
                continue
            if not any(relative == prefix or relative.startswith(prefix + "/") for prefix in SOURCE_LINK_ROOTS):
                continue
            candidate = root.joinpath(*normalized.parts)
            try:
                candidate.resolve(strict=False).relative_to(root)
            except (OSError, RuntimeError, ValueError):
                errors.append((document, f"source link target {raw} escapes the repository"))
                continue
            if not candidate.is_file():
                errors.append((document, f"source link target {relative} does not exist as a file"))
    for document, message in errors:
        print(f"{document}: {message}", file=sys.stderr)
    return not errors


def _packaged_paths(package_list):
    try:
        return {line.strip().replace("\\", "/").removeprefix("./")
                for line in package_list.read_text(encoding="utf-8").splitlines() if line.strip()}
    except (OSError, UnicodeError):
        return set()


def _heading_anchors(text):
    anchors = set()
    for title in heading_titles(text):
        # GitHub-style heading IDs: lowercase, remove punctuation, spaces become hyphens.
        title = title.lower()
        title = re.sub(r"[^\w\- ]", "", title, flags=re.UNICODE)
        anchors.add(re.sub(r"\s+", "-", title))
    return anchors


def _is_local_link_to(destination, path, fragment=None):
    parts = urlsplit(destination)
    return (not parts.scheme and not parts.netloc and parts.path == path and
            (fragment is None or parts.fragment == fragment))


def check_migration_contract(root, package_list):
    """Check stable bilingual migration entry points without consulting sibling repositories."""
    errors = []
    packaged = _packaged_paths(package_list)
    contents = {}
    for page in MIGRATION_PAGES:
        try:
            contents[page] = (root / page).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            errors.append((page, f"cannot read migration page: {error}"))
            continue
        if page not in packaged:
            errors.append((page, "migration page is not included in the Cargo package list"))
    for page, peer in ((MIGRATION_PAGES[0], MIGRATION_PAGES[1]),
                       (MIGRATION_PAGES[1], MIGRATION_PAGES[0])):
        text = contents.get(page)
        if text is not None and not any(
                _is_local_link_to(link, peer.removeprefix("doc/"))
                for link in markdown_links(text)):
            errors.append((page, f"missing reciprocal migration link to {peer}"))
    for page, (guide, anchor) in MIGRATION_GUIDES.items():
        text = contents.get(page)
        if text is None:
            continue
        if not (root / guide).is_file():
            errors.append((page, f"guide target {guide} does not exist"))
            continue
        if not any(_is_local_link_to(link, PurePosixPath(guide).name, anchor)
                   for link in markdown_links(text)):
            errors.append((page, f"missing link to {guide}#{anchor}"))
        guide_text = (root / guide).read_text(encoding="utf-8")
        if anchor not in _heading_anchors(guide_text):
            errors.append((page, f"guide anchor {anchor} does not exist in {guide}"))
    for readme in ("README.md", "README.zh_CN.md"):
        try:
            readme_text = (root / readme).read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            errors.append((readme, f"cannot read README: {error}"))
            continue
        target = "doc/migration.md" if readme == "README.md" else "doc/migration.zh_CN.md"
        if not any(_is_local_link_to(link, target) for link in markdown_links(readme_text)):
            errors.append((readme, f"missing migration entry link to {target}"))
    for document, message in errors:
        print(f"{document}: {message}", file=sys.stderr)
    return not errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--package-list", required=True, type=Path)
    args = parser.parse_args()
    return 0 if check(args.root.resolve(), args.package_list) else 1


if __name__ == "__main__":
    raise SystemExit(main())
