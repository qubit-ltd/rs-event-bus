#!/usr/bin/env python3
"""Check links to repository tools against the crate package contents."""

import argparse
import os
import re
import sys
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit


DOCUMENTS = ("README.md", "README.zh_CN.md", "doc/user_guide.md", "doc/user_guide.zh_CN.md",
             "doc/design.md", "doc/design.zh_CN.md")
MIGRATION_PAGES = ("doc/migration.md", "doc/migration.zh_CN.md")
SOURCE_LINK_ROOTS = ("src", "tests", "benches", "fuzz")
MIGRATION_GUIDES = {
    "doc/migration.md": ("doc/user_guide.md", "migrate-from-020-to-021"),
    "doc/migration.zh_CN.md": ("doc/user_guide.zh_CN.md", "从-020-迁移到-021"),
}
LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
FENCE = re.compile(r"^\s*(`{3,}|~{3,})")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$")


def markdown_links(text):
    """Yield link destinations outside fenced code blocks."""
    fence_char = None
    fence_size = 0
    for line in text.splitlines():
        if fence_char is None:
            fence = FENCE.match(line)
            if fence:
                marker = fence.group(1)
                fence_char, fence_size = marker[0], len(marker)
                continue
            yield from (match.group(1).strip().split(maxsplit=1)[0].strip("<>")
                        for match in LINK.finditer(line))
            continue
        closing = re.fullmatch(r"\s*(`{3,}|~{3,})[ \t]*", line)
        if closing:
            marker = closing.group(1)
            if marker[0] == fence_char and len(marker) >= fence_size:
                fence_char, fence_size = None, 0


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
    return not errors and check_explicit_source_links(root) and check_migration_contract(root, package_list)


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
    for line in text.splitlines():
        match = HEADING.match(line)
        if match:
            # GitHub-style heading IDs: lowercase, remove punctuation, spaces become hyphens.
            title = match.group(1).strip().lower()
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
