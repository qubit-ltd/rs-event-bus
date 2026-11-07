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
LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
FENCE = re.compile(r"^\s*(`{3,}|~{3,})")


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
    return not errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--package-list", required=True, type=Path)
    args = parser.parse_args()
    return 0 if check(args.root.resolve(), args.package_list) else 1


if __name__ == "__main__":
    raise SystemExit(main())
