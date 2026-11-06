#!/usr/bin/env python3
"""Check crate documentation links and validation-input references."""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit


README_EN = "README.md"
README_ZH = "README.zh_CN.md"
READMES = (README_EN, README_ZH)
GUIDES = ("doc/user_guide.md", "doc/user_guide.zh_CN.md", "doc/design.md", "doc/design.zh_CN.md")
RESOURCES = (
    "scripts/downstream-compatibility.json",
    "scripts/downstream-redis-image.txt",
)
GRAPH_SCRIPT = "scripts/check_event_bus_graph.py"
LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
FENCE = re.compile(r"^\s*(`{3,}|~{3,})")
SHA = r"[0-9a-fA-F]{40}"
REPOSITORY = "https://github.com/qubit-ltd/rs-event-bus/blob/"


def fail(path: str, message: str) -> None:
    print(f"{path}: {message}", file=sys.stderr)


def read_text(root: Path, relative: str) -> str:
    return (root / relative).read_text(encoding="utf-8")


def markdown_links(text: str):
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
            yield from (match.group(1).strip().split(maxsplit=1)[0].strip("<>") for match in LINK.finditer(line))
            continue

        closing = re.fullmatch(r"\s*(`{3,}|~{3,})[ \t]*", line)
        if closing:
            marker = closing.group(1)
            if marker[0] == fence_char and len(marker) >= fence_size:
                fence_char, fence_size = None, 0
                continue


def normalized_local_path(document: str, destination: str):
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


def package_entries(package_list: Path):
    lines = package_list.read_text(encoding="utf-8").splitlines()
    return {line.strip().replace("\\", "/").removeprefix("./") for line in lines if line.strip()}


def extract_blob(destination: str, expected_path: str):
    match = re.fullmatch(re.escape(REPOSITORY) + rf"({SHA})/(.+)", destination)
    if match is None or match.group(2) != expected_path:
        return None
    return match.group(1), match.group(2)


def is_repository_blob_for(destination: str, expected_path: str) -> bool:
    parts = urlsplit(destination)
    prefix = "/qubit-ltd/rs-event-bus/blob/"
    return (
        parts.scheme == "https"
        and parts.netloc == "github.com"
        and parts.path.startswith(prefix)
        and parts.path.partition(prefix)[2].count("/") >= 1
        and parts.path.partition(prefix)[2].split("/", 1)[1] == expected_path
    )


def compare_blob(root: Path, path: str, sha: str, errors) -> None:
    try:
        current = (root / path).read_bytes()
    except OSError as error:
        errors.append((path, f"cannot read current input: {error}"))
        return
    try:
        result = subprocess.run(
            ["git", "show", f"{sha}:{path}"], cwd=root, capture_output=True, check=False
        )
    except OSError as error:
        errors.append((path, f"cannot read Git snapshot {sha}: {error}"))
        return
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        errors.append((path, f"Git snapshot {sha} is unavailable{': ' + detail if detail else ''}"))
    elif result.stdout != current:
        errors.append((path, f"Git snapshot {sha} content differs from the current input"))


def check(root: Path, package_list: Path) -> bool:
    errors = []
    try:
        packaged = package_entries(package_list)
    except (OSError, UnicodeError) as error:
        fail(str(package_list), f"cannot read package list: {error}")
        return False

    readme_blobs = {}
    for readme in READMES:
        try:
            text = read_text(root, readme)
        except (OSError, UnicodeError) as error:
            errors.append((readme, f"cannot read README: {error}"))
            continue
        if readme == README_EN:
            if re.search(r"\bfull\s+Git\s+checkout\b", text, re.IGNORECASE) is None:
                errors.append((readme, "must state that repository tools require a full Git checkout"))
        elif re.search(r"完整\s*Git\s*checkout", text, re.IGNORECASE) is None:
            errors.append((readme, "必须说明仓库工具需要完整 Git checkout"))

        for destination in markdown_links(text):
            local = normalized_local_path(readme, destination)
            if local is not None and local not in packaged:
                errors.append((readme, f"local link target {local} is not included in the Cargo package list"))

        matches = {}
        destinations = list(markdown_links(text))
        for resource in RESOURCES:
            references = [destination for destination in destinations
                          if is_repository_blob_for(destination, resource)]
            exact = [blob for destination in references
                     if (blob := extract_blob(destination, resource)) is not None]
            if len(references) != 1 or len(exact) != 1:
                errors.append((readme, f"must contain exactly one fixed 40-digit blob link to {resource}"))
            else:
                matches[resource] = exact[0]
                readme_blobs[(readme, resource)] = exact[0]
        if len(matches) == len(RESOURCES):
            readme_blobs[(readme, "all")] = tuple(matches[resource] for resource in RESOURCES)

    for resource in RESOURCES:
        en = readme_blobs.get((README_EN, resource))
        zh = readme_blobs.get((README_ZH, resource))
        if en is not None and zh is not None and en != zh:
            errors.append((README_ZH, f"fixed blob link for {resource} differs from {README_EN}"))
        for blob in (en, zh):
            if blob is not None:
                compare_blob(root, resource, blob[0], errors)

    graph_shas = []
    for document in GUIDES:
        try:
            destinations = list(markdown_links(read_text(root, document)))
        except (OSError, UnicodeError) as error:
            errors.append((document, f"cannot read document: {error}"))
            continue
        links = [blob for destination in destinations
                 if (blob := extract_blob(destination, GRAPH_SCRIPT)) is not None]
        references = [destination for destination in destinations
                      if is_repository_blob_for(destination, GRAPH_SCRIPT)]
        if len(references) != 1 or len(links) != 1:
            errors.append((document, f"must link exactly once to the fixed {GRAPH_SCRIPT} input definitions"))
        else:
            graph_shas.append((document, links[0][0]))
    if len(graph_shas) == len(GUIDES):
        distinct = {sha for _, sha in graph_shas}
        if len(distinct) != 1:
            for document, _ in graph_shas:
                errors.append((document, "input-definition link SHA must match all other guide/design documents"))
        for sha in distinct:
            compare_blob(root, GRAPH_SCRIPT, sha, errors)

    for path, message in errors:
        fail(path, message)
    return not errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--package-list", required=True, type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    return 0 if check(root, args.package_list) else 1


if __name__ == "__main__":
    raise SystemExit(main())
