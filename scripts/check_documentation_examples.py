#!/usr/bin/env python3
import argparse
import html
import re
import subprocess
from urllib.parse import unquote
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCAL = "examples/local_delivery.rs"
LIB = "src/lib.rs"
ASYNC = "tests/fixtures/documentation_consumer/src/bin/async_local.rs"
ASYNC_SUBSCRIPTION = "src/facade/async_subscription.rs"
SPEC = "tests/fixtures/documentation_consumer/src/provider_spec.rs"
PROVIDER_CONTRACT = "tests/fixtures/documentation_consumer/src/provider_contract.rs"
CODEC = "tests/fixtures/documentation_consumer/src/order_created_codec.rs"
RECEIPT = "tests/fixtures/documentation_consumer/src/receipt_safety.rs"
SHUTDOWN = "tests/fixtures/documentation_consumer/src/bounded_shutdown.rs"
CAPACITY = "tests/fixtures/documentation_consumer/src/local_capacity.rs"
REPUBLISH = "tests/fixtures/documentation_consumer/src/republish_action.rs"
RETRY = "tests/fixtures/documentation_consumer/src/retry_policy.rs"
README_ORDERS = "tests/fixtures/documentation_consumer/src/readme_orders.rs"
MONITOR = "tests/fixtures/documentation_consumer/src/run_state_monitor.rs"
WEIGHTED = "tests/fixtures/documentation_consumer/src/weighted_notifications.rs"
DEAD_LETTER = "tests/fixtures/documentation_consumer/src/dead_letter_codec.rs"
HEADER_LIMITS = "tests/fixtures/documentation_consumer/src/header_limits.rs"
REQUIRED = {
    LIB: {LOCAL},
    "README.md": {LOCAL, README_ORDERS},
    "README.zh_CN.md": {LOCAL, README_ORDERS},
    "doc/user_guide.md": {LOCAL, ASYNC, CODEC, DEAD_LETTER, HEADER_LIMITS, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY, MONITOR, WEIGHTED},
    "doc/user_guide.zh_CN.md": {LOCAL, ASYNC, CODEC, DEAD_LETTER, HEADER_LIMITS, RECEIPT, SHUTDOWN, CAPACITY, REPUBLISH, RETRY, MONITOR, WEIGHTED},
    "doc/design.md": {SPEC, PROVIDER_CONTRACT, DEAD_LETTER, HEADER_LIMITS},
    "doc/design.zh_CN.md": {SPEC, PROVIDER_CONTRACT, DEAD_LETTER, HEADER_LIMITS},
}
PATTERN = re.compile(
    r"<!-- event-bus-source: (?P<path>[^\n]+?) -->\s*"
    r"(?P<fence>[\x60~]{3})rust\s*\n(?P<code>.*?)\n(?P=fence)",
    re.DOTALL,
)
FILE_HEADER = re.compile(
    r"\A// =+\n"
    r"//    Copyright \(c\) [^\n]+\n"
    r"//\n"
    r"//    SPDX-License-Identifier: Apache-2\.0\n"
    r"//\n"
    r"//    Licensed under the Apache License, Version 2\.0\.\n"
    r"// =+\n"
)
EXAMPLE_DOCUMENTS = {LIB, "README.md", "README.zh_CN.md", "doc/user_guide.md", "doc/user_guide.zh_CN.md", "doc/design.md", "doc/design.zh_CN.md"}


def read_document_text(path):
    text = path.read_text(encoding="utf-8")
    if path.suffix != ".rs":
        return text
    lines = text.splitlines()
    start = next((i for i, line in enumerate(lines) if line.startswith("//!")), None)
    if start is None:
        return ""
    result = []
    for line in lines[start:]:
        if not line.startswith("//!"):
            break
        result.append(line[3:].removeprefix(" "))
    return "\n".join(result)


DOCUMENT_LINKS = {
    "User guide": "doc/user_guide.md",
    "用户指南": "doc/user_guide.zh_CN.md",
    "Design": "doc/design.md",
    "设计文档": "doc/design.zh_CN.md",
}


def check_snapshot_content(snapshot_sha, fetch_snapshot, document_shas):
    errors = []
    try:
        if fetch_snapshot:
            present = subprocess.run(
                ["git", "cat-file", "-e", f"{snapshot_sha}^{{commit}}"],
                cwd=ROOT,
                capture_output=True,
                check=False,
            )
            if present.returncode != 0:
                fetched = subprocess.run(
                    ["git", "fetch", "--no-tags", "--depth=1", "origin", snapshot_sha],
                    cwd=ROOT,
                    capture_output=True,
                    check=False,
                )
                if fetched.returncode != 0:
                    detail = fetched.stderr.decode("utf-8", errors="replace").strip()
                    errors.append(f"snapshot {snapshot_sha} fetch failed: {detail or 'git fetch failed'}")
                    return errors
        snapshot_documents = {}
        for document_path in DOCUMENT_LINKS.values():
            result = subprocess.run(
                ["git", "show", f"{snapshot_sha}:{document_path}"],
                cwd=ROOT,
                capture_output=True,
                check=False,
            )
            if result.returncode != 0:
                detail = result.stderr.decode("utf-8", errors="replace").strip()
                errors.append(
                    f"{document_path}: snapshot {snapshot_sha} unavailable"
                    f"{': ' + detail if detail else ''}"
                )
            elif result.stdout != (ROOT / document_path).read_bytes():
                errors.append(f"{document_path}: snapshot {snapshot_sha} content drift")
            else:
                snapshot_documents[document_path] = result.stdout.decode("utf-8")
        if len(snapshot_documents) == len(DOCUMENT_LINKS):
            errors.extend(check_method_links(snapshot_documents, document_shas))
    except OSError as error:
        errors.append(f"snapshot {snapshot_sha}: unable to run git: {error}")
    return errors


def check_method_links(snapshot_documents=None, document_shas=None):
    errors = []
    try:
        source = (ROOT / ASYNC_SUBSCRIPTION).read_text(encoding="utf-8")
    except OSError as error:
        return [f"{ASYNC_SUBSCRIPTION}: unable to read AsyncSubscription::run() Rustdoc: {error}"]
    match = re.search(r"(?P<docs>(?:\s*///.*\n)+)\s*pub async fn run\b", source)
    if match is None:
        return [f"{ASYNC_SUBSCRIPTION}: unable to locate AsyncSubscription::run() Rustdoc"]
    docs = "\n".join(re.sub(r"^\s*/// ?", "", line) for line in match.group("docs").splitlines())
    docs = re.sub(r"\s+", " ", docs)
    for label, path, home_label in (("English user guide", "doc/user_guide.md", "User guide"),
                                    ("Chinese user guide", "doc/user_guide.zh_CN.md", "用户指南")):
        found = re.findall(rf"\[{re.escape(label)}\]\(([^)]+)\)", docs)
        prefix = f"{ASYNC_SUBSCRIPTION}: AsyncSubscription::run(): {label}"
        if len(found) != 1:
            errors.append(f"{prefix}: expected exactly one guide link (found {len(found)})")
            continue
        parsed = re.fullmatch(
            rf"https://github\.com/qubit-ltd/rs-event-bus/blob/([0-9a-f]{{40}})/{re.escape(path)}#(.+)", found[0]
        )
        if parsed is None:
            errors.append(f"{prefix}: invalid SHA, path, or anchor in {found[0]!r}")
            continue
        sha, fragment = parsed.groups()
        expected_sha = (document_shas or {}).get(home_label)
        if expected_sha is None or sha != expected_sha:
            errors.append(f"{prefix}: SHA {sha} does not match the crate home guide link")
        decoded = unquote(html.unescape(fragment))
        headings = read_headings(snapshot_documents[path] if snapshot_documents else read_document_text(ROOT / path))
        if decoded not in headings:
            errors.append(f"{prefix}: anchor {decoded!r} does not identify a heading in {path}")
    return errors


def read_headings(text):
    headings = set()
    for line in text.splitlines():
        if not re.match(r"^#{1,6} ", line):
            continue
        title = re.sub(r"[`*_~]", "", line.lstrip("#").strip()).lower()
        slug = re.sub(r"[^\w -]", "", title, flags=re.UNICODE).replace(" ", "-")
        headings.add(slug)
    return headings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-snapshot-content", action="store_true")
    parser.add_argument("--fetch-snapshot", action="store_true")
    args = parser.parse_args()
    if args.fetch_snapshot and not args.check_snapshot_content:
        parser.error("--fetch-snapshot requires --check-snapshot-content")

    errors = []
    document_text = read_document_text(ROOT / LIB)
    document_shas = {}
    for label, path in DOCUMENT_LINKS.items():
        label_pattern = re.escape(label)
        matches = re.findall(rf"\[{label_pattern}\]\(([^)]*)\)", document_text)
        if len(matches) != 1:
            errors.append(f"{LIB}: [{label}] must appear exactly once (found {len(matches)})")
            continue
        url = matches[0]
        expected = re.fullmatch(
            rf"https://github\.com/qubit-ltd/rs-event-bus/blob/([0-9a-f]{{40}})/{re.escape(path)}",
            url,
        )
        if expected is None:
            errors.append(
                f"{LIB}: [{label}] has invalid snapshot link {url!r}; expected "
                f"https://github.com/qubit-ltd/rs-event-bus/blob/<40-lowercase-hex-sha>/{path}"
            )
        else:
            document_shas[label] = expected.group(1)
    if len(set(document_shas.values())) > 1:
        for label, sha in document_shas.items():
            errors.append(f"{LIB}: [{label}] uses snapshot SHA {sha}, but all four links must use the same SHA")

    if not args.check_snapshot_content:
        errors.extend(check_method_links(document_shas=document_shas))

    for document, required in REQUIRED.items():
        text = read_document_text(ROOT / document)
        found = set()
        for match in PATTERN.finditer(text):
            name = match.group("path")
            source = (ROOT / name).resolve()
            if not source.is_relative_to(ROOT) or not source.is_file():
                errors.append(f"{document}: invalid source path {name}")
                continue
            found.add(name)
            source_code = source.read_text(encoding="utf-8")
            if document in EXAMPLE_DOCUMENTS:
                source_code = FILE_HEADER.sub("", source_code)
            if name == SHUTDOWN:
                test_module = "\n#[cfg(test)]\nmod tests;\n"
                if source_code.count(test_module) == 1:
                    source_code = source_code.replace(test_module, "\n", 1)
            if match.group("code").strip() != source_code.strip():
                errors.append(f"{document}: example drift from {name}")
        for missing in sorted(required - found):
            errors.append(f"{document}: missing checked example {missing}")

    if args.check_snapshot_content and not errors and len(set(document_shas.values())) == 1:
        errors.extend(check_snapshot_content(next(iter(document_shas.values())), args.fetch_snapshot, document_shas))
    if errors:
        raise SystemExit("\n".join(errors))
    print("documentation examples match their compiled sources and snapshot links")


if __name__ == "__main__":
    main()
